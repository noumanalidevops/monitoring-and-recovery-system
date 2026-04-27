from collections import deque
from datetime import datetime, timedelta
from threading import Lock, Thread
import json
import logging
import math
import os
import shutil
import time

import joblib
import numpy as np
import requests
from fastapi import FastAPI
from prometheus_client import generate_latest
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from starlette.responses import PlainTextResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Anomaly Detection Service")

# ---- Runtime + Model Config ----
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
MODEL_MAX_AGE_HOURS = int(os.getenv("MODEL_MAX_AGE_HOURS", "24"))
RETRAIN_INTERVAL_HOURS = int(os.getenv("RETRAIN_INTERVAL_HOURS", "24"))
RETRAIN_COOLDOWN_SECONDS = int(os.getenv("RETRAIN_COOLDOWN_SECONDS", "900"))
BOOTSTRAP_RETRY_SECONDS = int(os.getenv("BOOTSTRAP_RETRY_SECONDS", "60"))
DRIFT_WINDOW_SIZE = int(os.getenv("DRIFT_WINDOW_SIZE", "60"))
DRIFT_ZSCORE_THRESHOLD = float(os.getenv("DRIFT_ZSCORE_THRESHOLD", "2.5"))
DRIFT_CONSECUTIVE_LIMIT = int(os.getenv("DRIFT_CONSECUTIVE_LIMIT", "5"))

PRIMARY_MODEL_PATH = os.path.join(MODEL_DIR, "model.pkl")
PRIMARY_SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
PRIMARY_META_PATH = os.path.join(MODEL_DIR, "metadata.json")
FALLBACK_MODEL_PATH = os.path.join(MODEL_DIR, "last_good_model.pkl")
FALLBACK_SCALER_PATH = os.path.join(MODEL_DIR, "last_good_scaler.pkl")
FALLBACK_META_PATH = os.path.join(MODEL_DIR, "last_good_metadata.json")

# ---- Shared State ----
model = None
scaler = None
is_trained = False
training_in_progress = False
model_version = None
last_trained_at = None
last_retrain_request_ts = 0.0
drift_breach_count = 0
recent_features = deque(maxlen=DRIFT_WINDOW_SIZE)

state_lock = Lock()
training_lock = Lock()


def _safe_float(value):
    return 0.0 if math.isnan(value) or math.isinf(value) else float(value)


def _ensure_model_dir():
    os.makedirs(MODEL_DIR, exist_ok=True)


def _save_metadata(path, metadata):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)


def _load_metadata(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _set_runtime_model(next_model, next_scaler, metadata):
    global model, scaler, is_trained, model_version, last_trained_at
    model = next_model
    scaler = next_scaler
    is_trained = True
    model_version = metadata.get("model_version")
    ts = metadata.get("trained_at")
    last_trained_at = datetime.fromisoformat(ts) if ts else None


def _copy_primary_to_fallback():
    if os.path.exists(PRIMARY_MODEL_PATH):
        shutil.copy2(PRIMARY_MODEL_PATH, FALLBACK_MODEL_PATH)
    if os.path.exists(PRIMARY_SCALER_PATH):
        shutil.copy2(PRIMARY_SCALER_PATH, FALLBACK_SCALER_PATH)
    if os.path.exists(PRIMARY_META_PATH):
        shutil.copy2(PRIMARY_META_PATH, FALLBACK_META_PATH)


def _is_model_expired():
    if not last_trained_at:
        return True
    age = datetime.utcnow() - last_trained_at
    return age > timedelta(hours=MODEL_MAX_AGE_HOURS)


def _can_trigger_retrain():
    cooldown = RETRAIN_COOLDOWN_SECONDS if is_trained else BOOTSTRAP_RETRY_SECONDS
    return (time.time() - last_retrain_request_ts) >= cooldown


def _record_retrain_trigger():
    global last_retrain_request_ts
    last_retrain_request_ts = time.time()


def load_model_from_disk():
    global training_in_progress
    _ensure_model_dir()
    candidates = [
        (PRIMARY_MODEL_PATH, PRIMARY_SCALER_PATH, PRIMARY_META_PATH, "primary"),
        (FALLBACK_MODEL_PATH, FALLBACK_SCALER_PATH, FALLBACK_META_PATH, "fallback"),
    ]
    for model_path, scaler_path, meta_path, label in candidates:
        if not (os.path.exists(model_path) and os.path.exists(scaler_path)):
            continue
        try:
            loaded_model = joblib.load(model_path)
            loaded_scaler = joblib.load(scaler_path)
            metadata = _load_metadata(meta_path)
            with state_lock:
                _set_runtime_model(loaded_model, loaded_scaler, metadata)
                training_in_progress = False
            logger.info("Loaded %s model version=%s", label, model_version)
            return True
        except Exception as exc:
            logger.error("Failed loading %s model: %s", label, exc)
    return False


def get_metric_value(query):
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5,
        )
        data = response.json()
        if data["status"] == "success" and data["data"]["result"]:
            return float(data["data"]["result"][0]["value"][1])
    except Exception as exc:
        logger.warning("Metric fetch failed: %s", exc)
    return 0.0


def collect_features():
    features = [
        _safe_float(get_metric_value('rate(app_requests_total{status="500"}[1m]) * 100')),
        _safe_float(get_metric_value("rate(app_request_latency_seconds_sum[1m])")),
        _safe_float(get_metric_value("rate(app_requests_total[1m])")),
        _safe_float(get_metric_value("app_error_rate")),
    ]
    logger.info("Features: %s", features)
    return features


def collect_training_data(sample_count=200, sleep_seconds=0.5):
    samples = []
    logger.info("Collecting %d training samples...", sample_count)
    for i in range(sample_count):
        features = collect_features()
        if any(f > 0 for f in features):
            samples.append(features)
        if i % 20 == 0:
            logger.info("Collected %d/%d samples, valid=%d", i, sample_count, len(samples))
        time.sleep(sleep_seconds)
    if len(samples) < 10:
        return None
    return np.array(samples)


def build_candidate_model(training_array):
    next_scaler = StandardScaler()
    scaled_data = next_scaler.fit_transform(training_array)

    next_model = IsolationForest(
        contamination=0.1,
        random_state=42,
        n_estimators=100,
    )
    next_model.fit(scaled_data)

    predictions = next_model.predict(scaled_data)
    anomaly_rate = float(np.mean(predictions == -1))
    if anomaly_rate > 0.40:
        raise ValueError(f"Candidate anomaly rate too high: {anomaly_rate:.2f}")

    metadata = {
        "model_version": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        "trained_at": datetime.utcnow().isoformat(),
        "samples_used": int(len(training_array)),
    }
    return next_model, next_scaler, metadata


def promote_model(next_model, next_scaler, metadata, reason):
    with state_lock:
        if is_trained:
            _copy_primary_to_fallback()
        _set_runtime_model(next_model, next_scaler, metadata)

        _ensure_model_dir()
        joblib.dump(model, PRIMARY_MODEL_PATH)
        joblib.dump(scaler, PRIMARY_SCALER_PATH)
        _save_metadata(PRIMARY_META_PATH, metadata)

    logger.info(
        "Promoted model version=%s reason=%s samples=%s",
        metadata.get("model_version"),
        reason,
        metadata.get("samples_used"),
    )


def training_pipeline(reason):
    global training_in_progress
    with training_lock:
        with state_lock:
            training_in_progress = True
        try:
            training_array = collect_training_data()
            if training_array is None:
                logger.warning("Training skipped: not enough data")
                return {"ok": False, "reason": "not_enough_data"}

            next_model, next_scaler, metadata = build_candidate_model(training_array)
            promote_model(next_model, next_scaler, metadata, reason=reason)
            return {
                "ok": True,
                "model_version": metadata["model_version"],
                "samples_used": metadata["samples_used"],
            }
        except Exception as exc:
            logger.error("Training failed, keeping current model. reason=%s error=%s", reason, exc)
            if not is_trained:
                load_model_from_disk()
            return {"ok": False, "reason": str(exc)}
        finally:
            with state_lock:
                training_in_progress = False


def _background_train(reason):
    logger.info("Background training started. reason=%s", reason)
    training_pipeline(reason=reason)


def trigger_background_training(reason):
    if not _can_trigger_retrain():
        logger.info("Retrain cooldown active; skip reason=%s", reason)
        return False
    _record_retrain_trigger()
    worker = Thread(target=_background_train, args=(reason,), daemon=True)
    worker.start()
    return True


def _drift_check_and_retrain(clean_features):
    global drift_breach_count
    if not is_trained or scaler is None:
        return
    recent_features.append(clean_features)
    if len(recent_features) < max(10, DRIFT_WINDOW_SIZE // 2):
        return

    baseline_mean = scaler.mean_
    baseline_std = np.where(scaler.scale_ < 1e-6, 1.0, scaler.scale_)
    current = np.array(clean_features, dtype=float)
    z = np.abs((current - baseline_mean) / baseline_std)
    avg_z = float(np.mean(z))

    if avg_z >= DRIFT_ZSCORE_THRESHOLD:
        drift_breach_count += 1
    else:
        drift_breach_count = 0

    if drift_breach_count >= DRIFT_CONSECUTIVE_LIMIT:
        triggered = trigger_background_training("drift_detected")
        if triggered:
            logger.warning("Drift detected avg_z=%.3f, triggered retrain", avg_z)
        drift_breach_count = 0


def scheduled_retrain_loop():
    interval_seconds = max(300, RETRAIN_INTERVAL_HOURS * 3600)
    while True:
        time.sleep(interval_seconds)
        trigger_background_training("scheduled_retrain")


@app.on_event("startup")
def startup_event():
    loaded = load_model_from_disk()
    if not loaded:
        logger.warning("No saved model found. Bootstrap training in background.")
        trigger_background_training("startup_no_model")
    elif _is_model_expired():
        logger.warning("Loaded model expired. Refresh training in background.")
        trigger_background_training("startup_model_expired")

    scheduler = Thread(target=scheduled_retrain_loop, daemon=True)
    scheduler.start()


@app.post("/train")
def train_model():
    result = training_pipeline(reason="manual_train")
    if not result["ok"]:
        return {"status": "failed", "error": result["reason"]}
    return {
        "status": "Model trained successfully!",
        "samples_used": result["samples_used"],
        "model_version": result["model_version"],
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/detect")
def detect_anomaly():
    if not is_trained:
        trigger_background_training("detect_no_model")
        return {
            "status": "model_not_trained",
            "message": "Model is bootstrapping in background. Retry shortly.",
            "training_in_progress": training_in_progress,
        }

    features = collect_features()
    clean_features = [_safe_float(f) for f in features]
    logger.info("Clean features for detection: %s", clean_features)

    with state_lock:
        scaled_features = scaler.transform(np.array([clean_features]))
        prediction = model.predict(scaled_features)[0]
        anomaly_score = model.score_samples(scaled_features)[0]

    _drift_check_and_retrain(clean_features)
    is_anomaly = bool(prediction == -1)

    result = {
        "is_anomaly": is_anomaly,
        "anomaly_score": round(float(anomaly_score), 4),
        "current_features": {
            "error_rate": round(features[0], 2),
            "avg_latency": round(features[1], 4),
            "request_rate": round(features[2], 4),
            "error_gauge": round(features[3], 2),
        },
        "timestamp": datetime.utcnow().isoformat(),
        "model_version": model_version,
        "verdict": "ANOMALY DETECTED!" if is_anomaly else "System Normal",
    }

    if is_anomaly:
        logger.error("ANOMALY DETECTED! Score: %.4f", anomaly_score)
    else:
        logger.info("System normal. Score: %.4f", anomaly_score)

    return result


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model_trained": is_trained,
        "training_in_progress": training_in_progress,
        "model_version": model_version,
        "last_trained_at": last_trained_at.isoformat() if last_trained_at else None,
        "model_expired": _is_model_expired() if is_trained else True,
    }


@app.get("/")
def root():
    return {
        "service": "AI Anomaly Detection",
        "status": "running",
        "model_trained": is_trained,
        "training_in_progress": training_in_progress,
        "model_version": model_version,
        "endpoints": ["/train", "/detect", "/health", "/metrics"],
    }


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return generate_latest()