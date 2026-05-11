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
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM
from starlette.responses import PlainTextResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Anomaly Detection Service")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
MODEL_MAX_AGE_HOURS = int(os.getenv("MODEL_MAX_AGE_HOURS", "24"))
RETRAIN_INTERVAL_HOURS = int(os.getenv("RETRAIN_INTERVAL_HOURS", "24"))
RETRAIN_COOLDOWN_SECONDS = int(os.getenv("RETRAIN_COOLDOWN_SECONDS", "900"))
BOOTSTRAP_RETRY_SECONDS = int(os.getenv("BOOTSTRAP_RETRY_SECONDS", "60"))

IFOREST_CONTAMINATION = float(os.getenv("IFOREST_CONTAMINATION", "0.02"))
LOF_CONTAMINATION = float(os.getenv("LOF_CONTAMINATION", "0.02"))
OCSVM_NU = float(os.getenv("OCSVM_NU", "0.02"))
LOF_NEIGHBORS = int(os.getenv("LOF_NEIGHBORS", "20"))

MIN_TRAIN_SAMPLES = int(os.getenv("MIN_TRAIN_SAMPLES", "120"))
TARGET_TRAIN_SAMPLES = int(os.getenv("TARGET_TRAIN_SAMPLES", "360"))
TRAIN_SAMPLE_INTERVAL_SECONDS = float(os.getenv("TRAIN_SAMPLE_INTERVAL_SECONDS", "0.5"))
TRAIN_STABILITY_SECONDS = int(os.getenv("TRAIN_STABILITY_SECONDS", "180"))
STABILITY_THRESHOLD = float(os.getenv("STABILITY_THRESHOLD", "0.78"))
MIN_DATASET_QUALITY = float(os.getenv("MIN_DATASET_QUALITY", "0.70"))

ANOMALY_INDEX_THRESHOLD = float(os.getenv("ANOMALY_INDEX_THRESHOLD", "2.5"))
WARNING_INDEX_THRESHOLD = float(os.getenv("WARNING_INDEX_THRESHOLD", "1.8"))
MAX_ANOMALY_RATE_FOR_RETRAIN = float(os.getenv("MAX_ANOMALY_RATE_FOR_RETRAIN", "0.2"))

PREDICTION_WINDOW_SIZE = 20
FEATURE_TREND_WINDOW = 10
HISTORY_SIZE = 100
COLLECTOR_INTERVAL_SECONDS = 30
SMOOTH_WINDOW = 5
RETRAIN_HISTORY_SIZE = 30

PRIMARY_META_PATH = os.path.join(MODEL_DIR, "metadata.json")
PRIMARY_SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
PRIMARY_IFOREST_PATH = os.path.join(MODEL_DIR, "iforest.pkl")
PRIMARY_LOF_PATH = os.path.join(MODEL_DIR, "lof.pkl")
PRIMARY_OCSVM_PATH = os.path.join(MODEL_DIR, "ocsvm.pkl")
FALLBACK_META_PATH = os.path.join(MODEL_DIR, "last_good_metadata.json")
FALLBACK_SCALER_PATH = os.path.join(MODEL_DIR, "last_good_scaler.pkl")
FALLBACK_IFOREST_PATH = os.path.join(MODEL_DIR, "last_good_iforest.pkl")
FALLBACK_LOF_PATH = os.path.join(MODEL_DIR, "last_good_lof.pkl")
FALLBACK_OCSVM_PATH = os.path.join(MODEL_DIR, "last_good_ocsvm.pkl")

FEATURE_NAMES = ["error_rate", "avg_latency", "request_rate", "error_gauge"]
FEATURE_CLIPS = {
    "error_rate": (0.0, 100.0),
    "avg_latency": (0.0, 2.0),
    "request_rate": (0.0, 500.0),
    "error_gauge": (0.0, 100.0),
}

iforest_model = None
lof_model = None
ocsvm_model = None
scaler = None

is_trained = False
training_in_progress = False
model_version = None
last_trained_at = None
last_retrain_request_ts = 0.0

training_feature_means = {}
training_feature_stds = {}
training_quality = 0.0
samples_used = 0
score_baseline = {
    "iforest_mean": 0.0,
    "iforest_std": 1.0,
    "lof_mean": 0.0,
    "lof_std": 1.0,
    "svm_mean": 0.0,
    "svm_std": 1.0,
}

score_window = deque(maxlen=PREDICTION_WINDOW_SIZE)
detection_history = deque(maxlen=HISTORY_SIZE)
retrain_history = deque(maxlen=RETRAIN_HISTORY_SIZE)
recent_detection_flags = deque(maxlen=200)
feature_reading_history = deque(maxlen=FEATURE_TREND_WINDOW)
feature_smoothing = {name: deque(maxlen=SMOOTH_WINDOW) for name in FEATURE_NAMES}

detection_stats = {
    "total_detections": 0,
    "anomalies_found": 0,
    "warnings_found": 0,
    "critical_found": 0,
    "all_3_agreed": 0,
    "2_of_3_agreed": 0,
}

state_lock = Lock()
training_lock = Lock()


def _safe_float(value):
    if value is None:
        return 0.0
    num = float(value)
    return 0.0 if math.isnan(num) or math.isinf(num) else num


def _clip_feature(name, value):
    lo, hi = FEATURE_CLIPS[name]
    return max(lo, min(hi, _safe_float(value)))


def _smooth_feature(name, value):
    history = feature_smoothing[name]
    history.append(value)
    return float(np.median(list(history)))


def _quality_score(samples):
    if len(samples) < MIN_TRAIN_SAMPLES:
        return 0.0
    arr = np.array(samples, dtype=float)
    variances = np.var(arr, axis=0)
    nonzero_ratio = float(np.mean(np.any(arr > 0, axis=1)))
    stable_ratio = float(np.mean(np.abs(np.diff(arr, axis=0)) < np.maximum(np.std(arr, axis=0), 1e-6)))
    var_score = float(np.mean(np.clip(variances / (variances + 1.0), 0, 1)))
    return round(0.45 * nonzero_ratio + 0.35 * stable_ratio + 0.20 * var_score, 4)


def _save_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _ensure_model_dir():
    os.makedirs(MODEL_DIR, exist_ok=True)


def _record_retrain(reason, status, details):
    retrain_history.appendleft({
        "timestamp": datetime.utcnow().isoformat(),
        "reason": reason,
        "status": status,
        "details": details,
    })


def _is_model_expired():
    if not last_trained_at:
        return True
    return (datetime.utcnow() - last_trained_at) > timedelta(hours=MODEL_MAX_AGE_HOURS)


def _is_retrain_eligible(reason):
    now = time.time()
    cooldown = RETRAIN_COOLDOWN_SECONDS if is_trained else BOOTSTRAP_RETRY_SECONDS
    if (now - last_retrain_request_ts) < cooldown:
        return False, "cooldown_active"
    if reason == "scheduled_retrain":
        recent = list(recent_detection_flags)
        if recent:
            anomaly_rate = float(np.mean(recent))
            if anomaly_rate > MAX_ANOMALY_RATE_FOR_RETRAIN:
                return False, f"anomaly_rate_too_high:{anomaly_rate:.2f}"
    return True, "ok"


def _copy_primary_to_fallback():
    pairs = [
        (PRIMARY_META_PATH, FALLBACK_META_PATH),
        (PRIMARY_SCALER_PATH, FALLBACK_SCALER_PATH),
        (PRIMARY_IFOREST_PATH, FALLBACK_IFOREST_PATH),
        (PRIMARY_LOF_PATH, FALLBACK_LOF_PATH),
        (PRIMARY_OCSVM_PATH, FALLBACK_OCSVM_PATH),
    ]
    for src, dst in pairs:
        if os.path.exists(src):
            shutil.copy2(src, dst)


def _set_runtime_bundle(bundle):
    global iforest_model, lof_model, ocsvm_model, scaler
    global is_trained, model_version, last_trained_at
    global training_feature_means, training_feature_stds, training_quality, samples_used, score_baseline

    iforest_model = bundle["iforest_model"]
    lof_model = bundle["lof_model"]
    ocsvm_model = bundle["ocsvm_model"]
    scaler = bundle["scaler"]
    metadata = bundle["metadata"]

    is_trained = True
    model_version = metadata.get("model_version")
    ts = metadata.get("trained_at")
    last_trained_at = datetime.fromisoformat(ts) if ts else None
    training_feature_means = metadata.get("feature_means", {})
    training_feature_stds = metadata.get("feature_stds", {})
    training_quality = float(metadata.get("dataset_quality", 0.0))
    samples_used = int(metadata.get("samples_used", 0))
    score_baseline = metadata.get("score_baseline", score_baseline)


def _bundle_from_paths(meta, sc, ifp, lfp, svp):
    if not all(os.path.exists(p) for p in [meta, sc, ifp, lfp, svp]):
        return None
    return {
        "metadata": _load_json(meta),
        "scaler": joblib.load(sc),
        "iforest_model": joblib.load(ifp),
        "lof_model": joblib.load(lfp),
        "ocsvm_model": joblib.load(svp),
    }


def load_model_from_disk():
    global training_in_progress
    _ensure_model_dir()
    for label, meta, sc, ifp, lfp, svp in [
        ("primary", PRIMARY_META_PATH, PRIMARY_SCALER_PATH, PRIMARY_IFOREST_PATH, PRIMARY_LOF_PATH, PRIMARY_OCSVM_PATH),
        ("fallback", FALLBACK_META_PATH, FALLBACK_SCALER_PATH, FALLBACK_IFOREST_PATH, FALLBACK_LOF_PATH, FALLBACK_OCSVM_PATH),
    ]:
        try:
            bundle = _bundle_from_paths(meta, sc, ifp, lfp, svp)
            if not bundle:
                continue
            with state_lock:
                _set_runtime_bundle(bundle)
                training_in_progress = False
            logger.info("Loaded %s model version=%s", label, model_version)
            return True
        except Exception as exc:
            logger.error("Failed loading %s bundle: %s", label, exc)
    return False


def _save_primary_bundle():
    _ensure_model_dir()
    joblib.dump(scaler, PRIMARY_SCALER_PATH)
    joblib.dump(iforest_model, PRIMARY_IFOREST_PATH)
    joblib.dump(lof_model, PRIMARY_LOF_PATH)
    joblib.dump(ocsvm_model, PRIMARY_OCSVM_PATH)
    metadata = {
        "model_version": model_version,
        "trained_at": last_trained_at.isoformat() if last_trained_at else None,
        "samples_used": samples_used,
        "dataset_quality": training_quality,
        "feature_means": training_feature_means,
        "feature_stds": training_feature_stds,
        "score_baseline": score_baseline,
        "sensitivity": {
            "iforest_contamination": IFOREST_CONTAMINATION,
            "lof_contamination": LOF_CONTAMINATION,
            "ocsvm_nu": OCSVM_NU,
        },
    }
    _save_json(PRIMARY_META_PATH, metadata)


def _metric(query):
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("data", {}).get("result", [])
        if payload.get("status") == "success" and result:
            return float(result[0]["value"][1]), False
    except Exception as exc:
        logger.warning("Metric query failed: %s (%s)", query, exc)
    return 0.0, True


def collect_features():
    queries = [
        ('rate(app_requests_total{status="500"}[1m]) * 100', "error_rate"),
        ("rate(app_request_latency_seconds_sum[1m])", "avg_latency"),
        ("rate(app_requests_total[1m])", "request_rate"),
        ("app_error_rate", "error_gauge"),
    ]
    values = []
    null_count = 0
    for query, name in queries:
        raw, is_null = _metric(query)
        if is_null:
            null_count += 1
        clipped = _clip_feature(name, raw)
        smooth = _smooth_feature(name, clipped)
        values.append(smooth)
    return values, {"null_metric_count": null_count, "smoothed": True}


def _features_to_map(values):
    return {
        "error_rate": round(values[0], 4),
        "avg_latency": round(values[1], 6),
        "request_rate": round(values[2], 6),
        "error_gauge": round(values[3], 4),
    }


def collect_training_data(sample_count=TARGET_TRAIN_SAMPLES, sleep_seconds=TRAIN_SAMPLE_INTERVAL_SECONDS):
    samples = []
    quality_flags = {"null_metric_hits": 0, "rejected_samples": 0}
    started = time.time()
    logger.info("Collecting training samples target=%d", sample_count)
    for idx in range(sample_count):
        values, info = collect_features()
        quality_flags["null_metric_hits"] += int(info["null_metric_count"] > 0)
        if np.std(values) < 1e-5 and np.mean(values) < 1e-4:
            quality_flags["rejected_samples"] += 1
        else:
            samples.append(values)
        if idx % 40 == 0:
            logger.info("Samples %d/%d valid=%d", idx, sample_count, len(samples))
        time.sleep(sleep_seconds)
    duration = time.time() - started
    dataset_quality = _quality_score(samples)
    return np.array(samples, dtype=float), {
        "duration_seconds": round(duration, 2),
        "dataset_quality": dataset_quality,
        **quality_flags,
    }


def _stability_check():
    points = []
    rounds = max(1, int(TRAIN_STABILITY_SECONDS / COLLECTOR_INTERVAL_SECONDS))
    for _ in range(rounds):
        values, _ = collect_features()
        points.append(values)
        time.sleep(COLLECTOR_INTERVAL_SECONDS)
    arr = np.array(points, dtype=float) if points else np.zeros((1, len(FEATURE_NAMES)))
    if len(arr) <= 1:
        return 0.0
    mean_std = float(np.mean(np.std(arr, axis=0)))
    score = float(np.clip(1.0 - (mean_std / 10.0), 0.0, 1.0))
    return round(score, 4)


def build_candidate_models(training_array, dataset_quality):
    next_scaler = StandardScaler()
    scaled = next_scaler.fit_transform(training_array)

    next_iforest = IsolationForest(
        contamination=IFOREST_CONTAMINATION,
        random_state=42,
        n_estimators=250,
    )
    next_iforest.fit(scaled)

    next_lof = LocalOutlierFactor(
        n_neighbors=LOF_NEIGHBORS,
        contamination=LOF_CONTAMINATION,
        novelty=True,
    )
    next_lof.fit(scaled)

    next_ocsvm = OneClassSVM(kernel="rbf", gamma="scale", nu=OCSVM_NU)
    next_ocsvm.fit(scaled)

    if_scores = next_iforest.score_samples(scaled)
    lof_scores = next_lof.score_samples(scaled)
    svm_scores = next_ocsvm.score_samples(scaled)

    means = np.mean(training_array, axis=0)
    stds = np.std(training_array, axis=0)
    safe_stds = np.where(stds < 1e-9, 1e-9, stds)
    metadata = {
        "model_version": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        "trained_at": datetime.utcnow().isoformat(),
        "samples_used": int(len(training_array)),
        "dataset_quality": dataset_quality,
        "feature_means": {FEATURE_NAMES[i]: float(means[i]) for i in range(len(FEATURE_NAMES))},
        "feature_stds": {FEATURE_NAMES[i]: float(safe_stds[i]) for i in range(len(FEATURE_NAMES))},
        "score_baseline": {
            "iforest_mean": float(np.mean(if_scores)),
            "iforest_std": float(max(np.std(if_scores), 1e-6)),
            "lof_mean": float(np.mean(lof_scores)),
            "lof_std": float(max(np.std(lof_scores), 1e-6)),
            "svm_mean": float(np.mean(svm_scores)),
            "svm_std": float(max(np.std(svm_scores), 1e-6)),
        },
    }
    return next_iforest, next_lof, next_ocsvm, next_scaler, metadata


def promote_models(iforest_next, lof_next, ocsvm_next, scaler_next, metadata, reason):
    global model_version, last_trained_at, samples_used
    global training_feature_means, training_feature_stds, training_quality, score_baseline
    global iforest_model, lof_model, ocsvm_model, scaler, is_trained

    with state_lock:
        if is_trained:
            _copy_primary_to_fallback()
        iforest_model = iforest_next
        lof_model = lof_next
        ocsvm_model = ocsvm_next
        scaler = scaler_next
        model_version = metadata["model_version"]
        last_trained_at = datetime.fromisoformat(metadata["trained_at"])
        samples_used = int(metadata["samples_used"])
        training_quality = float(metadata["dataset_quality"])
        training_feature_means = metadata["feature_means"]
        training_feature_stds = metadata["feature_stds"]
        score_baseline = metadata["score_baseline"]
        is_trained = True
        _save_primary_bundle()

    _record_retrain(reason, "success", {
        "model_version": model_version,
        "samples_used": samples_used,
        "dataset_quality": training_quality,
    })


def training_pipeline(reason):
    global training_in_progress, last_retrain_request_ts
    with training_lock:
        with state_lock:
            training_in_progress = True
        try:
            stability_score = _stability_check()
            if stability_score < STABILITY_THRESHOLD:
                _record_retrain(reason, "skipped", {"reason": "unstable_system", "stability_score": stability_score})
                return {"ok": False, "reason": f"unstable_system:{stability_score}"}

            training_array, quality_report = collect_training_data()
            quality = quality_report["dataset_quality"]
            if len(training_array) < MIN_TRAIN_SAMPLES:
                _record_retrain(reason, "skipped", {"reason": "not_enough_samples", **quality_report})
                return {"ok": False, "reason": "not_enough_data"}
            if quality < MIN_DATASET_QUALITY:
                _record_retrain(reason, "skipped", {"reason": "poor_data_quality", **quality_report})
                return {"ok": False, "reason": f"poor_data_quality:{quality}"}

            bundle = build_candidate_models(training_array, quality)
            promote_models(*bundle, reason)
            last_retrain_request_ts = time.time()
            return {
                "ok": True,
                "model_version": model_version,
                "samples_used": samples_used,
                "dataset_quality": quality,
                "stability_score": stability_score,
            }
        except Exception as exc:
            logger.error("Training failed reason=%s error=%s", reason, exc)
            _record_retrain(reason, "failed", {"error": str(exc)})
            if not is_trained:
                load_model_from_disk()
            return {"ok": False, "reason": str(exc)}
        finally:
            with state_lock:
                training_in_progress = False


def _background_train(reason):
    logger.info("Background training started reason=%s", reason)
    training_pipeline(reason)


def trigger_background_training(reason):
    eligible, note = _is_retrain_eligible(reason)
    if not eligible:
        _record_retrain(reason, "skipped", {"reason": note})
        return False
    worker = Thread(target=_background_train, args=(reason,), daemon=True)
    worker.start()
    return True


def _normalized_index(score, mean_key, std_key):
    mean = float(score_baseline.get(mean_key, 0.0))
    std = float(max(score_baseline.get(std_key, 1.0), 1e-6))
    return max(0.0, (mean - score) / std)


def _severity(votes, anomaly_index):
    if votes == 3 or anomaly_index >= 4.0:
        return "CRITICAL"
    if anomaly_index >= 2.8:
        return "HIGH"
    if votes >= 2 or anomaly_index >= WARNING_INDEX_THRESHOLD:
        return "WARNING"
    return "NORMAL"


def _ensemble_detect(values):
    with state_lock:
        local_scaler = scaler
        local_iforest = iforest_model
        local_lof = lof_model
        local_ocsvm = ocsvm_model
        local_version = model_version

    if not all([local_scaler, local_iforest, local_lof, local_ocsvm]):
        return None

    x = np.array([values], dtype=float)
    x_scaled = local_scaler.transform(x)
    if_pred = int(local_iforest.predict(x_scaled)[0])
    lof_pred = int(local_lof.predict(x_scaled)[0])
    svm_pred = int(local_ocsvm.predict(x_scaled)[0])
    if_score = float(local_iforest.score_samples(x_scaled)[0])
    lof_score = float(local_lof.score_samples(x_scaled)[0])
    svm_score = float(local_ocsvm.score_samples(x_scaled)[0])

    votes = sum([if_pred == -1, lof_pred == -1, svm_pred == -1])
    agreement_count = max(votes, 3 - votes)
    idx_if = _normalized_index(if_score, "iforest_mean", "iforest_std")
    idx_lof = _normalized_index(lof_score, "lof_mean", "lof_std")
    idx_svm = _normalized_index(svm_score, "svm_mean", "svm_std")
    anomaly_index = float(np.mean([idx_if, idx_lof, idx_svm]))
    confirmed = bool(votes == 3 or (votes >= 2 and anomaly_index >= ANOMALY_INDEX_THRESHOLD))
    is_warning = bool((votes >= 2 and not confirmed) or (anomaly_index >= WARNING_INDEX_THRESHOLD and not confirmed))
    severity = _severity(votes, anomaly_index)
    confidence_pct = float(np.clip((votes / 3.0) * 65 + min(anomaly_index / 5.0, 1.0) * 35, 0, 100))
    verdict = "System Normal"
    if confirmed:
        verdict = "ANOMALY DETECTED!"
    elif is_warning:
        verdict = "WARNING: POTENTIAL ANOMALY"

    return {
        "is_anomaly": confirmed,
        "is_warning": is_warning,
        "severity": severity,
        "anomaly_score": round(anomaly_index, 4),
        "confidence_text": f"{agreement_count}/3 models agree",
        "confidence_percent": round(confidence_pct, 2),
        "agreement_count": agreement_count,
        "anomaly_votes": votes,
        "model_version": local_version,
        "thresholds": {
            "warning_index_threshold": WARNING_INDEX_THRESHOLD,
            "confirmed_index_threshold": ANOMALY_INDEX_THRESHOLD,
        },
        "verdict": verdict,
        "individual_models": {
            "isolation_forest": {"prediction": "anomaly" if if_pred == -1 else "normal", "score": round(if_score, 4), "normalized_index": round(idx_if, 3)},
            "local_outlier_factor": {"prediction": "anomaly" if lof_pred == -1 else "normal", "score": round(lof_score, 4), "normalized_index": round(idx_lof, 3)},
            "one_class_svm": {"prediction": "anomaly" if svm_pred == -1 else "normal", "score": round(svm_score, 4), "normalized_index": round(idx_svm, 3)},
        },
    }


def _append_feature_reading(values):
    feature_reading_history.append({
        "timestamp": datetime.utcnow().isoformat(),
        "features": _features_to_map(values),
    })


def _update_detection_stats(detection):
    detection_stats["total_detections"] += 1
    if detection["is_anomaly"]:
        detection_stats["anomalies_found"] += 1
    if detection["is_warning"]:
        detection_stats["warnings_found"] += 1
    if detection["severity"] == "CRITICAL":
        detection_stats["critical_found"] += 1
    if detection["agreement_count"] == 3:
        detection_stats["all_3_agreed"] += 1
    elif detection["agreement_count"] == 2:
        detection_stats["2_of_3_agreed"] += 1
    recent_detection_flags.append(1 if detection["is_anomaly"] else 0)


def _collector_loop():
    while True:
        try:
            if is_trained:
                values, _ = collect_features()
                _append_feature_reading(values)
                detection = _ensemble_detect(values)
                if detection:
                    score_window.append(detection["anomaly_score"])
        except Exception as exc:
            logger.warning("Background collector error: %s", exc)
        time.sleep(COLLECTOR_INTERVAL_SECONDS)


def _scheduled_retrain_loop():
    interval_seconds = max(1800, RETRAIN_INTERVAL_HOURS * 3600)
    while True:
        time.sleep(interval_seconds)
        trigger_background_training("scheduled_retrain")


@app.on_event("startup")
def startup_event():
    loaded = load_model_from_disk()
    if not loaded:
        _record_retrain("startup", "info", {"message": "no_saved_model"})
        trigger_background_training("startup_no_model")
    elif _is_model_expired():
        trigger_background_training("startup_model_expired")
    Thread(target=_collector_loop, daemon=True).start()
    Thread(target=_scheduled_retrain_loop, daemon=True).start()


@app.post("/train")
def train_model():
    result = training_pipeline("manual_train")
    if not result["ok"]:
        return {"status": "failed", "error": result["reason"]}
    return {
        "status": "Model trained successfully!",
        "samples_used": result["samples_used"],
        "dataset_quality": result["dataset_quality"],
        "stability_score": result["stability_score"],
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
    values, info = collect_features()
    _append_feature_reading(values)
    detection = _ensemble_detect(values)
    if detection is None:
        return {
            "status": "model_not_trained",
            "message": "Model is bootstrapping in background. Retry shortly.",
            "training_in_progress": training_in_progress,
        }

    score_window.append(detection["anomaly_score"])
    _update_detection_stats(detection)
    result = {
        "is_anomaly": detection["is_anomaly"],
        "anomaly_score": detection["anomaly_score"],
        "current_features": _features_to_map(values),
        "timestamp": datetime.utcnow().isoformat(),
        "model_version": detection["model_version"],
        "verdict": detection["verdict"],
        "confidence": detection["confidence_text"],
        "confidence_percent": detection["confidence_percent"],
        "severity": detection["severity"],
        "is_warning": detection["is_warning"],
        "thresholds": detection["thresholds"],
        "smoothing_enabled": info["smoothed"],
        "null_metric_count": info["null_metric_count"],
        "individual_models": detection["individual_models"],
    }
    detection_history.appendleft({
        "timestamp": result["timestamp"],
        "is_anomaly": result["is_anomaly"],
        "is_warning": result["is_warning"],
        "severity": result["severity"],
        "anomaly_score": result["anomaly_score"],
        "confidence": result["confidence"],
        "confidence_percent": result["confidence_percent"],
        "verdict": "ANOMALY" if result["is_anomaly"] else ("WARNING" if result["is_warning"] else "NORMAL"),
        "features": result["current_features"],
        "models_voted": {
            "isolation_forest": result["individual_models"]["isolation_forest"]["prediction"],
            "local_outlier_factor": result["individual_models"]["local_outlier_factor"]["prediction"],
            "one_class_svm": result["individual_models"]["one_class_svm"]["prediction"],
        },
    })
    return result


@app.get("/explain")
def explain_anomaly():
    values, _ = collect_features()
    detection = _ensemble_detect(values) if is_trained else None
    means = dict(training_feature_means) if training_feature_means else {name: 0.0 for name in FEATURE_NAMES}
    stds = dict(training_feature_stds) if training_feature_stds else {name: 1.0 for name in FEATURE_NAMES}

    def severity_for(z_score):
        if z_score > 10:
            return "CRITICAL"
        if z_score >= 5:
            return "HIGH"
        if z_score >= 2:
            return "MEDIUM"
        return "NORMAL"

    deviations = {}
    ranked = []
    for idx, name in enumerate(FEATURE_NAMES):
        mean = float(means.get(name, 0.0))
        std = max(float(stds.get(name, 1.0)), 1e-9)
        current = float(values[idx])
        z = abs((current - mean) / std)
        pct = 0.0 if abs(mean) < 1e-9 else ((current - mean) / abs(mean)) * 100.0
        item = {
            "current": round(current, 4),
            "baseline_mean": round(mean, 4),
            "baseline_std": round(std, 4),
            "z_score": round(z, 4),
            "deviation": f"{abs(round(pct, 1))}% {'above' if pct >= 0 else 'below'} normal",
            "deviation_percent": f"{abs(round(pct, 1))}% {'above' if pct >= 0 else 'below'} normal",
            "severity": severity_for(z),
        }
        deviations[name] = item
        ranked.append({"feature": name, "z_score": item["z_score"], "severity": item["severity"], "deviation": item["deviation"]})

    ranked = sorted(ranked, key=lambda x: x["z_score"], reverse=True)
    top = ranked[0]["feature"] if ranked else "N/A"
    return {
        "is_currently_anomalous": bool(detection["is_anomaly"]) if detection else False,
        "top_contributor": top,
        "explanation": f"{top} is {deviations[top]['deviation']}" if top != "N/A" else "No feature deviation data",
        "feature_deviations": deviations,
        "ranked_contributors": ranked,
    }


@app.get("/predict")
def predict_trend():
    if len(feature_reading_history) < 3:
        return {
            "trend": "stable",
            "anomaly_risk": "low",
            "prediction": "Insufficient data for trend forecast",
            "confidence": 0.0,
            "based_on_readings": len(feature_reading_history),
        }
    rows = list(feature_reading_history)[-10:]
    scores = []
    for row in rows:
        f = row["features"]
        mean_er = float(training_feature_means.get("error_rate", 1.0) or 1.0)
        mean_lat = float(training_feature_means.get("avg_latency", 0.1) or 0.1)
        stress = (f["error_rate"] / max(mean_er, 1e-6)) * 0.6 + (f["avg_latency"] / max(mean_lat, 1e-6)) * 0.4
        scores.append(stress)
    slope = float(np.polyfit(range(len(scores)), scores, 1)[0])
    current_score = float(scores[-1])

    if slope > 0.12:
        trend = "degrading"
        risk = "high"
        prediction = "Anomaly likely in 2-3 minutes"
    elif slope > 0.04:
        trend = "degrading"
        risk = "medium"
        prediction = "Potential anomaly risk in next 5 minutes"
    elif slope < -0.04:
        trend = "improving"
        risk = "low"
        prediction = "System moving toward stable baseline"
    else:
        trend = "stable"
        risk = "low"
        prediction = "No immediate anomaly trend detected"
    confidence = float(np.clip(abs(slope) * 4 + min(current_score / 5, 0.3), 0.3, 0.95))
    return {
        "trend": trend,
        "anomaly_risk": risk,
        "prediction": prediction,
        "confidence": round(confidence, 2),
        "based_on_readings": len(rows),
        "slope": round(slope, 4),
    }


@app.get("/stats")
def model_stats():
    total = detection_stats["total_detections"]
    anomalies = detection_stats["anomalies_found"]
    warnings = detection_stats["warnings_found"]
    normal = max(0, total - anomalies - warnings)
    false_positive_est = 0.0 if total == 0 else (warnings / total) * 100.0
    return {
        "training_data": {
            "samples_used": samples_used,
            "dataset_quality": training_quality,
            "feature_means": training_feature_means,
            "feature_stds": training_feature_stds,
            "trained_at": last_trained_at.isoformat() if last_trained_at else None,
        },
        "models": {
            "isolation_forest": {"trained": iforest_model is not None, "contamination": IFOREST_CONTAMINATION},
            "local_outlier_factor": {"trained": lof_model is not None, "n_neighbors": LOF_NEIGHBORS, "contamination": LOF_CONTAMINATION},
            "one_class_svm": {"trained": ocsvm_model is not None, "kernel": "rbf", "nu": OCSVM_NU},
        },
        "detection_stats": {
            "total_detections": total,
            "anomalies_found": anomalies,
            "warnings_found": warnings,
            "normal_found": normal,
            "anomaly_rate": f"{round((anomalies / total) * 100, 2) if total else 0}%",
            "false_positive_estimate": f"{round(false_positive_est, 2)}%",
            "model_agreement": {
                "all_3_agreed": detection_stats["all_3_agreed"],
                "2_of_3_agreed": detection_stats["2_of_3_agreed"],
            },
        },
        "retrain_history": list(retrain_history),
    }


@app.get("/history")
def history():
    rows = list(detection_history)
    anomaly_count = sum(1 for row in rows if row.get("is_anomaly"))
    return {
        "total_records": len(rows),
        "anomaly_count": anomaly_count,
        "normal_count": len(rows) - anomaly_count,
        "history": rows,
    }


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
        "endpoints": ["/train", "/detect", "/health", "/metrics", "/explain", "/predict", "/stats", "/history"],
    }


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return generate_latest()
