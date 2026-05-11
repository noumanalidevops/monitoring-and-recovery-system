from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from starlette.responses import PlainTextResponse
from datetime import datetime
import json
import logging
import os
import random
import requests
import time

# ---- Logging Setup ----
# Ye logs banayega jo ELK Stack collect karega
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "fastapi-app",
            "message": record.getMessage(),
            "logger": record.name
        }
        return json.dumps(log_data)

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

app = FastAPI(title="Sample App")
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai-service:8001")
AUTO_HEALER_URL = os.getenv("AUTO_HEALER_URL", "http://auto-healer:8002")

# ---- Prometheus Metrics Define Karo ----
# Counter = sirf badhta hai (requests kitni aayi)
REQUEST_COUNT = Counter(
    'app_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status']
)

# Histogram = time measure karta hai
REQUEST_LATENCY = Histogram(
    'app_request_latency_seconds',
    'Request latency in seconds'
)

# Gauge = upar neeche ja sakta hai (CPU jaisa)
ERROR_GAUGE = Gauge(
    'app_error_rate',
    'Current error rate percentage'
)

# ---- Normal Endpoints ----
@app.get("/")
def home():
    logger.info("Home page accessed")
    REQUEST_COUNT.labels('GET', '/', '200').inc()
    return {"status": "App is running!", "message": "Welcome!"}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    with open("templates/dashboard.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/data")
def get_data():
    # Yahan hum intentionally kabhi kabhi slow/error bana rahe hain
    # Real app mein ye database calls hoti hain
    
    start = time.time()
    
    # 20% chance error ka
    if random.random() < 0.2:
        logger.error("Database connection failed!")
        REQUEST_COUNT.labels('GET', '/data', '500').inc()
        ERROR_GAUGE.set(random.uniform(20, 80))  # Error rate badha do
        return {"error": "Internal Server Error"}, 500
    
    # Normal case - thoda random delay
    time.sleep(random.uniform(0.01, 0.1))
    
    duration = time.time() - start
    REQUEST_LATENCY.observe(duration)
    REQUEST_COUNT.labels('GET', '/data', '200').inc()
    ERROR_GAUGE.set(random.uniform(0, 5))  # Normal error rate
    
    logger.info(f"Data fetched successfully in {duration:.3f}s")
    return {"data": [1, 2, 3, 4, 5], "latency": duration}

@app.get("/health")
def health_check():
    # Kubernetes aur Docker isko use karte hain check karne ke liye
    # ke app zinda hai ya nahi
    return {"status": "healthy"}


def _safe_get_json(url, timeout=4):
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            return response.json()
    except Exception:
        return None
    return None


def _derive_incidents_from_history(history_payload):
    history = (history_payload or {}).get("history", [])
    incidents = []
    for item in history:
        if not item.get("is_anomaly"):
            continue
        score = float(item.get("anomaly_score", 0))
        action = "ALERT"
        if score <= -0.8:
            action = "RESTART"
        elif score <= -0.6:
            action = "SCALE_UP"
        elif score <= -0.4:
            action = "CIRCUIT_BREAKER"
        incidents.append({
            "timestamp": item.get("timestamp"),
            "status": "open",
            "action": action,
            "duration": "auto",
            "score": score,
        })
        if len(incidents) >= 20:
            break
    return incidents


@app.get("/api/ai/health")
def ai_health_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/health")
    if payload is None:
        return {"status": "down", "model_trained": False, "training_in_progress": False}
    return payload


@app.get("/api/ai/detect")
def ai_detect_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/detect")
    if payload is None:
        return {"status": "unavailable", "message": "AI detect unavailable"}
    return payload


@app.get("/api/ai/explain")
def ai_explain_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/explain")
    if payload is None:
        return {"feature_deviations": {}, "top_contributor": "N/A"}
    return payload


@app.get("/api/ai/predict")
def ai_predict_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/predict")
    if payload is None:
        return {
            "trend": "stable",
            "anomaly_risk": "low",
            "prediction": "Prediction unavailable",
            "confidence": 0.0
        }
    return payload


@app.get("/api/ai/stats")
def ai_stats_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/stats")
    if payload is None:
        return {"detection_stats": {"total_detections": 0, "anomaly_rate": "0%"}}
    return payload


@app.get("/api/ai/history")
def ai_history_proxy():
    payload = _safe_get_json(f"{AI_SERVICE_URL}/history")
    if payload is None:
        return {"history": [], "total_records": 0}
    return payload


@app.post("/api/ai/train")
def ai_train_proxy():
    try:
        response = requests.post(f"{AI_SERVICE_URL}/train", timeout=900)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return {"status": "failed", "error": "AI training request failed"}


@app.get("/api/healer/health")
def healer_health_proxy():
    payload = _safe_get_json(f"{AUTO_HEALER_URL}/health")
    if payload is not None:
        return payload
    ai_health = _safe_get_json(f"{AI_SERVICE_URL}/health") or {}
    return {
        "status": "active" if ai_health else "down",
        "source": "derived",
        "checked_at": datetime.utcnow().isoformat()
    }


@app.get("/api/healer/incidents")
def healer_incidents_proxy():
    payload = _safe_get_json(f"{AUTO_HEALER_URL}/incidents")
    history_payload = _safe_get_json(f"{AI_SERVICE_URL}/history") or {}
    predicted_actions = _derive_incidents_from_history(history_payload)
    if payload is not None:
        real_actions = payload.get("incidents") if isinstance(payload, dict) else payload
        if not isinstance(real_actions, list):
            real_actions = []
        return {
            "real_actions": real_actions,
            "predicted_actions": predicted_actions,
            "real_available": True,
            "counts": {
                "real_actions": len(real_actions),
                "predicted_actions": len(predicted_actions),
            },
            "source": "auto_healer_plus_ai_history"
        }
    return {
        "real_actions": [],
        "predicted_actions": predicted_actions,
        "real_available": False,
        "counts": {
            "real_actions": 0,
            "predicted_actions": len(predicted_actions),
        },
        "source": "derived_from_ai_history"
    }

# ---- Prometheus Metrics Endpoint ----
@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    # Prometheus yahan se data collect karega
    return generate_latest()