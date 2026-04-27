from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from starlette.responses import PlainTextResponse
import json
import logging
import random
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

# ---- Prometheus Metrics Endpoint ----
@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    # Prometheus yahan se data collect karega
    return generate_latest()