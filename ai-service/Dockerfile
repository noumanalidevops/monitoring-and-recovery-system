FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Seedha install karo — requirements.txt pe depend mat karo
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    "uvicorn[standard]==0.24.0" \
    scikit-learn==1.3.2 \
    numpy==1.26.2 \
    requests==2.31.0 \
    prometheus-client==0.19.0 \
    joblib==1.5.3

COPY . .

EXPOSE 8001

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]