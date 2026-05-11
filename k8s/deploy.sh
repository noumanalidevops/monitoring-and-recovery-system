#!/usr/bin/env bash
set -euo pipefail

echo "Applying namespace..."
kubectl apply -f k8s/namespace.yml

echo "Applying app deployment and autoscaling..."
kubectl apply -f k8s/app-deployment.yml
kubectl apply -f k8s/services.yml
kubectl apply -f k8s/app-hpa.yml

echo "Applying monitoring stack..."
kubectl apply -f k8s/prometheus-deployment.yml
kubectl apply -f k8s/grafana-deployment.yml

echo "Applying AI and auto-healer services..."
kubectl apply -f k8s/ai-service-deployment.yml
kubectl apply -f k8s/auto-healer-deployment.yml

echo "All manifests applied successfully."
echo "Check workloads: kubectl get all -n ai-healing"
