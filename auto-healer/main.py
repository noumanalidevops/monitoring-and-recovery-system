# -*- coding: utf-8 -*-
import requests
import time
import logging
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---- Configuration ----
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai-service:8001")
APP_URL = os.getenv("APP_URL", "http://app:8000")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))
EMAIL_HOST = os.getenv("EMAIL_HOST", "")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "")
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "true").lower() == "true"

# Cooldown tracking
last_action_time = {}
COOLDOWN_SECONDS = 300  # 5 minutes

def send_slack_alert(message, is_critical=False):
    """Slack pe notification bhejo"""
    if not SLACK_WEBHOOK:
        logger.info("[ALERT - No Slack configured] %s", message)
        return

    prefix = "[CRITICAL]" if is_critical else "[WARNING]"
    payload = {
        "text": "{} Auto-Healer Alert\n{}\nTime: {}".format(
            prefix, message, datetime.now()
        )
    }

    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
        logger.info("Slack alert sent!")
    except Exception as e:
        logger.error("Slack alert failed: %s", e)


def send_email_alert(message, is_critical=False):
    """Email alert bhejo via SMTP"""
    if not all([EMAIL_HOST, ALERT_FROM_EMAIL, ALERT_TO_EMAIL]):
        logger.info("[ALERT - No Email configured] %s", message)
        return

    subject_prefix = "[CRITICAL]" if is_critical else "[WARNING]"
    subject = "{} Auto-Healer Alert".format(subject_prefix)
    body = "{}\n\nTime: {}".format(message, datetime.now())

    mime_message = MIMEText(body, "plain", "utf-8")
    mime_message["Subject"] = subject
    mime_message["From"] = ALERT_FROM_EMAIL
    mime_message["To"] = ALERT_TO_EMAIL

    try:
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=10) as server:
            if EMAIL_USE_TLS:
                server.starttls()
            if EMAIL_USERNAME and EMAIL_PASSWORD:
                server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(ALERT_FROM_EMAIL, [ALERT_TO_EMAIL], mime_message.as_string())
        logger.info("Email alert sent!")
    except Exception as e:
        logger.error("Email alert failed: %s", e)


def send_alert(message, is_critical=False):
    """Configured channels par alert bhejo"""
    send_slack_alert(message, is_critical=is_critical)
    send_email_alert(message, is_critical=is_critical)


def restart_app():
    """App health check karo aur restart attempt karo"""
    logger.warning("Attempting to restart app service...")
    try:
        response = requests.get("{}/health".format(APP_URL), timeout=5)
        if response.status_code == 200:
            logger.info("App is responding. Health check passed.")
            logger.info("In production Kubernetes: kubectl rollout restart deployment/app")
    except Exception as e:
        logger.error("App unreachable: %s. Docker will auto-restart it.", e)


def check_cooldown(action_name):
    """
    Cooldown check — baar baar same action na ho
    Returns True agar action allowed hai
    """
    if action_name in last_action_time:
        elapsed = time.time() - last_action_time[action_name]
        if elapsed < COOLDOWN_SECONDS:
            remaining = COOLDOWN_SECONDS - elapsed
            logger.info("Cooldown active for '%s'. %.0fs remaining.", action_name, remaining)
            return False
    return True


def handle_anomaly(detection_result):
    """Anomaly detect hone pe remediation actions lo"""
    features = detection_result.get("current_features", {})
    score = detection_result.get("anomaly_score", 0)

    logger.error("*** ANOMALY DETECTED! Score: %s ***", score)
    logger.error("Features at time of anomaly: %s", features)

    # ---- Action 1: Alert (HAMESHA) ----
    alert_msg = (
        "Anomaly detected!\n"
        "Score: {}\n"
        "Error Rate: {:.1f}%\n"
        "Avg Latency: {:.1f}ms\n"
        "Error Gauge: {:.1f}%"
    ).format(
        score,
        features.get("error_rate", 0),
        features.get("avg_latency", 0) * 1000,
        features.get("error_gauge", 0)
    )
    send_alert(alert_msg, is_critical=True)

    # ---- Action 2: High Error Rate — Restart ----
    if features.get("error_rate", 0) > 30:
        if check_cooldown("restart"):
            logger.warning("High error rate detected! Triggering restart...")
            restart_app()
            last_action_time["restart"] = time.time()
            send_alert("Auto-restart triggered due to high error rate!")

    # ---- Action 3: High Latency — Alert ----
    if features.get("avg_latency", 0) > 0.5:
        logger.warning(
            "High latency detected: %.0fms",
            features.get("avg_latency", 0) * 1000
        )
        if check_cooldown("latency_alert"):
            send_alert(
                "High latency: {:.0f}ms".format(
                    features.get("avg_latency", 0) * 1000
                )
            )
            last_action_time["latency_alert"] = time.time()


def main_loop():
    """Main monitoring loop — hamesha chalata rahega"""
    logger.info("Auto-Healer started!")
    logger.info("Checking every %d seconds...", CHECK_INTERVAL)

    # AI service ready hone ka wait karo
    logger.info("Waiting 15 seconds for AI service to be ready...")
    time.sleep(15)

    consecutive_anomalies = 0

    while True:
        try:
            response = requests.get(
                "{}/detect".format(AI_SERVICE_URL),
                timeout=10
            )

            if response.status_code == 200:
                result = response.json()

                # Model abhi train nahi hua
                if result.get("status") == "model_not_trained":
                    logger.warning("AI model not trained yet. Waiting...")
                    time.sleep(CHECK_INTERVAL)
                    continue

                if result.get("is_anomaly"):
                    consecutive_anomalies += 1
                    logger.warning(
                        "Anomaly #%d detected | Score: %s",
                        consecutive_anomalies,
                        result.get("anomaly_score")
                    )

                    # 3 consecutive anomalies ke baad action lo
                    if consecutive_anomalies >= 3:
                        handle_anomaly(result)
                        consecutive_anomalies = 0
                else:
                    if consecutive_anomalies > 0:
                        logger.info(
                            "System back to normal after %d anomalies",
                            consecutive_anomalies
                        )
                    consecutive_anomalies = 0
                    logger.info(
                        "System normal | Score: %.4f",
                        result.get("anomaly_score", 0)
                    )
            else:
                logger.warning("AI service returned status: %d", response.status_code)

        except requests.exceptions.ConnectionError:
            logger.warning("AI Service not reachable. Will retry in %ds...", CHECK_INTERVAL)
        except Exception as e:
            logger.error("Unexpected error in main loop: %s", e)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main_loop()