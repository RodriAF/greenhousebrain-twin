#!/usr/bin/env python3
"""
GreenhouseBrain — Alerter Service
===================================
Consumes alert messages from MQTT and dispatches notifications via:
  - Webhook (Slack, Teams, generic HTTP)
  - Email (SMTP)
  - Stdout/log (always active)

Alert deduplication: same zone+type is suppressed for COOLDOWN_MINUTES
to avoid alert fatigue during sustained critical conditions.

MQTT Topics consumed:
  greenhouse/alerts   — alert payloads from controller
"""

import json
import logging
import os
import signal
import smtplib
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Optional, Tuple

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import OperationalError

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("alerter")

# Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "greenhouse")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "greenhouse_mqtt")

WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")

# Alert deduplication cooldown (minutes per zone+type combination)
COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", 30))

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "greenhouse"),
    "user": os.getenv("DB_USER", "ghbrain"),
    "password": os.getenv("DB_PASSWORD", "ghbrain_secret"),
}

# Zone/crop metadata for readable notifications
ZONE_META = {
    "zone_a": {"name": "Zone A — Tomato", "crop": "Solanum lycopersicum"},
    "zone_b": {"name": "Zone B — Pepper", "crop": "Capsicum annuum"},
    "zone_c": {"name": "Zone C — Cucumber", "crop": "Cucumis sativus"},
}

SEVERITY_LABEL = {
    "INFO": "[INFO]:",
    "WARNING": "[WARNING]:",
    "CRITICAL": "[CRITICAL]:",
}

ALERT_TYPE_DESCRIPTIONS = {
    "FROST_ALERT": "Frost risk — irreversible crop damage",
    "HEAT_STRESS": "Heat stress — reduced fruit set and yield",
    "VWC_CRITICAL_LOW": "Critical soil moisture — emergency night irrigation required",
    "VWC_CRITICAL_HIGH": "Waterlogging detected — risk of root asphyxia",
    "SENSOR_OFFLINE": "Sensor offline — check equipment connection",
}


# Deduplication
_alert_cooldowns: Dict[Tuple[str, str], datetime] = {}


def is_cooldown_active(zone_id: str, alert_type: str) -> bool:
    key = (zone_id, alert_type)
    last_sent = _alert_cooldowns.get(key)
    if last_sent is None:
        return False
    return datetime.now(timezone.utc) - last_sent < timedelta(minutes=COOLDOWN_MINUTES)


def mark_alert_sent(zone_id: str, alert_type: str) -> None:
    _alert_cooldowns[(zone_id, alert_type)] = datetime.now(timezone.utc)


# Notification Dispatchers
def format_alert_message(payload: dict) -> str:
    zone_id = payload.get("zone_id", "unknown")
    meta = ZONE_META.get(zone_id, {"name": zone_id, "crop": ""})
    alert_type = payload.get("alert_type", "UNKNOWN")
    severity = payload.get("severity", "INFO")
    value = payload.get("value", 0)
    threshold = payload.get("threshold", 0)
    ts = payload.get("ts", "")

    description = ALERT_TYPE_DESCRIPTIONS.get(alert_type, alert_type)
    sev_label = SEVERITY_LABEL.get(severity, "[WARNING]")

    return (
        f"{sev_label} ALERT {severity} — {meta['name']}\n"
        f"Type: {alert_type}\n"
        f"Description: {description}\n"
        f"Value: {value:.1f} | Threshold: {threshold:.1f}\n"
        f"Crop: {meta['crop']}\n"
        f"Timestamp: {ts}"
    )


def send_webhook(payload: dict) -> bool:
    if not WEBHOOK_URL:
        return False
    try:
        message = format_alert_message(payload)
        # Slack-compatible format
        body = json.dumps({"text": message}).encode("utf-8")
        req = urllib.request.Request(
            WEBHOOK_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("[SENT]: Webhook sent (status %s)", resp.status)
            return True
    except urllib.error.URLError as e:
        log.warning("[WARNING]: Webhook failed: %s", e)
        return False


def send_email(payload: dict) -> bool:
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, EMAIL_TO]):
        return False

    zone_id = payload.get("zone_id", "unknown")
    meta = ZONE_META.get(zone_id, {"name": zone_id, "crop": ""})
    alert_type = payload.get("alert_type", "UNKNOWN")
    severity = payload.get("severity", "INFO")

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (
            f"[GreenhouseBrain] {severity} — {meta['name']} — {alert_type}"
        )
        msg["From"] = SMTP_USER
        msg["To"] = EMAIL_TO

        text_body = format_alert_message(payload).replace("\n", "\r\n")
        html_body = f"""
        <html><body style="font-family: Arial, sans-serif; padding: 20px;">
          <h2 style="color: #e74c3c;">GreenhouseBrain Alert</h2>
          <table border="0" cellpadding="8" style="border-collapse: collapse; width: 100%;">
            <tr><td><strong>Zone:</strong></td><td>{meta['name']}</td></tr>
            <tr><td><strong>Alert type:</strong></td><td>{alert_type}</td></tr>
            <tr><td><strong>Severity:</strong></td><td>{severity}</td></tr>
            <tr><td><strong>Value:</strong></td><td>{payload.get('value', 0):.1f}</td></tr>
            <tr><td><strong>Threshold:</strong></td><td>{payload.get('threshold', 0):.1f}</td></tr>
            <tr><td><strong>Message:</strong></td><td>{payload.get('message', '')}</td></tr>
            <tr><td><strong>Time:</strong></td><td>{payload.get('ts', '')}</td></tr>
          </table>
          <p style="color: #666; margin-top: 20px;">
            GreenhouseBrain — Automated greenhouse management system
          </p>
        </body></html>
        """

        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [EMAIL_TO], msg.as_string())

        log.info("[SENT]: Alert email sent to %s", EMAIL_TO)
        return True

    except Exception as e:
        log.warning("[WARNING]: Email send failed: %s", e)
        return False


def dispatch_alert(payload: dict) -> None:
    """
    Central alert dispatcher.
    Applies deduplication, logs, and dispatches to configured channels.
    """
    zone_id = payload.get("zone_id", "unknown")
    alert_type = payload.get("alert_type", "UNKNOWN")
    severity = payload.get("severity", "INFO")

    # Always log
    sev_label = SEVERITY_LABEL.get(severity, "[WARNING]")
    meta = ZONE_META.get(zone_id, {"name": zone_id, "crop": ""})
    log.warning(
        "%s [%s] %s — val=%.2f thresh=%.2f",
        sev_label, zone_id, alert_type,
        payload.get("value", 0), payload.get("threshold", 0)
    )

    # Deduplication check
    if is_cooldown_active(zone_id, alert_type):
        log.debug("[INFO]: Alert suppressed (cooldown active): %s/%s", zone_id, alert_type)
        return

    mark_alert_sent(zone_id, alert_type)

    # Dispatch to channels
    if WEBHOOK_URL:
        send_webhook(payload)

    if SMTP_HOST:
        send_email(payload)


# MQTT
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("[SUCCESS]: Alerter connected to MQTT broker")
        client.subscribe("greenhouse/alerts", qos=2)
        log.info("[SUCCESS]: Subscribed to greenhouse/alerts")
    else:
        log.error("[ERROR]: MQTT connection failed: rc=%s", rc)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        dispatch_alert(payload)
    except (json.JSONDecodeError, KeyError) as e:
        log.warning("[WARNING]: Failed to parse alert: %s", e)


def get_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        client_id=f"greenhouse_alerter_{int(time.time())}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    return client


# Main
_running = True


def shutdown_handler(sig, frame):
    global _running
    log.info("[STOPPED]: Alerter shutdown requested")
    _running = False


def main():
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    log.info("[INFO]: GreenhouseBrain Alerter starting...")
    log.info("   Webhook: %s", "configured" if WEBHOOK_URL else "not configured")
    log.info("   Email:   %s", "configured" if SMTP_HOST else "not configured")
    log.info("   Cooldown: %d minutes per alert type", COOLDOWN_MINUTES)

    time.sleep(10)

    client = get_mqtt_client()
    client.loop_start()

    log.info("[SUCCESS]: Alerter running — listening for alerts...")

    while _running:
        time.sleep(5)

    log.info("[STOPPED]: Alerter stopped.")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()