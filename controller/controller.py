#!/usr/bin/env python3
"""
GreenhouseBrain — Irrigation Controller (Decision Engine)
=========================================================
Evaluates each zone every time a new sensor reading arrives (event-driven, so it
works the same at TIME_SCALE=1 and during the accelerated catch-up) and issues commands.

Decision Logic per zone:
  1. VWC < vwc_min AND current_hour ∈ [irrigation_start, irrigation_end]
     → IRRIGATION_ON
  2. VWC < vwc_min AND outside irrigation window
     → IRRIGATION_ON + is_emergency=True (night emergency)
  3. VWC >= vwc_max → IRRIGATION_OFF
  4. temp_air > heat_threshold → VENTILATION_ON
  5. temp_air < frost_threshold → ALERT_CRITICA (published to alerter topic)

Each decision is persisted in TimescaleDB with full justification.

MQTT Topics:
  Subscribes: greenhouse/{zone_id}/sensors
  Publishes:  greenhouse/{zone_id}/commands
              greenhouse/alerts (for critical conditions)
"""

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError

# Logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("controller")


# Zone Threshold Configuration (from environment)

@dataclass
class ZoneThresholds:
    zone_id: str
    vwc_min: float          # % — irrigation trigger
    vwc_max: float          # % — irrigation stop
    temp_heat: float        # °C — ventilation trigger
    temp_frost: float       # °C — frost alert
    irr_start: int          # hour — irrigation window start
    irr_end: int            # hour — irrigation window end

    # Runtime state
    last_vwc: float = field(default=0.0)
    last_temp: float = field(default=0.0)
    last_rh: float = field(default=0.0)
    irrigating: bool = field(default=False)
    ventilation_on: bool = field(default=False)
    last_seen: Optional[datetime] = field(default=None)
    last_command: Optional[str] = field(default=None)
    last_ts: Optional[str] = field(default=None)
    vwc_valid: bool = field(default=True)                         # False while the probe reports garbage
    last_raw_vwc: float = field(default=0.0)
    irrigating_since: Optional[datetime] = field(default=None)   # data-time of IRRIGATION_ON
    ineffective_warned: bool = field(default=False)              # one warning per irrigation cycle


# Warn if a zone irrigates longer than this (data-time minutes) without recovering
IRRIGATION_MAX_MINUTES = float(os.getenv("IRRIGATION_MAX_MINUTES", 180))

# A VWC reading at or below this value is treated as a disconnected probe
VWC_MIN_VALID = float(os.getenv("VWC_MIN_VALID", 1.0))

# Minimum data-time between two alerts of the same type in the same zone
ALERT_COOLDOWN_MINUTES = float(os.getenv("ALERT_COOLDOWN_MINUTES", 30))
_last_alert: Dict[tuple, datetime] = {}


def alert_allowed(zone_id: str, kind: str, now: datetime) -> bool:
    """True (and records it) if the cooldown for this zone/alert type has elapsed."""
    last = _last_alert.get((zone_id, kind))
    if last is not None and 0 <= (now - last).total_seconds() < ALERT_COOLDOWN_MINUTES * 60:
        return False
    _last_alert[(zone_id, kind)] = now
    return True


ZONES: Dict[str, ZoneThresholds] = {
    "zone_a": ZoneThresholds(
        zone_id="zone_a",
        vwc_min=float(os.getenv("ZONE_A_VWC_MIN", 55)),
        vwc_max=float(os.getenv("ZONE_A_VWC_MAX", 80)),
        temp_heat=float(os.getenv("ZONE_A_TEMP_HEAT", 30)),
        temp_frost=float(os.getenv("ZONE_A_TEMP_FROST", 5)),
        irr_start=int(os.getenv("ZONE_A_IRRIGATION_START", 7)),
        irr_end=int(os.getenv("ZONE_A_IRRIGATION_END", 19)),
    ),
    "zone_b": ZoneThresholds(
        zone_id="zone_b",
        vwc_min=float(os.getenv("ZONE_B_VWC_MIN", 52)),
        vwc_max=float(os.getenv("ZONE_B_VWC_MAX", 72)),
        temp_heat=float(os.getenv("ZONE_B_TEMP_HEAT", 32)),
        temp_frost=float(os.getenv("ZONE_B_TEMP_FROST", 8)),
        irr_start=int(os.getenv("ZONE_B_IRRIGATION_START", 7)),
        irr_end=int(os.getenv("ZONE_B_IRRIGATION_END", 18)),
    ),
    "zone_c": ZoneThresholds(
        zone_id="zone_c",
        vwc_min=float(os.getenv("ZONE_C_VWC_MIN", 65)),
        vwc_max=float(os.getenv("ZONE_C_VWC_MAX", 85)),
        temp_heat=float(os.getenv("ZONE_C_TEMP_HEAT", 33)),
        temp_frost=float(os.getenv("ZONE_C_TEMP_FROST", 10)),
        irr_start=int(os.getenv("ZONE_C_IRRIGATION_START", 6)),
        irr_end=int(os.getenv("ZONE_C_IRRIGATION_END", 20)),
    ),
}


# Database Connection

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "greenhouse"),
    "user": os.getenv("DB_USER", "ghbrain"),
    "password": os.getenv("DB_PASSWORD", "ghbrain_secret"),
}

_db_conn: Optional[psycopg2.extensions.connection] = None


def get_db() -> psycopg2.extensions.connection:
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        retries = 0
        while retries < 10:
            try:
                _db_conn = psycopg2.connect(**DB_CONFIG)
                _db_conn.autocommit = False
                log.info("[SUCCESS]: Connected to TimescaleDB")
                break
            except OperationalError as e:
                retries += 1
                log.warning("[WAITING] DB not ready (%s/10): %s", retries, e)
                time.sleep(5)
        else:
            raise RuntimeError("Could not connect to TimescaleDB after 10 retries")
    return _db_conn


def persist_decision(zone_id: str, command: str, vwc: float, temp: float,
                     reason: str, is_emergency: bool = False, sim_ts=None) -> None:
    """Persist irrigation/ventilation decision to TimescaleDB."""
    global _db_conn
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO irrigation_events
                    (time, zone_id, command, vwc_at_trigger,
                     temp_at_trigger, reason, is_emergency)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (sim_ts or datetime.now(timezone.utc).isoformat(), zone_id, command, vwc, temp, reason, is_emergency)
            )
        conn.commit()
        log.debug("[SAVING]: Persisted decision: [%s] %s — %s", zone_id, command, reason)
    except Exception as e:
        log.error("[ERROR]: Failed to persist decision: %s", e)
        try:
            get_db().rollback()
        except Exception:
            pass
        _db_conn = None  # Force reconnect


def persist_alert(zone_id: str, alert_type: str, severity: str,
                  value: float, threshold: float, message: str, sim_ts=None) -> None:
    """Persist critical alert to TimescaleDB."""
    global _db_conn
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts
                    (time, zone_id, alert_type, severity, value, threshold, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (sim_ts or datetime.now(timezone.utc).isoformat(), zone_id, alert_type, severity, value, threshold, message)
            )
        conn.commit()
        log.warning("[ALERT] Alert persisted: [%s] %s (val=%.1f, thresh=%.1f)",
                    zone_id, alert_type, value, threshold)
    except Exception as e:
        log.error("[ERROR]: Failed to persist alert: %s", e)
        try:
            get_db().rollback()
        except Exception:
            pass
        _db_conn = None  # Force reconnect


# MQTT Client

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "greenhouse")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "greenhouse_mqtt")

_mqtt_client: Optional[mqtt.Client] = None


def publish_command(client: mqtt.Client, zone_id: str, command: str,
                    reason: str, extra: dict = None, sim_ts=None) -> None:
    """Publish a control command to the zone's command topic."""
    payload = {
        "ts": sim_ts or datetime.now(timezone.utc).isoformat(),
        "zone_id": zone_id,
        "command": command,
        "reason": reason,
        "source": "controller",
    }
    if extra:
        payload.update(extra)

    topic = f"greenhouse/{zone_id}/commands"
    result = client.publish(topic, json.dumps(payload), qos=1)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        log.info("[SENT]: [%s] → %s | %s", zone_id, command, reason)
    else:
        log.error("[ERROR]: Failed to publish command to %s (rc=%s)", topic, result.rc)


def publish_alert(client: mqtt.Client, zone_id: str, alert_type: str,
                  severity: str, value: float, threshold: float, message: str,
                  sim_ts=None) -> None:
    """Publish alert to the alerts topic (consumed by alerter service)."""
    payload = {
        "ts": sim_ts or datetime.now(timezone.utc).isoformat(),
        "zone_id": zone_id,
        "alert_type": alert_type,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "message": message,
        "source": "controller",
    }
    client.publish("greenhouse/alerts", json.dumps(payload), qos=2)
    log.warning("[ALERT] Alert published: [%s] %s — %s (val=%.1f)",
                zone_id, alert_type, message, value)


# Decision Engine
def evaluate_zone(client: mqtt.Client, zone: ZoneThresholds) -> None:
    """
    Core decision logic for a single zone.
    Called every EVAL_INTERVAL seconds.
    """
    if zone.last_seen is None:
        log.debug("[SKIP]:  [%s] No sensor data yet, skipping evaluation", zone.zone_id)
        return

    if zone.last_ts:
        sim_dt = datetime.fromisoformat(zone.last_ts)
    else:
        sim_dt = datetime.now(timezone.utc)
    hour = sim_dt.hour

    vwc = zone.last_vwc
    temp = zone.last_temp
    sim_ts = zone.last_ts

    # ── Sensor sanity check ──────────────────────────────────
    sensor_ok = zone.vwc_valid
    if not sensor_ok and alert_allowed(zone.zone_id, "SENSOR_OFFLINE", sim_dt):
        reason = (
            f"VWC probe reports {zone.last_raw_vwc:.1f}% (<= {VWC_MIN_VALID}%): "
            f"reading ignored, no irrigation decision taken. Check the sensor."
        )
        publish_alert(client, zone.zone_id, "SENSOR_OFFLINE", "WARNING",
                      zone.last_raw_vwc, VWC_MIN_VALID, reason, sim_ts=sim_ts)
        persist_alert(zone.zone_id, "SENSOR_OFFLINE", "WARNING",
                      zone.last_raw_vwc, VWC_MIN_VALID, reason, sim_ts=sim_ts)

    # ── Irrigation decision ──────────────────────────────────
    in_window = zone.irr_start <= hour < zone.irr_end

    if sensor_ok and vwc < zone.vwc_min and not zone.irrigating:
        if in_window:
            reason = (
                f"VWC {vwc:.1f}% below trigger threshold {zone.vwc_min}% "
                f"within irrigation window {zone.irr_start}h-{zone.irr_end}h"
            )
            publish_command(client, zone.zone_id, "IRRIGATION_ON", reason, sim_ts=sim_ts)
            persist_decision(zone.zone_id, "IRRIGATION_ON", vwc, temp, reason,
                             is_emergency=False, sim_ts=sim_ts)
        else:
            # Night/outside-window emergency irrigation
            reason = (
                f"EMERGENCY: VWC {vwc:.1f}% critically below {zone.vwc_min}% "
                f"outside irrigation window (hour={hour}h). Night emergency."
            )
            publish_command(client, zone.zone_id, "IRRIGATION_ON", reason,
                            extra={"emergency": True}, sim_ts=sim_ts)
            persist_decision(zone.zone_id, "IRRIGATION_ON", vwc, temp, reason,
                             is_emergency=True, sim_ts=sim_ts)
            if alert_allowed(zone.zone_id, "VWC_CRITICAL_LOW", sim_dt):
                publish_alert(client, zone.zone_id, "VWC_CRITICAL_LOW", "WARNING",
                              vwc, zone.vwc_min, reason, sim_ts=sim_ts)
                persist_alert(zone.zone_id, "VWC_CRITICAL_LOW", "WARNING",
                              vwc, zone.vwc_min, reason, sim_ts=sim_ts)
        zone.irrigating = True
        zone.irrigating_since = sim_dt
        zone.ineffective_warned = False

    elif sensor_ok and vwc >= zone.vwc_max and zone.irrigating:
        reason = (
            f"VWC {vwc:.1f}% reached target {zone.vwc_max}%. "
            f"Stopping irrigation to prevent waterlogging."
        )
        publish_command(client, zone.zone_id, "IRRIGATION_OFF", reason, sim_ts=sim_ts)
        persist_decision(zone.zone_id, "IRRIGATION_OFF", vwc, temp, reason, sim_ts=sim_ts)
        zone.irrigating = False
        zone.irrigating_since = None

    # Safety net: irrigating for too long and the soil is still dry
    if (sensor_ok and zone.irrigating and zone.irrigating_since is not None
            and not zone.ineffective_warned and vwc < zone.vwc_min
            and (sim_dt - zone.irrigating_since).total_seconds() / 60.0 > IRRIGATION_MAX_MINUTES):
        minutes = (sim_dt - zone.irrigating_since).total_seconds() / 60.0
        reason = (
            f"Irrigation active for {minutes:.0f} min but VWC is still {vwc:.1f}% "
            f"(< {zone.vwc_min}%). Check water supply / valves."
        )
        persist_alert(zone.zone_id, "IRRIGATION_INEFFECTIVE", "WARNING",
                      vwc, zone.vwc_min, reason, sim_ts=sim_ts)
        zone.ineffective_warned = True

    # ── Ventilation decision ─────────────────────────────────
    if temp > zone.temp_heat and not zone.ventilation_on:
        reason = (
            f"Air temperature {temp:.1f}°C exceeds heat threshold {zone.temp_heat}°C. "
            f"Activating ventilation to prevent heat stress."
        )
        publish_command(client, zone.zone_id, "VENTILATION_ON", reason, sim_ts=sim_ts)
        persist_decision(zone.zone_id, "VENTILATION_ON", vwc, temp, reason, sim_ts=sim_ts)
        if alert_allowed(zone.zone_id, "HEAT_STRESS", sim_dt):
            persist_alert(zone.zone_id, "HEAT_STRESS", "WARNING",
                          temp, zone.temp_heat, reason, sim_ts=sim_ts)
        zone.ventilation_on = True

    elif temp <= (zone.temp_heat - 2.0) and zone.ventilation_on:
        # Hysteresis: turn off ventilation 2°C below threshold
        reason = (
            f"Temperature {temp:.1f}°C normalized below {zone.temp_heat - 2.0}°C. "
            f"Deactivating ventilation."
        )
        publish_command(client, zone.zone_id, "VENTILATION_OFF", reason, sim_ts=sim_ts)
        persist_decision(zone.zone_id, "VENTILATION_OFF", vwc, temp, reason, sim_ts=sim_ts)
        zone.ventilation_on = False

    # ── Frost alert ──────────────────────────────────────────
    if temp < zone.temp_frost:
        if alert_allowed(zone.zone_id, "FROST_ALERT", sim_dt):
            reason = (
                f"CRITICAL: Air temperature {temp:.1f}°C below frost threshold "
                f"{zone.temp_frost}°C. Crop damage risk!"
            )
            publish_alert(client, zone.zone_id, "FROST_ALERT", "CRITICAL",
                          temp, zone.temp_frost, reason, sim_ts=sim_ts)
            persist_alert(zone.zone_id, "FROST_ALERT", "CRITICAL",
                          temp, zone.temp_frost, reason, sim_ts=sim_ts)
            publish_command(client, zone.zone_id, "ALERT_CRITICAL", reason,
                            extra={"alert_type": "FROST"}, sim_ts=sim_ts)

    log.debug(
        "[INFO]: [%s] Eval complete | VWC=%.1f%% T=%.1f°C | Irr=%s Vent=%s",
        zone.zone_id, vwc, temp,
        "ON" if zone.irrigating else "OFF",
        "ON" if zone.ventilation_on else "OFF",
    )


def on_sensor_message(client, userdata, msg):
    """Handle incoming sensor data and update zone state."""
    try:
        payload = json.loads(msg.payload.decode())
        zone_id = payload.get("zone_id")

        if zone_id not in ZONES:
            return

        zone = ZONES[zone_id]
        if payload.get("ts") == zone.last_ts:
            return   # duplicate / retained replay of the same reading
        sensors = payload.get("sensors", {})
        raw_vwc = sensors.get("vwc")
        if raw_vwc is not None:
            zone.last_raw_vwc = raw_vwc
            zone.vwc_valid = raw_vwc > VWC_MIN_VALID
            if zone.vwc_valid:
                zone.last_vwc = raw_vwc      # keep the last VALID reading otherwise
        zone.last_temp = sensors.get("temp_air", zone.last_temp)
        zone.last_rh = sensors.get("humidity_rh", zone.last_rh)
        zone.last_seen = datetime.now()
        zone.last_ts = payload.get("ts")

        # Sync with the REAL actuator state reported by the simulator/PLC, so a
        # stale in-memory flag can never block new IRRIGATION_ON commands.
        actuators = payload.get("actuators", {})
        if "irrigating" in actuators:
            real = bool(actuators["irrigating"])
            if real != zone.irrigating:
                log.warning("[SYNC] [%s] irrigating flag out of sync (controller=%s, actuator=%s) -> using actuator",
                            zone_id, zone.irrigating, real)
                zone.irrigating = real
                zone.irrigating_since = (datetime.fromisoformat(zone.last_ts)
                                         if real and zone.last_ts else None)
                zone.ineffective_warned = False
        if "ventilation" in actuators:
            zone.ventilation_on = bool(actuators["ventilation"])

        log.debug("[SENT]: [%s] VWC=%.1f%% T=%.1f°C RH=%.0f%%",
                  zone_id, zone.last_vwc, zone.last_temp, zone.last_rh)

        evaluate_zone(client, zone)

    except (json.JSONDecodeError, KeyError) as e:
        log.warning("[WARNING]: Failed to parse sensor payload: %s", e)
    except Exception as e:
        log.error("[ERROR]: Controller evaluation error: %s", e, exc_info=True)


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("[SUCCESS]: Controller connected to MQTT broker")
        for zone_id in ZONES:
            topic = f"greenhouse/{zone_id}/sensors"
            client.subscribe(topic, qos=1)
            log.info("[SUCCESS]: Subscribed to %s", topic)
    else:
        log.error("[ERROR]: MQTT connection failed: rc=%s", rc)


def get_mqtt_client() -> mqtt.Client:
    global _mqtt_client
    if _mqtt_client is not None:
        return _mqtt_client

    client = mqtt.Client(
        client_id=f"greenhouse_ctrl_{int(time.time())}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_sensor_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    log.info("[INFO]: Controller connecting to MQTT at %s:%s", MQTT_HOST, MQTT_PORT)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    _mqtt_client = client
    return client


# Main Loop

_running = True


def shutdown_handler(sig, frame):
    global _running
    log.info("[STOP]: Controller shutdown requested")
    _running = False


def main():
    global _running

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    log.info("[INFO]: GreenhouseBrain Controller starting...")
    log.info("[INFO]:   Evaluation: on every sensor reading | irrigation watchdog: %.0f min",
             IRRIGATION_MAX_MINUTES)
    for zid, z in ZONES.items():
        log.info("[INFO]:   [%s] VWC: %.0f%%–%.0f%% | Heat: %.0f°C | Frost: %.0f°C | Window: %02d:00–%02d:00",
                 zid, z.vwc_min, z.vwc_max, z.temp_heat, z.temp_frost, z.irr_start, z.irr_end)

    time.sleep(10)   # wait for broker + simulator to be ready
    client = get_mqtt_client()
    time.sleep(3)

    # Initial DB connection check
    try:
        get_db()
    except RuntimeError as e:
        log.error("[ERROR]: DB connection failed: %s", e)

    # Evaluation is event-driven (see on_sensor_message); the main thread only idles.
    while _running:
        time.sleep(1)

    log.info("[STOP]: Controller stopped.")
    if _mqtt_client:
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()


if __name__ == "__main__":
    main()
