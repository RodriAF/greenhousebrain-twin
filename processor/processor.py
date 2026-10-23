#!/usr/bin/env python3
"""
GreenhouseBrain — Data Processor
=================================
Consumes MQTT sensor and command messages and persists to TimescaleDB.
Calculates reference evapotranspiration (ETo) using simplified Penman-Monteith.

MQTT Topics consumed:
  greenhouse/{zone_id}/sensors  → sensor_readings hypertable
  greenhouse/{zone_id}/commands → irrigation_events hypertable
  greenhouse/alerts             → alerts hypertable

ETo Calculation (daily, per zone):
  Uses FAO-56 simplified Penman-Monteith with available sensors:
    ETo = (0.408 × Δ × (Rn - G) + γ × (900/(T+273)) × u2 × (es - ea))
          / (Δ + γ × (1 + 0.34 × u2))

  Where:
    Rn  = net radiation (from solar_radiation sensor)
    T   = mean air temperature
    es  = saturation vapour pressure
    ea  = actual vapour pressure (from RH sensor)
    u2  = wind speed (assumed 1.0 m/s inside greenhouse)
    Δ   = slope of saturation vapour pressure curve
    γ   = psychrometric constant (~0.067 kPa/°C at 1000m)
"""

import json
import logging
import math
import os
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, date
from typing import Dict, List, Optional

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
from pydantic import ValidationError

from schemas import AlertPayload, CommandPayload, SensorPayload

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("processor")


# Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "greenhouse")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "greenhouse_mqtt")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "greenhouse"),
    "user": os.getenv("DB_USER", "ghbrain"),
    "password": os.getenv("DB_PASSWORD", "ghbrain_secret"),
}

# Altitude above sea level (affects psychrometric constant)
ALTITUDE_M = float(os.getenv("ALTITUDE_M", 50.0))


# ETo Model — Simplified Penman-Monteith (FAO-56)
def saturation_vapour_pressure(temp_c: float) -> float:
    """es [kPa] — Saturation vapour pressure at temperature T (°C)."""
    return 0.6108 * math.exp((17.27 * temp_c) / (temp_c + 237.3))


def slope_vapour_pressure(temp_c: float) -> float:
    """Δ [kPa/°C] — Slope of saturation vapour pressure curve."""
    es = saturation_vapour_pressure(temp_c)
    return (4098 * es) / ((temp_c + 237.3) ** 2)


def psychrometric_constant(altitude_m: float) -> float:
    """γ [kPa/°C] — Psychrometric constant based on altitude."""
    # Atmospheric pressure at altitude
    P = 101.3 * ((293 - 0.0065 * altitude_m) / 293) ** 5.26
    return 0.000665 * P


def solar_to_net_radiation(solar_w_m2: float, temp_c: float, rh: float) -> float:
    """
    Estimate net radiation Rn [MJ/m²/day] from solar irradiance.
    Applies albedo (α=0.23) and longwave correction.
    """
    # Convert W/m² to MJ/m²/day (average reading over a minute period)
    Rs_mj = solar_w_m2 * 86400 / 1_000_000  # W/m² to MJ/m²/day

    # Net shortwave radiation (albedo = 0.23 for reference crop)
    Rns = (1 - 0.23) * Rs_mj

    # Net longwave radiation (simplified Stefan-Boltzmann)
    T_k = temp_c + 273.16
    sigma = 4.903e-9   # Stefan-Boltzmann [MJ/K⁴/m²/day]
    ea = (rh / 100.0) * saturation_vapour_pressure(temp_c)
    Rnl = sigma * (T_k ** 4) * (0.34 - 0.14 * math.sqrt(ea)) * (1.35 * min(Rs_mj / max(0.01, 0.75 * Rs_mj), 1) - 0.35)

    return Rns - Rnl


def calculate_eto(temp_c: float, rh: float, solar_w_m2: float,
                  wind_u2: float = 1.0) -> float:
    """
    FAO-56 Penman-Monteith Reference ETo [mm/day].

    Parameters
    ----------
    temp_c   : mean air temperature (°C)
    rh       : relative humidity (%)
    solar_w_m2 : solar radiation (W/m²)
    wind_u2  : wind speed at 2m height (m/s), default 1.0 for greenhouse
    """
    T = temp_c
    delta = slope_vapour_pressure(T)
    gamma = psychrometric_constant(ALTITUDE_M)
    es = saturation_vapour_pressure(T)
    ea = (rh / 100.0) * es
    Rn = solar_to_net_radiation(solar_w_m2, T, rh)
    G = 0.0  # Soil heat flux — negligible for daily calc

    numerator = (
        0.408 * delta * (Rn - G)
        + gamma * (900 / (T + 273)) * wind_u2 * (es - ea)
    )
    denominator = delta + gamma * (1 + 0.34 * wind_u2)

    eto = numerator / denominator
    return max(0.0, round(eto, 3))


# Daily ETo Accumulator
class DailyAccumulator:
    """Accumulates sensor readings for daily ETo calculation."""

    def __init__(self):
        self._data: Dict[str, Dict[date, List[dict]]] = defaultdict(lambda: defaultdict(list))
        self._lock = threading.Lock()

    def add(self, zone_id: str, day: date, reading: dict):
        with self._lock:
            self._data[zone_id][day].append(reading)

    def get_day_stats(self, zone_id: str, day: date) -> Optional[dict]:
        with self._lock:
            readings = self._data[zone_id].get(day, [])
        if not readings:
            return None

        temps = [r["temp_air"] for r in readings if r.get("temp_air") is not None]
        rhs = [r["humidity_rh"] for r in readings if r.get("humidity_rh") is not None]
        rads = [r["solar_radiation"] for r in readings if r.get("solar_radiation") is not None]

        if not temps or not rhs or not rads:
            return None

        return {
            "temp_mean": sum(temps) / len(temps),
            "temp_max": max(temps),
            "temp_min": min(temps),
            "rh_mean": sum(rhs) / len(rhs),
            "radiation_mean": sum(rads) / len(rads),
            "sample_count": len(readings),
        }

    def clear_day(self, zone_id: str, day: date):
        with self._lock:
            self._data[zone_id].pop(day, None)


_accumulator = DailyAccumulator()
_last_eto_date: Dict[str, date] = {}


# Database
_db_conn: Optional[psycopg2.extensions.connection] = None


def get_db() -> psycopg2.extensions.connection:
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        for attempt in range(1, 11):
            try:
                _db_conn = psycopg2.connect(**DB_CONFIG)
                _db_conn.autocommit = False
                log.info("[SUCCESS]: Processor connected to TimescaleDB")
                return _db_conn
            except OperationalError as e:
                log.warning("[WAITTING]: DB retry %d/10: %s", attempt, e)
                time.sleep(5)
        raise RuntimeError("TimescaleDB unavailable after 10 retries")
    return _db_conn


def insert_sensor_reading(payload: dict) -> None:
    sensors = payload.get("sensors", {})
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sensor_readings (
                    time, zone_id, crop, vwc, temp_soil, temp_air,
                    humidity_rh, solar_radiation, co2_ppm, raw_payload
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    payload.get("ts"),
                    payload.get("zone_id"),
                    payload.get("crop"),
                    sensors.get("vwc"),
                    sensors.get("temp_soil"),
                    sensors.get("temp_air"),
                    sensors.get("humidity_rh"),
                    sensors.get("solar_radiation"),
                    sensors.get("co2_ppm"),
                    json.dumps(payload),
                )
            )
        conn.commit()
    except Exception as e:
        log.error("[ERROR]: insert_sensor_reading failed: %s", e)
        try:
            get_db().rollback()
        except Exception:
            pass
        global _db_conn
        _db_conn = None


def insert_command_event(payload: dict) -> None:
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO irrigation_events (
                    time, zone_id, command, reason, is_emergency
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    payload.get("ts"),
                    payload.get("zone_id"),
                    payload.get("command"),
                    payload.get("reason"),
                    payload.get("emergency", False),
                )
            )
        conn.commit()
    except Exception as e:
        log.error("[ERROR]: insert_command_event failed: %s", e)


def insert_alert(payload: dict) -> None:
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (
                    time, zone_id, alert_type, severity,
                    value, threshold, message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payload.get("ts"),
                    payload.get("zone_id"),
                    payload.get("alert_type"),
                    payload.get("severity"),
                    payload.get("value"),
                    payload.get("threshold"),
                    payload.get("message"),
                )
            )
        conn.commit()
    except Exception as e:
        log.error("[ERROR]: insert_alert failed: %s", e)


def insert_eto(zone_id: str, day: date, stats: dict, eto_mm: float) -> None:
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eto_daily (
                    time, zone_id, eto_mm, temp_max, temp_min,
                    humidity_mean, radiation_mean
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc),
                    zone_id,
                    eto_mm,
                    stats["temp_max"],
                    stats["temp_min"],
                    stats["rh_mean"],
                    stats["radiation_mean"],
                )
            )
        conn.commit()
        log.info("[INFO]: ETo stored: [%s] %s → %.2fmm/day", zone_id, day, eto_mm)
    except Exception as e:
        log.error("[ERROR]: insert_eto failed: %s", e)


# ETo Daily Computation
def maybe_compute_eto(zone_id: str, sim_date: date) -> None:
    """Check if we have a full day of data and compute ETo."""
    #today = date.today()
    yesterday = date.fromordinal(sim_date.toordinal() - 1)

    last_computed = _last_eto_date.get(zone_id)
    if last_computed == yesterday:
        return  # Already computed for yesterday

    stats = _accumulator.get_day_stats(zone_id, yesterday)
    if stats and stats["sample_count"] >= 10:  # At least 10 readings
        eto = calculate_eto(
            temp_c=stats["temp_mean"],
            rh=stats["rh_mean"],
            solar_w_m2=stats["radiation_mean"],
            wind_u2=1.0  # Greenhouse wind speed assumption
        )
        insert_eto(zone_id, yesterday, stats, eto)
        _last_eto_date[zone_id] = yesterday
        _accumulator.clear_day(zone_id, yesterday)
        log.info("[INFO]: ETo [%s] %s: %.2fmm | T_mean=%.1f°C RH=%.0f%% Rad=%.0fW/m²",
                 zone_id, yesterday, eto,
                 stats["temp_mean"], stats["rh_mean"], stats["radiation_mean"])


# MQTT Handlers
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("[SUCCESS]: Processor connected to MQTT broker")
        # Subscribe to all sensor, command, and alert topics
        subscriptions = [
            ("greenhouse/+/sensors", 1),
            ("greenhouse/+/commands", 1),
            ("greenhouse/alerts", 1),
        ]
        for topic, qos in subscriptions:
            client.subscribe(topic, qos=qos)
            log.info("[SUCCESS]: Subscribed to %s", topic)
    else:
        log.error("[ERROR]: MQTT connection failed: rc=%s", rc)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        if "/sensors" in topic:
            try:
                validated = SensorPayload.model_validate(payload)
            except ValidationError as e:
                log.warning("[WARNING]: Invalid sensor payload from %s — discarded: %s", topic, e)
                return

            insert_sensor_reading(payload)

            # Accumulate for ETo
            zone_id = validated.zone_id
            today = date.today()
            sim_ts = validated.ts.date()
            sim_date = sim_ts.date() if isinstance(sim_ts, datetime) else sim_ts 
            _accumulator.add(zone_id, sim_date, {
                "temp_air": validated.sensors.temp_air,
                "humidity_rh": validated.sensors.humidity_rh,
                "solar_radiation": validated.sensors.solar_radiation,
            })
            maybe_compute_eto(zone_id, sim_date)

            log.debug("[SAVED]: [%s] Sensor reading persisted (VWC=%.1f%%)",
                      zone_id, validated.sensors.vwc)

        elif "/commands" in topic:
            try:
                CommandPayload.model_validate(payload)
            except ValidationError as e:
                log.warning("[WARNING]: Invalid command payload — discarded: %s", e)
                return
            # Commands are persisted by the controller itself (it also stores VWC,
            # temperature and the justification), so no insert here -> no duplicates.
            log.debug("[SEEN]: Command observed: %s", payload.get("command"))

        elif topic == "greenhouse/alerts":
            try:
                AlertPayload.model_validate(payload)
            except ValidationError as e:
                log.warning("[WARNING]: Invalid alert payload — discarded: %s", e)
                return
            if payload.get("source") == "controller":
                # Already persisted by the controller; skip to avoid duplicate rows.
                log.debug("[SEEN]: Controller alert already persisted: %s", payload.get("alert_type"))
                return
            insert_alert(payload)
            log.info("[ALERT]: Alert persisted: [%s] %s",
                     payload.get("zone_id"), payload.get("alert_type"))

    except (json.JSONDecodeError, KeyError) as e:
        log.warning("[WARNING]: Failed to process message on %s: %s", msg.topic, e)
    except Exception as e:
        log.error("[ERROR]: Unexpected error processing %s: %s", msg.topic, e, exc_info=True)


def get_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        client_id=f"greenhouse_proc_{int(time.time())}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    log.info("[INFO]: Processor connecting to MQTT at %s:%s", MQTT_HOST, MQTT_PORT)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    return client


# Main
_running = True


def shutdown_handler(sig, frame):
    global _running
    log.info("[STOPPED]: Processor shutdown requested")
    _running = False


def main():
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    log.info("[INFO]: GreenhouseBrain Processor starting...")
    log.info("   Altitude: %.0fm (for ETo psychrometric constant)", ALTITUDE_M)

    time.sleep(15)  # wait for DB + broker to be ready

    try:
        get_db()
    except RuntimeError as e:
        log.error("[ERROR]: %s", e)

    client = get_mqtt_client()
    client.loop_start()

    log.info("[SUCCESS]: Processor running — waiting for MQTT messages...")

    while _running:
        time.sleep(5)

    log.info("[STOPPED]: Processor stopped.")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
