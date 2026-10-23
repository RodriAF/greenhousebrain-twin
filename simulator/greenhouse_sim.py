#!/usr/bin/env python3
"""
GreenhouseBrain — Multi-Zone Greenhouse Sensor Simulator
=========================================================
Simulates realistic sensor telemetry for 3 horticultural zones:
  - Zone A: Tomato (Solanum lycopersicum)
  - Zone B: Pepper (Capsicum annuum)
  - Zone C: Cucumber (Cucumis sativus)

Physics models:
  - Solar cycle (sinusoidal) with configurable sunrise/sunset
  - Evapotranspiration: VWC drain proportional to radiation x temp coefficient
  - Irrigation: VWC recovery pulse when triggered by controller command
  - Temperature: diurnal cycle + crop-specific stress amplifiers
  - CO2: inversely correlated with photosynthesis activity during daylight

MQTT Topics published:
  greenhouse/{zone_id}/sensors  — sensor payload (JSON)
"""

import json
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import paho.mqtt.client as mqtt

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("greenhouse_sim")


# Zone Configuration
@dataclass
class ZoneConfig:
    """Agronomic parameters and thresholds per zone/crop."""
    zone_id: str
    zone_name: str
    crop: str
    crop_scientific: str

    # VWC (Volumetric Water Content) parameters
    vwc_optimal_min: float      # % — lower bound of optimal range
    vwc_optimal_max: float      # % — upper bound of optimal range
    vwc_trigger_low: float      # % — irrigation trigger threshold
    vwc_trigger_high: float     # % — irrigation stop threshold
    vwc_initial: float          # % — starting VWC

    # Temperature parameters (°C)
    temp_air_day_min: float     # Day minimum comfortable temp
    temp_air_day_max: float     # Day maximum comfortable temp
    temp_air_night_min: float   # Night minimum comfortable temp
    temp_air_night_max: float   # Night maximum comfortable temp
    temp_base_day: float        # Baseline day simulation temp
    temp_base_night: float      # Baseline night simulation temp
    temp_frost_threshold: float # Critical low temp (frost alert)
    temp_heat_threshold: float  # Critical high temp (heat stress)

    # Humidity parameters (%)
    rh_optimal_min: float
    rh_optimal_max: float
    rh_base: float

    # CO2 parameters (ppm)
    co2_base_day: float         # During photosynthesis (CO2 absorbed)
    co2_base_night: float       # Respiration only (CO2 emitted)

    # Evapotranspiration coefficients
    kc: float                   # Crop coefficient (FAO-56)
    et_radiation_coeff: float   # VWC drain per W/m² per min
    et_temp_coeff: float        # Additional drain per °C above 20°C

    # Irrigation recovery
    irrigation_vwc_rate: float        # VWC % gained per minute during irrigation
    irrigation_liters_per_min: float  # L/min flowrate per zone

    # Runtime state
    vwc: float = field(init=False)
    irrigating: bool = field(default=False, init=False)
    ventilation_on: bool = field(default=False, init=False)

    def __post_init__(self):
        self.vwc = self.vwc_initial + random.uniform(-2.0, 2.0)


# Zone A — Tomato
ZONE_A = ZoneConfig(
    zone_id="zone_a",
    zone_name="Zone A — Tomato",
    crop="tomato",
    crop_scientific="Solanum lycopersicum",
    vwc_optimal_min=60.0,
    vwc_optimal_max=80.0,
    vwc_trigger_low=float(os.getenv("ZONE_A_VWC_MIN", 55)),
    vwc_trigger_high=float(os.getenv("ZONE_A_VWC_MAX", 80)),
    vwc_initial=68.0,
    temp_air_day_min=20.0,
    temp_air_day_max=26.0,
    temp_air_night_min=16.0,
    temp_air_night_max=18.0,
    temp_base_day=23.0,
    temp_base_night=17.0,
    temp_frost_threshold=float(os.getenv("ZONE_A_TEMP_FROST", 5)),
    temp_heat_threshold=float(os.getenv("ZONE_A_TEMP_HEAT", 30)),
    rh_optimal_min=60.0,
    rh_optimal_max=70.0,
    rh_base=65.0,
    co2_base_day=380.0,
    co2_base_night=520.0,
    kc=1.15,                     # Tomato mid-season Kc (FAO-56)
    et_radiation_coeff=0.0018,   # VWC % drain per W/m² per simulation minute
    et_temp_coeff=0.0005,
    irrigation_vwc_rate=0.8,     # %/min recovery
    irrigation_liters_per_min=12.0,
)

# Zone B — Pepper
ZONE_B = ZoneConfig(
    zone_id="zone_b",
    zone_name="Zone B — Pepper",
    crop="pepper",
    crop_scientific="Capsicum annuum",
    vwc_optimal_min=55.0,
    vwc_optimal_max=75.0,
    vwc_trigger_low=float(os.getenv("ZONE_B_VWC_MIN", 52)),
    vwc_trigger_high=float(os.getenv("ZONE_B_VWC_MAX", 72)),
    vwc_initial=63.0,
    temp_air_day_min=22.0,
    temp_air_day_max=28.0,
    temp_air_night_min=18.0,
    temp_air_night_max=20.0,
    temp_base_day=25.0,
    temp_base_night=19.0,
    temp_frost_threshold=float(os.getenv("ZONE_B_TEMP_FROST", 8)),
    temp_heat_threshold=float(os.getenv("ZONE_B_TEMP_HEAT", 32)),
    rh_optimal_min=55.0,
    rh_optimal_max=70.0,
    rh_base=62.0,
    co2_base_day=370.0,
    co2_base_night=510.0,
    kc=1.05,
    et_radiation_coeff=0.0015,   # Pepper slightly less water-demanding
    et_temp_coeff=0.0004,
    irrigation_vwc_rate=0.6,     # Pepper sensitive to overwatering — slower
    irrigation_liters_per_min=9.0,
)

# Zone C — Cucumber
ZONE_C = ZoneConfig(
    zone_id="zone_c",
    zone_name="Zone C — Cucumber",
    crop="cucumber",
    crop_scientific="Cucumis sativus",
    vwc_optimal_min=70.0,
    vwc_optimal_max=85.0,
    vwc_trigger_low=float(os.getenv("ZONE_C_VWC_MIN", 65)),
    vwc_trigger_high=float(os.getenv("ZONE_C_VWC_MAX", 85)),
    vwc_initial=75.0,
    temp_air_day_min=22.0,
    temp_air_day_max=30.0,
    temp_air_night_min=18.0,
    temp_air_night_max=22.0,
    temp_base_day=26.0,
    temp_base_night=20.0,
    temp_frost_threshold=float(os.getenv("ZONE_C_TEMP_FROST", 10)),
    temp_heat_threshold=float(os.getenv("ZONE_C_TEMP_HEAT", 33)),
    rh_optimal_min=75.0,
    rh_optimal_max=90.0,
    rh_base=80.0,               # Cucumber needs high humidity
    co2_base_day=360.0,
    co2_base_night=500.0,
    kc=1.20,                    # Cucumber — highest water demand
    et_radiation_coeff=0.0022,
    et_temp_coeff=0.0006,
    irrigation_vwc_rate=1.0,    # Cucumber — fast irrigation
    irrigation_liters_per_min=15.0,
)

ZONES = [ZONE_A, ZONE_B, ZONE_C]


# Extreme Weather Engine
class ExtremeEvent:
    """
    Represents an active weather event.
    Contains the deltas applied to sensors during its duration.
    """
    def __init__(self, name: str, ticks: int,
                 temp_delta: float = 0.0,
                 radiation_factor: float = 1.0,
                 vwc_drain_factor: float = 1.0,
                 rh_delta: float = 0.0,
                 force_vwc: Optional[float] = None):
        self.name = name
        self.ticks_remaining = ticks
        self.temp_delta = temp_delta              # °C added to air_temp
        self.radiation_factor = radiation_factor
        self.vwc_drain_factor = vwc_drain_factor  # multiplies ET drainage
        self.rh_delta = rh_delta
        self.force_vwc = force_vwc                # overrides the REPORTED VWC only (sensor failure)

    @property
    def active(self) -> bool:
        return self.ticks_remaining > 0

    def tick(self):
        self.ticks_remaining -= 1


# Catalog of possible events and their parameters
EVENT_CATALOG = [
    # Heat wave: +12°C, radiation x1.4, ET x2 — triggers HEAT_STRESS and drains VWC quickly
    lambda: ExtremeEvent("HEAT_WAVE",      ticks=4, temp_delta=+12.0, radiation_factor=1.4, vwc_drain_factor=2.0, rh_delta=-20.0),
    # Cold snap: -15°C — triggers FROST_ALERT immediately
    lambda: ExtremeEvent("COLD_SNAP",      ticks=3, temp_delta=-15.0, radiation_factor=0.0, rh_delta=+15.0),
    # Flash drought: ET x3 without temperature change — VWC drops to threshold in few ticks
    lambda: ExtremeEvent("DROUGHT_SPIKE",  ticks=5, vwc_drain_factor=3.0, rh_delta=-15.0),
    # Sensor failure: the probe reports 0% (real soil moisture is NOT affected)
    lambda: ExtremeEvent("SENSOR_FAILURE", ticks=3, force_vwc=0.0),
    # Storm: total cloud cover, high humidity, low temperature — non-critical but visible
    lambda: ExtremeEvent("STORM_FRONT",    ticks=4, temp_delta=-4.0, radiation_factor=0.05, rh_delta=+25.0),
]

# Probability of an event starting, per zone and per SIMULATED HOUR (2 %).
# Converted to a per-tick probability in maybe_trigger_event().
EVENT_PROBABILITY = float(os.getenv("EXTREME_EVENT_PROBABILITY", 0.02))
# Base tick duration (can be reduced for demos)
EVENT_TICK_SCALE  = float(os.getenv("EXTREME_EVENT_TICK_SCALE", 1.0))

# Global state: one active event per zone (None = no active event)
_active_events: dict[str, Optional[ExtremeEvent]] = {
    z.zone_id: None for z in [ZONE_A, ZONE_B, ZONE_C]
}


def maybe_trigger_event(zone_id: str) -> None:
    """Randomly decides whether to start a new extreme event in the zone."""
    if _active_events[zone_id] is not None:
        return  # one is already active, do not stack
    p_tick = 1.0 - (1.0 - EVENT_PROBABILITY) ** (PUBLISH_INTERVAL / 3600.0)
    if random.random() < p_tick:
        factory = random.choice(EVENT_CATALOG)
        event = factory()
        # Catalog durations were written for 30-minute ticks -> convert to the
        # real step so an event lasts the same amount of data-time.
        event.ticks_remaining = max(
            1, round(event.ticks_remaining * (1800.0 / PUBLISH_INTERVAL) * EVENT_TICK_SCALE))
        _active_events[zone_id] = event
        log.warning(
            "[WARNING]: [%s] EXTREME EVENT STARTED: %s (%d ticks)",
            zone_id, event.name, event.ticks_remaining
        )


def apply_event(zone_id: str, air_temp: float, radiation: float,
                rh: float) -> tuple[float, float, float]:
    """
    Applies active event deltas to the already calculated values.
    Returns modified (air_temp, radiation, rh).
    """
    event = _active_events.get(zone_id)
    if event is None or not event.active:
        return air_temp, radiation, rh

    air_temp  = air_temp  + event.temp_delta
    radiation = max(0.0, radiation * event.radiation_factor)
    rh        = max(0.0, min(100.0, rh + event.rh_delta))
    return air_temp, radiation, rh


def advance_event(zone_id: str) -> None:
    """Consumes one tick of the active event. Removes it if it has ended."""
    event = _active_events.get(zone_id)
    if event is None:
        return
    event.tick()
    if not event.active:
        log.info("[SUCCESS]: [%s] Extreme event '%s' ended", zone_id, event.name)
        _active_events[zone_id] = None


# Solar Cycle Model
SUNRISE_HOUR  = float(os.getenv("SUNRISE_HOUR", 7))
SUNSET_HOUR   = float(os.getenv("SUNSET_HOUR", 20))
MAX_RADIATION = 850.0   # W/m² — peak clear-sky radiation in summer


def solar_radiation(hour: float, cloud_factor: float = 1.0) -> float:
    """
    Sinusoidal solar irradiance model.
    Returns W/m² based on hour of day (0-24).
    cloud_factor in [0, 1]: 1.0 = clear sky, 0.3 = overcast
    """
    day_length = SUNSET_HOUR - SUNRISE_HOUR
    if hour < SUNRISE_HOUR or hour > SUNSET_HOUR:
        return 0.0
    # Normalize to [0, pi] over the daylight window
    t_norm    = (hour - SUNRISE_HOUR) / day_length
    radiation = MAX_RADIATION * math.sin(math.pi * t_norm) * cloud_factor
    return max(0.0, radiation)


def solar_position_factor(hour: float) -> float:
    """Returns 0.0 (night) to 1.0 (solar noon) for temperature model."""
    day_length = SUNSET_HOUR - SUNRISE_HOUR
    if hour < SUNRISE_HOUR or hour > SUNSET_HOUR:
        return 0.0
    return math.sin(math.pi * (hour - SUNRISE_HOUR) / day_length)


# Sensor Physics Models
def simulate_air_temperature(zone: ZoneConfig, hour: float) -> float:
    """
    Diurnal temperature model.
    Peak ~2h after solar noon, minimum just before sunrise.
    """
    pos = solar_position_factor(hour)
    # Lag peak by ~2h after solar noon
    lag_hour = min(hour + 2.0, SUNSET_HOUR)
    lag_pos  = solar_position_factor(lag_hour)

    base_temp = zone.temp_base_night + (zone.temp_base_day - zone.temp_base_night) * lag_pos

    # Ventilation cooling effect
    ventilation_delta = -2.5 if zone.ventilation_on else 0.0

    noise = random.gauss(0, 0.3)
    return round(base_temp + ventilation_delta + noise, 2)


def simulate_soil_temperature(air_temp: float, hour: float) -> float:
    """Soil temp lags air by 2-3h and has smaller amplitude."""
    lag_factor = 0.6
    base_soil  = 18.0 + (air_temp - 18.0) * lag_factor
    noise      = random.gauss(0, 0.2)
    return round(base_soil + noise, 2)


def simulate_humidity(zone: ZoneConfig, air_temp: float, irrigating: bool) -> float:
    """
    RH inversely correlated with temperature.
    Irrigation slightly increases local humidity.
    """
    temp_delta = air_temp - zone.temp_base_day
    rh = zone.rh_base - (temp_delta * 1.2)

    if irrigating:
        rh += random.uniform(3.0, 6.0)   # Irrigation increases local humidity

    noise = random.gauss(0, 1.5)
    return round(max(30.0, min(98.0, rh + noise)), 1)


def simulate_co2(zone: ZoneConfig, radiation: float, hour: float) -> float:
    """
    CO2 drops during photosynthesis (daylight), rises at night.
    Canopy density and crop type influence absorption.
    """
    is_day = radiation > 10.0
    base   = zone.co2_base_day if is_day else zone.co2_base_night

    # Stronger photosynthesis at peak radiation — more CO2 absorbed
    if is_day:
        photosyn_reduction = (radiation / MAX_RADIATION) * 80 * zone.kc
        base -= photosyn_reduction

    noise = random.gauss(0, 8.0)
    return round(max(350.0, min(1200.0, base + noise)), 1)


# The ET coefficients in ZoneConfig were tuned for a 30 s tick treated as
# "0.5 min". Now that dt is the real data-time step, they are rescaled once here
# (1/60) so that daily ET is realistic (~10-15 % VWC per sunny day).
ET_COEFF_SCALE = 1.0 / 60.0


def update_vwc(zone: ZoneConfig, radiation: float, air_temp: float,
               dt_minutes: float, et_factor: float = 1.0) -> None:
    """
    Updates zone.vwc in-place using a simplified ET model.

    Drain rate:
      ET = Kc x (radiation_coeff x radiation + temp_coeff x max(0, T - 20)) x dt
    Recovery:
      If irrigating: VWC += irrigation_rate x dt
    """
    # Evapotranspiration drain
    et_drain = zone.kc * (
        zone.et_radiation_coeff * radiation
        + zone.et_temp_coeff * max(0.0, air_temp - 20.0)
    ) * ET_COEFF_SCALE * et_factor * dt_minutes

    # Add soil drainage/percolation noise
    percolation = random.gauss(0, 0.05)

    zone.vwc -= et_drain + percolation

    # Irrigation recovery
    if zone.irrigating:
        zone.vwc += zone.irrigation_vwc_rate * dt_minutes

    # Clamp to physical bounds
    zone.vwc = max(15.0, min(98.0, zone.vwc))


# MQTT Client
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER", "greenhouse")
MQTT_PASS = os.getenv("MQTT_PASSWORD", "greenhouse_mqtt")

# Simulation clock (single clock: the data timestamp is NEVER in the future)
#
# PUBLISH_INTERVAL_SEC : data-time between two readings (seconds). In real-time
#                        mode this is also the real wait between readings.
# SIM_START_DAYS_AGO   : the simulation starts this many days in the past, so a
#                        fresh deployment already has history to show.
# TIME_SCALE           : speed of the CATCH-UP phase only (start -> now).
#                        TIME_SCALE=120 -> 1 reading every PUBLISH_INTERVAL/120 s.
#                        Once the simulated clock reaches the wall clock, the
#                        simulator runs in real time (TIME_SCALE no longer applies)
#                        and the last reading always carries the current time.
TIME_SCALE         = max(1.0, float(os.getenv("TIME_SCALE", 1.0)))
SIM_START_DAYS_AGO = float(os.getenv("SIM_START_DAYS_AGO", 0))
PUBLISH_INTERVAL   = int(os.getenv("PUBLISH_INTERVAL_SEC", 600))
STEP = timedelta(seconds=PUBLISH_INTERVAL)

_SIM_TIME_START = (datetime.now(timezone.utc) - timedelta(days=SIM_START_DAYS_AGO)
                   ).replace(microsecond=0)

_mqtt_client: Optional[mqtt.Client] = None
_running = True


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("[SUCCESS]: Connected to MQTT broker at %s:%s", MQTT_HOST, MQTT_PORT)
        # Subscribe to command topics for all zones
        for zone in ZONES:
            topic = f"greenhouse/{zone.zone_id}/commands"
            client.subscribe(topic, qos=1)
            log.info("[INFO]: Subscribed to %s", topic)
    else:
        log.error("[ERROR]: MQTT connection failed with code %s", rc)


def on_message(client, userdata, msg):
    """Handle incoming command messages from the controller."""
    try:
        payload = json.loads(msg.payload.decode())
        zone_id = payload.get("zone_id")
        command = payload.get("command")

        zone = next((z for z in ZONES if z.zone_id == zone_id), None)
        if zone is None:
            return

        if command == "IRRIGATION_ON":
            zone.irrigating = True
            log.info("[INFO]: [%s] Irrigation STARTED (VWC: %.1f%%)", zone_id, zone.vwc)
        elif command == "IRRIGATION_OFF":
            zone.irrigating = False
            log.info("[INFO]: [%s] Irrigation STOPPED (VWC: %.1f%%)", zone_id, zone.vwc)
        elif command == "VENTILATION_ON":
            zone.ventilation_on = True
            log.info("[INFO]: [%s] Ventilation ACTIVATED", zone_id)
        elif command == "VENTILATION_OFF":
            zone.ventilation_on = False
            log.info("[INFO]: [%s] Ventilation DEACTIVATED", zone_id)

    except (json.JSONDecodeError, KeyError) as e:
        log.warning("[WARNING]: Failed to parse command: %s — %s", msg.payload, e)


def get_mqtt_client() -> mqtt.Client:
    global _mqtt_client
    if _mqtt_client is not None:
        return _mqtt_client

    client = mqtt.Client(
        client_id=f"greenhouse_sim_{int(time.time())}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    log.info("[INFO]: Connecting to MQTT broker at %s:%s ...", MQTT_HOST, MQTT_PORT)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    _mqtt_client = client
    return client


# Main Simulation Loop
def build_payload(zone: ZoneConfig, radiation: float, air_temp: float,
                  soil_temp: float, rh: float, co2: float, ts: datetime,
                  event_name: Optional[str] = None,
                  vwc_override: Optional[float] = None) -> dict:
    reported_vwc = zone.vwc if vwc_override is None else vwc_override
    return {
        "ts": ts.isoformat(),
        "zone_id": zone.zone_id,
        "zone_name": zone.zone_name,
        "crop": zone.crop,
        "crop_scientific": zone.crop_scientific,
        "sensors": {
            "vwc": round(reported_vwc, 2),
            "temp_soil": soil_temp,
            "temp_air": air_temp,
            "humidity_rh": rh,
            "solar_radiation": round(radiation, 1),
            "co2_ppm": co2,
        },
        "actuators": {
            "irrigating": zone.irrigating,
            "ventilation": zone.ventilation_on,
        },
        "meta": {
            "vwc_trigger_low": zone.vwc_trigger_low,
            "vwc_trigger_high": zone.vwc_trigger_high,
            "simulator_version": "1.1.0",
            "extreme_event": event_name,  # None under normal conditions
        },
    }


def simulate_once(client: mqtt.Client, cloud_factor: float,
                  tick_time: datetime) -> None:
    """Run one simulation tick for all zones."""
    hour       = tick_time.hour + tick_time.minute / 60.0
    radiation  = solar_radiation(hour, cloud_factor)
    dt_minutes = PUBLISH_INTERVAL / 60.0   # data-time minutes elapsed per tick

    for zone in ZONES:
        # Extreme weather
        maybe_trigger_event(zone.zone_id)
        event = _active_events.get(zone.zone_id)

        air_temp  = simulate_air_temperature(zone, hour)
        soil_temp = simulate_soil_temperature(air_temp, hour)
        rh        = simulate_humidity(zone, air_temp, zone.irrigating)
        co2       = simulate_co2(zone, radiation, hour)

        # Apply event deltas on top of normal values
        zone_rad = radiation
        air_temp, zone_rad, rh = apply_event(zone.zone_id, air_temp, zone_rad, rh)

        # The soil always follows the normal ET/irrigation model. A SENSOR_FAILURE
        # only corrupts the REPORTED reading (a broken probe does not dry the soil).
        drain_factor = event.vwc_drain_factor if event else 1.0
        update_vwc(zone, zone_rad, air_temp, dt_minutes, et_factor=drain_factor)
        vwc_override = event.force_vwc if (event and event.force_vwc is not None) else None

        advance_event(zone.zone_id)

        payload = build_payload(zone, zone_rad, air_temp, soil_temp, rh, co2,
                                event_name=event.name if event else None,
                                ts=tick_time, vwc_override=vwc_override)
        topic  = f"greenhouse/{zone.zone_id}/sensors"
        result = client.publish(topic, json.dumps(payload), qos=1, retain=True)

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            log.debug(
                "[%s] VWC=%.1f%% T=%.1f°C RH=%.0f%% Rad=%.0fW/m²%s",
                zone.zone_id, zone.vwc, air_temp, rh, zone_rad,
                f" [{event.name}]" if event else ""
            )
        else:
            log.warning("[WARNING]: Failed to publish to %s (rc=%s)", topic, result.rc)

    log.info(
        "[INFO]: Tick done — Hour=%.2fh | Rad=%.0fW/m² | Cloud=%.0f%%",
        hour, radiation, (1 - cloud_factor) * 100
    )


def generate_cloud_factor(tick_time: datetime) -> float:
    """
    Daily weather variation.
    Returns a cloud factor [0.3-1.0] that changes slowly over time.
    """
    # Slow sinusoidal variation + random daily component
    base       = 0.7 + 0.3 * math.sin(tick_time.hour * math.pi / 12)
    daily_seed = hash(tick_time.strftime("%Y-%m-%d")) % 1000 / 1000.0
    factor     = base * (0.7 + 0.3 * daily_seed) + random.gauss(0, 0.05)
    return max(0.3, min(1.0, factor))


def shutdown_handler(sig, frame):
    global _running
    log.info("[STOPPED]: Shutdown signal received, stopping simulator...")
    _running = False


def main():
    global _running

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    log.info("[INFO]: GreenhouseBrain Simulator starting...")
    log.info("   Zones: %s", [z.zone_id for z in ZONES])
    log.info("   Reading every %ds of data-time | Sunrise: %sh | Sunset: %sh",
             PUBLISH_INTERVAL, SUNRISE_HOUR, SUNSET_HOUR)
    log.info("   Start: %s (%.1f days ago) | catch-up TIME_SCALE=%.0fx, then real time",
             _SIM_TIME_START.isoformat(), SIM_START_DAYS_AGO, TIME_SCALE)

    # Wait for broker readiness
    time.sleep(5)
    client = get_mqtt_client()
    time.sleep(2)  # allow on_connect to fire

    tick_time            = _SIM_TIME_START
    cloud_factor         = generate_cloud_factor(tick_time)
    ticks_per_hour       = max(1, int(3600 / PUBLISH_INTERVAL))
    tick_count           = 0
    was_catching_up      = True

    while _running:
        try:
            if tick_count % ticks_per_hour == 0:
                cloud_factor = generate_cloud_factor(tick_time)

            simulate_once(client, cloud_factor, tick_time)
            tick_count += 1
        except Exception as e:
            log.error("[ERROR]: Simulation error: %s", e, exc_info=True)

        next_tick = tick_time + STEP
        now = datetime.now(timezone.utc)
        if next_tick <= now:
            # Catch-up phase: the next reading is already in the past -> go fast.
            pause = max(0.2, PUBLISH_INTERVAL / TIME_SCALE)
        else:
            # Real-time phase: wait until the next reading is "now".
            if was_catching_up:
                log.info("[INFO]: Simulation caught up with the wall clock — running in real time")
                was_catching_up = False
            pause = (next_tick - now).total_seconds()
        tick_time = next_tick

        # Sleep in small increments to remain responsive to SIGTERM
        end = time.monotonic() + pause
        while _running and time.monotonic() < end:
            time.sleep(min(1.0, max(0.0, end - time.monotonic())))

    log.info("[STOPPED]: Simulator stopped cleanly.")
    if _mqtt_client:
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()


if __name__ == "__main__":
    main()