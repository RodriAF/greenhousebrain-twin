

# GreenhouseBrain

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![MQTT](https://img.shields.io/badge/MQTT-Mosquitto_2.0-660066?style=flat-square&logo=eclipsemosquitto&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL_15-FDB515?style=flat-square&logo=timescale&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?style=flat-square&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=flat-square&logo=pydantic&logoColor=white)

Automated multi-zone greenhouse management system built around an event-driven microservices architecture. Ingests real-time sensor telemetry via MQTT, applies agronomic decision logic per crop zone, persists time-series data to TimescaleDB, and dispatches critical alerts through configurable notification channels. Designed as a production-ready template for precision agriculture IoT deployments.

---

## Problem Context

A horticultural cooperative in southeastern Spain operates three greenhouse cultivation zones for tomato, pepper, and cucumber. Until now, **irrigation has been managed manually** — operators visually inspect the substrate and decide when to water based on experience and feel.

This approach creates three recurring problems:

1. **Heat periods**: On days above 35°C, evapotranspiration can reach 7–9 mm/day. Without soil moisture sensors, operators don't know whether the last irrigation was sufficient. Crops suffer heat stress silently until visible symptoms appear (leaf wilt, flower drop), at which point damage is already done.

2. **Holiday coverage**: Substitute personnel without crop knowledge tend to either over-water (root asphyxia, fungal diseases) or under-water (water stress, reduced yield). A single week of mismanagement can cost 15–30% of seasonal yield.

3. **No historical record**: Without data, it's impossible to correlate rainfall events, temperature anomalies, or irrigation decisions with yield outcomes. There is no way to optimize the system year over year.

**GreenhouseBrain solves all three** by deploying soil moisture sensors, a rules-based irrigation controller, and a full observability stack with dashboards and alerts.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          GREENHOUSE NETWORK                         │
│                                                                     │
│  ┌─────────────┐  greenhouse/{zone}/sensors  ┌──────────────────┐   │
│  │  Simulator  │ ──────────────────────────► │                  │   │
│  │             │ ◄────────────────────────── │    Mosquitto     │   │
│  └─────────────┘  greenhouse/{zone}/commands │   MQTT broker    │   │
│                                              │    MQTTv5        │   │
│  ┌─────────────┐  greenhouse/{zone}/commands │                  │   │
│  │             │ ──────────────────────────► │                  │   │
│  │             │ ◄────────────────────────── │                  │   │
│  │ Controller  │  greenhouse/{zone}/sensors  │                  │   │
│  │             │  greenhouse/alerts ───────► │                  │   │
│  └──────┬──────┘                             └────────┬─────────┘   │
│         │                                             │             │
│         │ SQL INSERT                     all topics   │             │
│         │ (irrigation_events,            (sensors +   │             │
│         │  alerts)                      commands +    │             │
│         │                                alerts)      │             │
│         │                                             ▼             │
│         │                                   ┌──────────────────┐    │
│         │                                   │    Processor     │    │
│         │                                   │  Pydantic v2     │    │
│         │                                   │  validation +    │    │
│         │                                   │  ETo (FAO-56)    │    │
│         │                                   └────────┬─────────┘    │
│         │                                            │ SQL INSERT   │
│         │                                            ▼              │
│         │                              ┌───────────────────────┐    │
│         └─────────────────────────────►│      TimescaleDB      │    │
│                                        │  sensor_readings      │    │
│                                        │  irrigation_events    │    │
│                                        │  alerts               │    │
│                                        │  eto_daily            │    │
│                                        │  continuous aggs      │    │
│                                        └──────────┬────────────┘    │
│                                                   │ SQL SELECT      │
│  ┌─────────────┐  greenhouse/alerts               ▼                 │
│  │   Alerter   │ ◄──────────────────── Mosquitto  ┌────────────┐    │
│  │  webhook +  │                                  │  Grafana   │    │
│  │  email      │                                  │ dashboards │    │
│  └─────────────┘                                  └────────────┘    │
└─────────────────────────────────────────────────────────────────────┘

  ──►  publish (QoS 1/2)
  ◄──  subscribe
```

All services communicate exclusively through MQTT topics. No service has direct knowledge of another — the broker is the sole integration point.

---

## Services

### Simulator (`simulator/`)

Generates physically realistic multi-zone telemetry without requiring hardware. Each zone models a distinct crop with independent agronomic parameters.

- Diurnal solar cycle using a sinusoidal model (configurable sunrise/sunset)
- Evapotranspiration drain proportional to radiation and air temperature, weighted by FAO-56 crop coefficient (Kc)
- VWC recovery on irrigation command receipt — closes the control loop without hardware
- Probabilistic extreme weather events: heat waves, cold snaps, drought spikes, storm fronts, sensor failures
- Time acceleration via `TIME_SCALE` (up to 288x real time for compressed demos)

| Zone | Crop | Scientific Name |
|------|------|-----------------|
| zone_a | Tomato | *Solanum lycopersicum* |
| zone_b | Pepper | *Capsicum annuum* |
| zone_c | Cucumber | *Cucumis sativus* |

### Processor (`processor/`)

MQTT consumer that validates, transforms, and persists all incoming messages.

- Pydantic v2 schema validation at the ingestion boundary — malformed payloads are discarded with a warning, never written to the database
- Inserts sensor readings, irrigation events, and alerts into their respective hypertables
- Accumulates intra-day readings per zone and computes reference evapotranspiration (ETo) using the FAO-56 Penman-Monteith equation once a full day of data is available
- Thread-safe daily accumulator with automatic rollover

### Controller (`controller/`)

Stateful decision engine that evaluates zone conditions on a configurable interval and issues actuator commands.

Decision logic per zone (evaluated every `EVAL_INTERVAL_SEC`, default 5 minutes):

1. VWC below minimum threshold within irrigation window → `IRRIGATION_ON`
2. VWC below minimum threshold outside irrigation window → `IRRIGATION_ON` (emergency, `is_emergency=True`) + `VWC_CRITICAL_LOW` alert
3. VWC at or above maximum threshold while irrigating → `IRRIGATION_OFF`
4. Air temperature exceeds heat threshold → `VENTILATION_ON` + `HEAT_STRESS` alert
5. Air temperature 2°C below heat threshold while ventilating → `VENTILATION_OFF` (hysteresis)
6. Air temperature below frost threshold → `FROST_ALERT` (CRITICAL) + `ALERT_CRITICAL` command

All decisions are persisted to `irrigation_events` with a human-readable justification string.

### Alerter (`alerter/`)

Consumes `greenhouse/alerts` and dispatches notifications to configured channels.

- In-process deduplication: same zone + alert type is suppressed for a configurable cooldown (default 30 minutes) to prevent alert fatigue during sustained conditions
- Channels: stdout/log (always active), Slack-compatible webhook, SMTP email with HTML template
- Channels activate automatically when the corresponding environment variables are set — no code changes required

### Schemas (`processor/schemas.py`)

Pydantic v2 models enforced at the processor ingestion boundary:

- `SensorPayload` — field-level range validation for all physical sensor values
- `CommandPayload` — command allowlist, minimum reason length, zone validation
- `AlertPayload` — alert type allowlist, severity enum, finite value assertion

---

## Data Model

All time-series tables are TimescaleDB hypertables. Compression and retention policies are applied automatically via `init.sql` on first startup.

| Table | Partition | Compression | Retention |
|-------|-----------|-------------|-----------|
| `sensor_readings` | 7-day chunks | After 30 days | 2 years |
| `irrigation_events` | 30-day chunks | — | — |
| `alerts` | 30-day chunks | — | — |
| `eto_daily` | 90-day chunks | — | — |

Continuous aggregates pre-compute hourly and daily rollups, making Grafana queries fast regardless of raw data volume.

| View | Granularity | Refresh |
|------|-------------|---------|
| `sensor_hourly` | 1 hour | Every 30 min |
| `sensor_daily` | 1 day | Every 1 hour |

---

## Quick Start

**Prerequisites:** Docker ≥ 24.0, Docker Compose ≥ 2.20, 4 GB RAM

### 1. Clone and configure

```bash
git clone https://github.com/RodriAF/greenhousebrain-twin.git
cd greenhousebrain-twin

# Create environment file and edit credentials and thresholds
cp example.env .env

# Edit passwords and settings
```

#### Minimum setup

No hardware required. The stack includes a sensor simulator that generates
realistic telemetry for all three zones out of the box. By default it runs
at `TIME_SCALE=60` — one simulated day every 24 real minutes — so you will
see irrigation decisions and alerts within the first few minutes.

```bash
# In .env, these three control the simulation:
TIME_SCALE=60            # 1 simulated day every 24 real minutes
SIM_START_HOUR=7         # Start the clock at 07:00 (sunrise)
PUBLISH_INTERVAL_SEC=30  # Publish sensor readings every 30s
```

### 2. Launch the stack

```bash
docker compose up -d --build

# Watch logs
docker compose logs -f

# Check service health
docker compose ps
```

No manual setup needed for the MQTT credentials. The broker container reads `MQTT_USER` and `MQTT_PASSWORD`
from `.env` at startup and creates the password file automatically via the
`entrypoint` defined in `docker-compose.yml`. Just make sure both variables
are set in your `.env` before running `docker compose up`.

If you ever need to regenerate the credentials file manually (e.g. after a password change):

```bash
# Create the Mosquitto password file
docker run --rm -it eclipse-mosquitto:2.0 \
  mosquitto_passwd -c -b /dev/stdout greenhouse your_mqtt_password_here \
  > mosquitto/config/passwd

# Or if you have mosquitto installed locally:
mosquitto_passwd -c mosquitto/config/passwd greenhouse
```

### 3. Access dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / greenhouse2024 |
| MQTT (raw) | localhost:1883 | greenhouse / (from .env) |
| TimescaleDB | localhost:5432 | ghbrain / (from .env) |

### 4. Verify data flow

```bash
# Watch MQTT messages in real time
docker exec -it greenhouse_mosquitto \
  mosquitto_sub -h localhost -u greenhouse -P your_pass \
  -t "greenhouse/#" -v

# Check sensor readings in DB
docker exec -it greenhouse_timescaledb \
  psql -U ghbrain -d greenhouse -c \
  "SELECT zone_id, vwc, temp_air, time FROM sensor_readings ORDER BY time DESC LIMIT 10;"

# Check irrigation decisions
docker exec -it greenhouse_timescaledb \
  psql -U ghbrain -d greenhouse -c \
  "SELECT zone_id, command, vwc_at_trigger, reason, time FROM irrigation_events ORDER BY time DESC LIMIT 10;"
```

### 5. Simulation options

For simulation options, time acceleration, and extreme weather events,
see [docs/SIMULATION.md](docs/SIMULATION.md).

---

## Configuration Reference

All parameters are injected via environment variables. See `example.env` for the full list with comments.

| Variable | Default | Description |
|----------|---------|-------------|
| `TIME_SCALE` | `1.0` | Simulation speed multiplier |
| `PUBLISH_INTERVAL_SEC` | `120` | Sensor publish cadence (seconds) |
| `EVAL_INTERVAL_SEC` | `300` | Controller evaluation cadence (seconds) |
| `EXTREME_EVENT_PROBABILITY` | `0.02` | Per-tick probability of a weather event |
| `ALERT_COOLDOWN_MINUTES` | `30` | Alert deduplication window |
| `ALTITUDE_M` | `50.0` | Site altitude for ETo psychrometric constant |
| `SUNRISE_HOUR` / `SUNSET_HOUR` | `7` / `20` | Solar cycle window |
| `ALERT_WEBHOOK_URL` | _(empty)_ | Slack or generic webhook endpoint |
| `SMTP_HOST` | _(empty)_ | SMTP relay for email alerts |
| `ZONE_A_VWC_MIN` / `_MAX` | `55` / `80` | Per-zone irrigation thresholds |
| `ZONE_X_TEMP_HEAT` | zone-specific | Ventilation trigger (°C) |
| `ZONE_X_TEMP_FROST` | zone-specific | Frost alert threshold (°C) |
| `ZONE_X_IRRIGATION_START/END` | zone-specific | Daytime irrigation window (hour) |

Zone thresholds for all three zones are fully configurable without code changes. Notification channels (webhook, email) activate automatically when their variables are set.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Message broker | Eclipse Mosquitto 2.0 (MQTT v5) |
| Time-series database | TimescaleDB on PostgreSQL 15 |
| Observability | Grafana 10.4 with continuous aggregates |
| Services runtime | Python 3.11 |
| MQTT client | paho-mqtt |
| Schema validation | Pydantic v2 |
| Containerisation | Docker Compose |
| Agronomic model | FAO-56 Penman-Monteith (ETo) |

---

## Roadmap / Extensions


- [ ] **Fertigation**: Add EC (electrical conductivity) sensor and nutrient dosing control
- [ ] **Weather forecast**: Integrate AEMET API to pre-irrigate before high-ET days
- [ ] **ML anomaly detection**: Detect sensor drift or pipe leaks from VWC pattern changes
- [ ] **Mobile PWA**: Responsive Grafana + push notifications via Grafana Alerting

---

## Further Reading

- [Simulation guide](docs/SIMULATION.md) — time acceleration, solar cycle,
  extreme weather events, and sensor payload reference
- [Agronomic and physics models](docs/MODELS.md) — per-zone crop parameters,
  ET drain model, and FAO-56 Penman-Monteith implementation

---
