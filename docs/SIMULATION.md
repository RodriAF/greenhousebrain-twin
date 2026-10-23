
# Running the Simulation

The simulator replaces physical sensors entirely — no hardware required. All behaviour is driven by environment variables.

## Minimum setup

```bash
# In .env, these control the simulation clock:
SIM_START_DAYS_AGO=4         # Start 4 days in the past -> the demo already has history
TIME_SCALE=120               # Catch-up speed (120 = 2 h of data per real minute)
PUBLISH_INTERVAL_SEC=300     # One reading every 5 minutes of data-time
```

## Simulation clock

There is a **single clock** and data is never stamped in the future:

1. The simulator starts at `now − SIM_START_DAYS_AGO` and publishes one reading every `PUBLISH_INTERVAL_SEC` of data-time.
2. While the clock is behind the wall clock (*catch-up*), it runs `TIME_SCALE` times faster than real time (one reading every `PUBLISH_INTERVAL_SEC / TIME_SCALE` real seconds).
3. When it reaches "now" it switches automatically to real time: the newest reading always carries the current time.

| Setting | Catch-up of 4 days takes |
|---|---|
| `TIME_SCALE=1` | 4 days (no backfill, real time only) |
| `TIME_SCALE=120` | ~48 minutes |
| `TIME_SCALE=720` | ~8 minutes |

Once the backfill is not needed any more, set `TIME_SCALE=1` and `SIM_START_DAYS_AGO=0`.

## Solar cycle

`SUNRISE_HOUR` and `SUNSET_HOUR` define the daylight window for the sinusoidal radiation model. Outside this window, solar radiation is zero, evapotranspiration drain stops, and CO2 switches to night respiration mode.

```bash
SUNRISE_HOUR=7   # Almería, summer
SUNSET_HOUR=21

SUNRISE_HOUR=8   # Almería, winter
```

## Extreme Weather Events

Triggered probabilistically via `EXTREME_EVENT_PROBABILITY` (probability per zone and per simulated hour). Only one event
can be active per zone at a time.

| Event | Temp delta | Radiation | ET drain | RH delta | Notes |
|-------|-----------|-----------|----------|----------|-------|
| `HEAT_WAVE` | +12°C | ×1.4 | ×2.0 | −20% | Triggers HEAT_STRESS + rapid VWC drain |
| `COLD_SNAP` | −15°C | ×0.0 | ×1.0 | +15% | Triggers FROST_ALERT immediately |
| `DROUGHT_SPIKE` | — | ×1.0 | ×3.0 | −15% | VWC drops to threshold in a few ticks |
| `SENSOR_FAILURE` | — | — | — | — | Probe reports 0% VWC; real soil moisture is unaffected. Controller ignores it and raises `SENSOR_OFFLINE` |
| `STORM_FRONT` | −4°C | ×0.05 | ×1.0 | +25% | Non-critical; visible in radiation chart |


```bash
EXTREME_EVENT_PROBABILITY=0.02   # per simulated hour — about one event per day across the 3 zones
EXTREME_EVENT_PROBABILITY=0.10   # useful for demos and testing alerts
```

Event durations are defined for 30-minute ticks and converted automatically to `PUBLISH_INTERVAL_SEC`, so a `HEAT_WAVE` always lasts 2 hours of data-time.

## What the simulator publishes

Every tick, one JSON payload per zone is published to `greenhouse/{zone_id}/sensors` with `retain=True`:

```json
{
  "ts": "2024-06-15T09:30:00+00:00",
  "zone_id": "zone_a",
  "crop": "tomato",
  "sensors": {
    "vwc": 67.4,
    "temp_air": 24.1,
    "temp_soil": 21.3,
    "humidity_rh": 63.0,
    "solar_radiation": 612.5,
    "co2_ppm": 358.2
  },
  "actuators": { "irrigating": false, "ventilation": false },
  "meta": { "extreme_event": null }
}
```

The simulator also subscribes to `greenhouse/{zone_id}/commands` and updates its internal actuator state when the controller sends `IRRIGATION_ON`, `IRRIGATION_OFF`, `VENTILATION_ON`, or `VENTILATION_OFF` — closing the control loop without any physical hardware.

---