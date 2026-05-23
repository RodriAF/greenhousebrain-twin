
# Running the Simulation

The simulator replaces physical sensors entirely — no hardware required. All behaviour is driven by environment variables.

## Minimum setup

```bash
# In .env, these three control the simulation:
TIME_SCALE=60            # 1 simulated day every 24 real minutes
SIM_START_HOUR=7         # Start the clock at 07:00 (sunrise)
PUBLISH_INTERVAL_SEC=30  # Publish sensor readings every 30s
```

## Time acceleration

`TIME_SCALE` compresses simulated time. The simulator advances its internal clock by `(PUBLISH_INTERVAL_SEC / 60) × TIME_SCALE` minutes on every tick.

| `TIME_SCALE` | 1 simulated day takes | Good for |
|---|---|---|
| `1` | 24 hours | Hardware replacement / real deployment |
| `12` | 2 hours | Integration testing |
| `60` | 24 minutes | Dashboard demos |
| `288` | 5 minutes | Stress-testing alerts and controller logic |

`SIM_START_HOUR` pins the starting hour of the simulated clock (0–23). Setting it to `7` means the simulation begins at sunrise — irrigation decisions and solar radiation changes are visible within the first few real minutes.

## Solar cycle

`SUNRISE_HOUR` and `SUNSET_HOUR` define the daylight window for the sinusoidal radiation model. Outside this window, solar radiation is zero, evapotranspiration drain stops, and CO2 switches to night respiration mode.

```bash
SUNRISE_HOUR=7   # Almería, summer
SUNSET_HOUR=21

SUNRISE_HOUR=8   # Almería, winter
```

## Extreme Weather Events

Triggered probabilistically each tick via `EXTREME_EVENT_PROBABILITY`. Only one event
can be active per zone at a time.

| Event | Temp delta | Radiation | ET drain | RH delta | Notes |
|-------|-----------|-----------|----------|----------|-------|
| `HEAT_WAVE` | +12°C | ×1.4 | ×2.0 | −20% | Triggers HEAT_STRESS + rapid VWC drain |
| `COLD_SNAP` | −15°C | ×0.0 | ×1.0 | +15% | Triggers FROST_ALERT immediately |
| `DROUGHT_SPIKE` | — | ×1.0 | ×3.0 | −15% | VWC drops to threshold in a few ticks |
| `SENSOR_FAILURE` | — | — | — | — | Forces VWC to 0% (disconnected sensor) |
| `STORM_FRONT` | −4°C | ×0.05 | ×1.0 | +25% | Non-critical; visible in radiation chart |


```bash
EXTREME_EVENT_PROBABILITY=0.02   # 2% per tick — realistic background rate
EXTREME_EVENT_PROBABILITY=0.15   # 15% per tick — useful for demos and testing
```

At `TIME_SCALE=60` and `PUBLISH_INTERVAL_SEC=30`, each tick represents 30 simulated minutes. A `HEAT_WAVE` with `ticks=4` therefore lasts 2 simulated hours.

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