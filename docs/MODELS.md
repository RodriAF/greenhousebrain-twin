# Agronomic Model

## Zone A — Tomato (*Solanum lycopersicum*)

| Parameter | Value | Notes |
|-----------|-------|-------|
| VWC optimal | 60–80% | Coconut fiber substrate |
| Irrigation trigger | < 55% VWC | ET-based drain |
| Irrigation window | 07:00–19:00 | Emergency-only outside window |
| Irrigation stop | ≥ 80% VWC | Prevents root asphyxia |
| Day temperature | 20–26°C | Optimal photosynthesis |
| Night temperature | 16–18°C | Adequate for fruit set |
| Frost alert | < 5°C | Irreversible chilling injury |
| Heat ventilation | > 30°C | Pollen sterility risk |
| Crop coefficient Kc | 1.15 | FAO-56 mid-season |


> **Agronomic note**: Tomato is the most physiologically demanding crop of the three. Flower abortion (blossom drop) is triggered by temperatures above 32°C or below 10°C at night during anthesis. The controller's 30°C ventilation threshold is set conservatively to protect pollination.

## Zone B — Pepper (*Capsicum annuum*)

| Parameter | Value | Notes |
|-----------|-------|-------|
| VWC optimal | 55–75% | More sensitive to waterlogging |
| Irrigation trigger | < 52% VWC | Tighter lower bound |
| Irrigation window | 07:00–18:00 | Emergency-only outside window |
| Irrigation stop | ≥ 72% VWC | Reduced vs tomato (drainage risk) |
| Day temperature | 22–28°C | Optimal |
| Night temperature | 18–20°C | Adequate for fruit set |
| Frost alert | < 8°C | Higher frost sensitivity than tomato |
| Heat ventilation | > 32°C| |
| Crop coefficient Kc | 1.05 | FAO-56 |

> **Agronomic note**: Pepper is highly sensitive to root anoxia. Over-irrigation for even 24–48h can trigger Phytophthora capsici infection, especially in heavy substrates. The reduced VWC_MAX (72% vs 80% for tomato) reflects this sensitivity. Emergency night irrigation threshold is correspondingly tighter.

## Zone C — Cucumber (*Cucumis sativus*)

| Parameter | Value | Notes |
|-----------|-------|-------|
| VWC optimal | 70–85% | Highest water demand of the three |
| Irrigation trigger | < 65% VWC | |
| Irrigation window | 06:00–20:00 | Extended to meet high water demand  |
| Irrigation stop | ≥ 85% VWC | |
| Day temperature | 22–30°C | Wide comfortable range |
| Night temperature | 18–22°C |   |
| Humidity RH | > 75% | Critical for stomata function |
| Frost alert | < 10°C | Most cold-sensitive of the three |
| Heat ventilation | > 33°C |   |
| Crop coefficient Kc | 1.20 | Highest among three crops |

> **Agronomic note**: Cucumber has the highest water demand (Kc=1.20) and requires more frequent irrigation cycles. The irrigation window is extended (06:00–20:00) to accommodate this demand while still restricting night watering. Low humidity (<70% RH) causes epidermis cracking and bitter taste in fruit.

---

# Physics model

## Evapotranspiration (VWC Drain)

Each simulation tick computes VWC drain using a simplified ET model:

```
ET_drain = Kc × (radiation_coeff × solar_radiation + temp_coeff × max(0, T_air - 20°C)) × Δt
```

Where:
- `Kc` = FAO-56 crop coefficient
- `radiation_coeff` = calibrated per crop (0.0015–0.0022 % VWC/W/m²/min)
- `temp_coeff` = calibrated per crop (0.0004–0.0006 % VWC/°C/min)

This produces realistic daily VWC cycles where soil moisture drops through the day proportionally to light intensity and temperature, and recovers only during active irrigation.

## Reference ETo — Penman-Monteith FAO-56

The processor calculates daily ETo (mm/day) using available sensors:

```
ETo = (0.408 × Δ × (Rn - G) + γ × (900/(T+273)) × u₂ × (es - ea))
       ─────────────────────────────────────────────────────────────
                         Δ + γ × (1 + 0.34 × u₂)
```

Where:
- `Rn` = net radiation (derived from solar radiation sensor + Stefan-Boltzmann)
- `T` = mean daily air temperature
- `es` = saturation vapour pressure
- `ea` = actual vapour pressure (from RH sensor)
- `u₂` = wind speed at 2m (assumed 1.0 m/s inside greenhouse)
- `Δ` = slope of vapour pressure curve
- `γ` = psychrometric constant (altitude-adjusted)

ETo is stored in `eto_daily` hypertable and visualized in the season analysis dashboard.


# Technical References

- Allen, R.G. et al. (1998). *FAO Irrigation and Drainage Paper 56: Crop Evapotranspiration*. FAO Rome.
- Doorenbos, J. & Kassam, A.H. (1979). *Yield Response to Water*. FAO Irrigation Paper 33.
- Penman, H.L. (1948). Natural evaporation from open water, bare soil and grass. *Proceedings of the Royal Society*.
- TimescaleDB documentation: https://docs.timescale.com


---
