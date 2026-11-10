"""ETo maths (processor) — checked against known FAO-56 values and physical sanity."""
from datetime import date

import pytest
import processor as p


def test_saturation_vapour_pressure_fao56_reference():
    # FAO-56 Table 2.3: es(20 °C) = 2.338 kPa
    assert p.saturation_vapour_pressure(20) == pytest.approx(2.338, abs=0.005)


def test_psychrometric_constant_decreases_with_altitude():
    assert p.psychrometric_constant(0) > p.psychrometric_constant(1000)
    assert p.psychrometric_constant(0) == pytest.approx(0.0674, abs=0.001)


def test_eto_is_never_negative():
    assert p.calculate_eto(temp_c=2, rh=95, solar_w_m2=0) >= 0.0


def test_eto_grows_with_radiation_and_dryness():
    base = p.calculate_eto(25, 60, 200)
    assert p.calculate_eto(25, 60, 600) > base       # more sun
    assert p.calculate_eto(25, 30, 200) > base       # drier air


def test_eto_plausible_range_for_hot_sunny_day():
    # Not a calibration test: just guards against unit blunders (e.g. 100x off).
    assert 0.5 < p.calculate_eto(28, 50, 400) < 15


def test_accumulator_stats_and_clear():
    acc = p.DailyAccumulator()
    day = date(2026, 6, 15)
    assert acc.get_day_stats("zone_a", day) is None
    for t in (10.0, 20.0, 30.0):
        acc.add("zone_a", day, {"temp_air": t, "humidity_rh": 50, "solar_radiation": 100})
    s = acc.get_day_stats("zone_a", day)
    assert (s["temp_mean"], s["temp_max"], s["temp_min"], s["sample_count"]) == (20.0, 30.0, 10.0, 3)
    acc.clear_day("zone_a", day)
    assert acc.get_day_stats("zone_a", day) is None
