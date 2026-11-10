"""Simulator physics + the contract with the processor's schema."""
import random
from datetime import datetime, timezone

import pytest

import greenhouse_sim as s
from schemas import SensorPayload

def test_no_radiation_at_night_and_peak_at_midday():
    assert s.solar_radiation(2) == 0.0
    noon = (s.SUNRISE_HOUR + s.SUNSET_HOUR) / 2
    assert s.solar_radiation(noon) == pytest.approx(s.MAX_RADIATION)
    assert s.solar_radiation(noon, cloud_factor=0.3) < s.solar_radiation(noon)


def test_vwc_drains_by_day_and_recovers_with_irrigation():
    random.seed(1)
    z = s.ZONES[0]
    z.irrigating, z.vwc = False, 60.0
    s.update_vwc(z, radiation=800, air_temp=30, dt_minutes=10)
    assert z.vwc < 60.0
    z.irrigating, z.vwc = True, 60.0
    s.update_vwc(z, radiation=0, air_temp=15, dt_minutes=10)
    assert z.vwc > 60.0


def test_vwc_stays_within_physical_bounds():
    z = s.ZONES[0]
    z.irrigating, z.vwc = True, 97.9
    s.update_vwc(z, 0, 15, dt_minutes=600)
    assert z.vwc <= 98.0
    z.irrigating, z.vwc = False, 15.1
    s.update_vwc(z, 850, 40, dt_minutes=600)
    assert z.vwc >= 15.0


@pytest.mark.parametrize("zone", s.ZONES, ids=lambda z: z.zone_id)
def test_published_payload_passes_processor_schema(zone):
    """If this fails, the simulator and the processor disagree on the contract."""
    payload = s.build_payload(zone, radiation=500.0, air_temp=24.0, soil_temp=21.0,
                              rh=65.0, co2=420.0, ts=datetime.now(timezone.utc))
    SensorPayload.model_validate(payload)
