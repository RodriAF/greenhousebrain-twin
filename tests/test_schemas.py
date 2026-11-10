"""Trust boundary: the processor must reject bad payloads before they hit the DB."""
import copy

import pytest
from pydantic import ValidationError

from schemas import AlertPayload, CommandPayload, SensorPayload


def test_valid_sensor_payload_is_accepted(sensor_payload):
    assert SensorPayload.model_validate(sensor_payload).zone_id == "zone_a"


@pytest.mark.parametrize("field,value", [
    ("vwc", -1), ("vwc", 101),
    ("temp_air", 99), ("humidity_rh", 120),
    ("solar_radiation", -5), ("co2_ppm", 10),
])
def test_sensor_out_of_range_is_rejected(sensor_payload, field, value):
    p = copy.deepcopy(sensor_payload)
    p["sensors"][field] = value
    with pytest.raises(ValidationError):
        SensorPayload.model_validate(p)


def test_sensor_missing_field_is_rejected(sensor_payload):
    del sensor_payload["sensors"]["co2_ppm"]
    with pytest.raises(ValidationError):
        SensorPayload.model_validate(sensor_payload)


@pytest.mark.parametrize("field,value", [("zone_id", "zone_z"), ("crop", "banana")])
def test_unknown_zone_or_crop_is_rejected(sensor_payload, field, value):
    sensor_payload[field] = value
    with pytest.raises(ValidationError):
        SensorPayload.model_validate(sensor_payload)


def test_command_validation():
    ok = {"ts": "2026-06-15T12:00:00+00:00", "zone_id": "zone_b",
          "command": "IRRIGATION_ON", "reason": "VWC too low"}
    assert CommandPayload.model_validate(ok).emergency is False
    with pytest.raises(ValidationError):
        CommandPayload.model_validate({**ok, "command": "SELF_DESTRUCT"})
    with pytest.raises(ValidationError):
        CommandPayload.model_validate({**ok, "reason": "x"})  # too short


def test_alert_validation():
    ok = {"ts": "2026-06-15T12:00:00+00:00", "zone_id": "zone_c",
          "alert_type": "FROST_ALERT", "severity": "CRITICAL",
          "value": 2.0, "threshold": 10.0, "message": "Frost risk"}
    assert AlertPayload.model_validate(ok).severity == "CRITICAL"
    with pytest.raises(ValidationError):
        AlertPayload.model_validate({**ok, "severity": "PANIC"})
    with pytest.raises(ValidationError):
        AlertPayload.model_validate({**ok, "alert_type": "UFO"})
    with pytest.raises(ValidationError):
        AlertPayload.model_validate({**ok, "value": float("nan")})

