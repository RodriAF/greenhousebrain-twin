"""Processor MQTT handler: invalid data is dropped, valid data is persisted once."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import processor as p


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    for name in ("insert_sensor_reading", "insert_alert", "insert_eto"):
        monkeypatch.setattr(p, name, MagicMock())


def msg(topic, payload):
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return SimpleNamespace(topic=topic, payload=body)


def test_valid_sensor_message_is_persisted(sensor_payload):
    p.on_message(None, None, msg("greenhouse/zone_a/sensors", sensor_payload))
    p.insert_sensor_reading.assert_called_once()


def test_invalid_sensor_message_is_discarded(sensor_payload):
    sensor_payload["sensors"]["vwc"] = 500
    p.on_message(None, None, msg("greenhouse/zone_a/sensors", sensor_payload))
    p.insert_sensor_reading.assert_not_called()


def test_malformed_json_does_not_crash():
    p.on_message(None, None, msg("greenhouse/zone_a/sensors", b"{not json"))
    p.insert_sensor_reading.assert_not_called()


def test_alert_from_controller_is_not_duplicated():
    alert = {"ts": "2026-06-15T12:00:00+00:00", "zone_id": "zone_a",
             "alert_type": "FROST_ALERT", "severity": "CRITICAL",
             "value": 1.0, "threshold": 5.0, "message": "Frost risk",
             "source": "controller"}
    p.on_message(None, None, msg("greenhouse/alerts", alert))
    p.insert_alert.assert_not_called()
    alert.pop("source")
    p.on_message(None, None, msg("greenhouse/alerts", alert))
    p.insert_alert.assert_called_once()
