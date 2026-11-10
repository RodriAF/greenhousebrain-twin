"""Shared fixtures. Services are plain scripts (no package), so pytest.ini adds
their folders to sys.path. Nothing here needs MQTT or TimescaleDB: every
network/DB call is mocked."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sensor_payload():
    """A valid payload, same shape the simulator publishes."""
    return {
        "ts": "2026-06-15T12:00:00+00:00",
        "zone_id": "zone_a",
        "crop": "tomato",
        "sensors": {
            "vwc": 62.0, "temp_soil": 22.0, "temp_air": 26.0,
            "humidity_rh": 60.0, "solar_radiation": 700.0, "co2_ppm": 450.0,
        },
    }


@pytest.fixture
def mqtt_client():
    return MagicMock(name="mqtt_client")


@pytest.fixture
def controller_env(monkeypatch):
    """Controller with persistence mocked and a clean alert-cooldown table."""
    import controller as c
    c._last_alert.clear()
    for name in ("persist_decision", "persist_alert", "persist_irrigation_summary"):
        monkeypatch.setattr(c, name, MagicMock())
    return c


@pytest.fixture
def make_zone(controller_env):
    def _make(hour=12, vwc=70.0, temp=22.0, **kw):
        z = controller_env.ZoneThresholds(
            zone_id="zone_a", vwc_min=55, vwc_max=80, temp_heat=30,
            temp_frost=5, irr_start=7, irr_end=19)
        z.last_seen = datetime.now()
        z.last_ts = datetime(2026, 6, 15, hour, 0, tzinfo=timezone.utc).isoformat()
        z.last_vwc, z.last_temp = vwc, temp
        for k, v in kw.items():
            setattr(z, k, v)
        return z
    return _make


def commands_sent(client):
    """Command names published on any */commands topic."""
    import json
    out = []
    for call in client.publish.call_args_list:
        topic, body = call.args[0], call.args[1]
        if topic.endswith("/commands"):
            out.append(json.loads(body)["command"])
    return out
