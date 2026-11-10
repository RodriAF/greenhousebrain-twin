"""Decision engine: the rules documented in controller.py, one test each."""
from datetime import datetime, timedelta, timezone

from conftest import commands_sent


def run(c, client, zone):
    c.evaluate_zone(client, zone)
    return commands_sent(client)


def test_no_data_yet_does_nothing(controller_env, mqtt_client, make_zone):
    z = make_zone(); z.last_seen = None
    assert run(controller_env, mqtt_client, z) == []


def test_dry_soil_in_window_starts_irrigation(controller_env, mqtt_client, make_zone):
    z = make_zone(hour=10, vwc=40)
    assert run(controller_env, mqtt_client, z) == ["IRRIGATION_ON"]
    assert z.irrigating
    assert controller_env.persist_decision.call_args.kwargs["is_emergency"] is False


def test_dry_soil_at_night_is_emergency(controller_env, mqtt_client, make_zone):
    z = make_zone(hour=2, vwc=40)
    assert run(controller_env, mqtt_client, z) == ["IRRIGATION_ON"]
    assert controller_env.persist_decision.call_args.kwargs["is_emergency"] is True
    assert controller_env.persist_alert.call_args.args[1] == "VWC_CRITICAL_LOW"


def test_irrigation_stops_at_vwc_max(controller_env, mqtt_client, make_zone):
    started = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)
    z = make_zone(hour=10, vwc=81, irrigating=True, irrigating_since=started)
    assert run(controller_env, mqtt_client, z) == ["IRRIGATION_OFF"]
    assert not z.irrigating
    controller_env.persist_irrigation_summary.assert_called_once()


def test_already_irrigating_does_not_resend_on(controller_env, mqtt_client, make_zone):
    z = make_zone(hour=10, vwc=40, irrigating=True,
                  irrigating_since=datetime(2026, 6, 15, 9, 50, tzinfo=timezone.utc))
    assert run(controller_env, mqtt_client, z) == []


def test_invalid_probe_triggers_sensor_alert_not_irrigation(controller_env, mqtt_client, make_zone):
    z = make_zone(hour=10, vwc=40, vwc_valid=False, last_raw_vwc=0.0)
    assert run(controller_env, mqtt_client, z) == []
    assert controller_env.persist_alert.call_args.args[1] == "SENSOR_OFFLINE"


def test_ventilation_on_with_hysteresis(controller_env, mqtt_client, make_zone):
    z = make_zone(temp=31)
    assert run(controller_env, mqtt_client, z) == ["VENTILATION_ON"]
    mqtt_client.reset_mock()
    z.last_temp = 29                     # below threshold but inside the 2 °C band
    assert run(controller_env, mqtt_client, z) == []
    z.last_temp = 27.5                   # <= heat - 2
    assert run(controller_env, mqtt_client, z) == ["VENTILATION_OFF"]


def test_frost_raises_critical_alert(controller_env, mqtt_client, make_zone):
    z = make_zone(hour=4, temp=1)
    assert "ALERT_CRITICAL" in run(controller_env, mqtt_client, z)
    assert controller_env.persist_alert.call_args.args[1] == "FROST_ALERT"


def test_alert_cooldown_suppresses_repeats(controller_env):
    t0 = datetime(2026, 6, 15, 4, 0, tzinfo=timezone.utc)
    assert controller_env.alert_allowed("zone_a", "FROST_ALERT", t0)
    assert not controller_env.alert_allowed("zone_a", "FROST_ALERT", t0 + timedelta(minutes=5))
    assert controller_env.alert_allowed("zone_b", "FROST_ALERT", t0 + timedelta(minutes=5))  # other zone
    assert controller_env.alert_allowed("zone_a", "FROST_ALERT", t0 + timedelta(minutes=31))


def test_sensor_message_updates_state_and_ignores_duplicates(controller_env, mqtt_client, sensor_payload, monkeypatch):
    import json
    from types import SimpleNamespace
    ev = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
    monkeypatch.setattr(controller_env, "evaluate_zone", ev)
    msg = SimpleNamespace(payload=json.dumps(sensor_payload).encode())
    controller_env.on_sensor_message(mqtt_client, None, msg)
    controller_env.on_sensor_message(mqtt_client, None, msg)   # same ts -> ignored
    assert ev.call_count == 1
    assert controller_env.ZONES["zone_a"].last_vwc == 62.0
