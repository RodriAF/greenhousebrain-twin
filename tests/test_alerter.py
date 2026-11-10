"""Alerter: formatting, deduplication and graceful behaviour without channels."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import alerter as a

ALERT = {"ts": "2026-06-15T04:00:00+00:00", "zone_id": "zone_a",
         "alert_type": "FROST_ALERT", "severity": "CRITICAL",
         "value": 1.0, "threshold": 5.0, "message": "Frost risk"}


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    a._alert_cooldowns.clear()
    monkeypatch.setattr(a, "WEBHOOK_URL", "http://hook.invalid")
    monkeypatch.setattr(a, "SMTP_HOST", "")
    monkeypatch.setattr(a, "send_webhook", MagicMock(return_value=True))


def test_format_alert_message_contains_key_fields():
    text = a.format_alert_message(ALERT)
    assert "Zone A" in text and "FROST_ALERT" in text and "1.0" in text


def test_format_handles_unknown_zone_and_missing_fields():
    assert "zone_x" in a.format_alert_message({"zone_id": "zone_x"})


def test_dispatch_deduplicates_within_cooldown():
    a.dispatch_alert(ALERT)
    a.dispatch_alert(ALERT)
    assert a.send_webhook.call_count == 1


def test_dispatch_does_not_mix_zones_or_types():
    a.dispatch_alert(ALERT)
    a.dispatch_alert({**ALERT, "zone_id": "zone_b"})
    a.dispatch_alert({**ALERT, "alert_type": "HEAT_STRESS"})
    assert a.send_webhook.call_count == 3


def test_malformed_message_does_not_raise():
    a.on_message(None, None, SimpleNamespace(payload=b"garbage"))
    a.send_webhook.assert_not_called()


def test_email_and_webhook_skipped_when_unconfigured(monkeypatch):
    monkeypatch.setattr(a, "WEBHOOK_URL", "")
    monkeypatch.setattr(a, "SMTP_USER", "")
    assert a.send_email(ALERT) is False
