"""
GreenhouseBrain — schemas.py
Validation of incoming MQTT payloads using Pydantic.

Applied in the processor, just before inserting into TimescaleDB, 
which serves as the system's only true trust boundary.
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


VALID_ZONES = {"zone_a", "zone_b", "zone_c"}

VALID_COMMANDS = {
    "IRRIGATION_ON", "IRRIGATION_OFF",
    "VENTILATION_ON", "VENTILATION_OFF",
    "ALERT_CRITICAL",
}

VALID_ALERT_TYPES = {
    "FROST_ALERT", "HEAT_STRESS",
    "VWC_CRITICAL_LOW", "VWC_CRITICAL_HIGH",
    "SENSOR_OFFLINE",
}


# Sensor payload
class SensorData(BaseModel):
    vwc:             float = Field(ge=0.0,   le=100.0)   # Volumetric Water Content (%)
    temp_soil:       float = Field(ge=-5.0,  le=60.0)    # °C
    temp_air:        float = Field(ge=-10.0, le=60.0)    # °C
    humidity_rh:     float = Field(ge=0.0,   le=100.0)   # %
    solar_radiation: float = Field(ge=0.0,   le=1500.0)  # W/m²
    co2_ppm:         float = Field(ge=300.0, le=5000.0)  # ppm


class SensorPayload(BaseModel):
    ts:      datetime
    zone_id: str
    crop:    str
    sensors: SensorData

    @field_validator("zone_id")
    @classmethod
    def zone_must_be_valid(cls, v: str) -> str:
        if v not in VALID_ZONES:
            raise ValueError(f"zone_id '{v}' not in {VALID_ZONES}")
        return v

    @field_validator("crop")
    @classmethod
    def crop_must_be_known(cls, v: str) -> str:
        if v not in {"tomato", "pepper", "cucumber"}:
            raise ValueError(f"unknown crop '{v}'")
        return v


# Command payload
class CommandPayload(BaseModel):
    ts:        datetime
    zone_id:   str
    command:   str
    reason:    str = Field(min_length=5)
    emergency: bool = False

    @field_validator("zone_id")
    @classmethod
    def zone_must_be_valid(cls, v: str) -> str:
        if v not in VALID_ZONES:
            raise ValueError(f"zone_id '{v}' not in {VALID_ZONES}")
        return v

    @field_validator("command")
    @classmethod
    def command_must_be_valid(cls, v: str) -> str:
        if v not in VALID_COMMANDS:
            raise ValueError(f"command '{v}' not in {VALID_COMMANDS}")
        return v


# Alert payload
class AlertPayload(BaseModel):
    ts:         datetime
    zone_id:    str
    alert_type: str
    severity:   Literal["INFO", "WARNING", "CRITICAL"]
    value:      float
    threshold:  float
    message:    str = Field(min_length=5)

    @field_validator("zone_id")
    @classmethod
    def zone_must_be_valid(cls, v: str) -> str:
        if v not in VALID_ZONES:
            raise ValueError(f"zone_id '{v}' not in {VALID_ZONES}")
        return v

    @field_validator("alert_type")
    @classmethod
    def alert_type_must_be_valid(cls, v: str) -> str:
        if v not in VALID_ALERT_TYPES:
            raise ValueError(f"alert_type '{v}' not in {VALID_ALERT_TYPES}")
        return v

    @model_validator(mode="after")
    def value_and_threshold_must_be_finite(self) -> "AlertPayload":
        import math
        if not math.isfinite(self.value) or not math.isfinite(self.threshold):
            raise ValueError("value and threshold must be finite numbers")
        return self
