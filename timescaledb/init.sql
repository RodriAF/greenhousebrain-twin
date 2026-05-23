-- ============================================================
-- GreenhouseBrain — TimescaleDB Schema
-- Precision Agriculture Data Layer
-- ============================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────────────────────
-- SENSOR READINGS — Raw telemetry from all zones
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sensor_readings (
    time            TIMESTAMPTZ NOT NULL,
    zone_id         VARCHAR(10) NOT NULL,       -- zone_a, zone_b, zone_c
    crop            VARCHAR(50) NOT NULL,        -- tomato, pepper, cucumber
    vwc             DOUBLE PRECISION,            -- Volumetric Water Content (%)
    temp_soil       DOUBLE PRECISION,            -- Soil temperature (°C)
    temp_air        DOUBLE PRECISION,            -- Air temperature (°C)
    humidity_rh     DOUBLE PRECISION,            -- Relative humidity (%)
    solar_radiation DOUBLE PRECISION,            -- Solar radiation (W/m²)
    co2_ppm         DOUBLE PRECISION,            -- CO2 concentration (ppm)
    raw_payload     JSONB                        -- Full original payload
);

-- Convert to hypertable partitioned by time (7-day chunks for hot data)
SELECT create_hypertable(
    'sensor_readings',
    'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_sensor_zone_time
    ON sensor_readings (zone_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_time
    ON sensor_readings (time DESC);

ALTER TABLE sensor_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'zone_id'
);

-- Compression policy: compress chunks older than 30 days
SELECT add_compression_policy('sensor_readings', INTERVAL '30 days');

-- Retention policy: drop data older than 2 years
SELECT add_retention_policy('sensor_readings', INTERVAL '2 years');


-- ─────────────────────────────────────────────────────────────
-- IRRIGATION EVENTS — Commands issued by the controller
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS irrigation_events (
    time            TIMESTAMPTZ NOT NULL,
    zone_id         VARCHAR(10) NOT NULL,
    command         VARCHAR(30) NOT NULL,        -- IRRIGATION_ON, IRRIGATION_OFF, VENTILATION_ON, etc.
    vwc_at_trigger  DOUBLE PRECISION,            -- VWC value that triggered the command
    temp_at_trigger DOUBLE PRECISION,            -- Temp at decision time
    reason          TEXT,                        -- Human-readable justification
    is_emergency    BOOLEAN DEFAULT FALSE,       -- Night irrigation override
    duration_sec    INTEGER,                     -- Filled on IRRIGATION_OFF
    liters_est      DOUBLE PRECISION            -- Estimated water use (L)
);

SELECT create_hypertable(
    'irrigation_events',
    'time',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_irrigation_zone_time
    ON irrigation_events (zone_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_irrigation_command
    ON irrigation_events (command, time DESC);


-- ─────────────────────────────────────────────────────────────
-- ALERTS — Critical condition log
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    time            TIMESTAMPTZ NOT NULL,
    zone_id         VARCHAR(10) NOT NULL,
    alert_type      VARCHAR(50) NOT NULL,        -- FROST_ALERT, HEAT_STRESS, VWC_CRITICAL_LOW, etc.
    severity        VARCHAR(10) NOT NULL,        -- INFO, WARNING, CRITICAL
    value           DOUBLE PRECISION,            -- Sensor value that triggered alert
    threshold       DOUBLE PRECISION,            -- Threshold that was crossed
    message         TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    ack_time        TIMESTAMPTZ,
    ack_by          VARCHAR(100)
);

SELECT create_hypertable(
    'alerts',
    'time',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_alerts_zone_time
    ON alerts (zone_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_severity
    ON alerts (severity, time DESC);


-- ─────────────────────────────────────────────────────────────
-- ETo DAILY — Evapotranspiration per zone (Penman-Monteith)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eto_daily (
    time            TIMESTAMPTZ NOT NULL,        -- Date (truncated to day)
    zone_id         VARCHAR(10) NOT NULL,
    eto_mm          DOUBLE PRECISION,            -- Reference ETo (mm/day)
    temp_max        DOUBLE PRECISION,
    temp_min        DOUBLE PRECISION,
    humidity_mean   DOUBLE PRECISION,
    radiation_mean  DOUBLE PRECISION,
    irrigation_mm   DOUBLE PRECISION             -- Irrigation applied (estimated mm)
);

SELECT create_hypertable(
    'eto_daily',
    'time',
    chunk_time_interval => INTERVAL '90 days',
    if_not_exists => TRUE
);


-- ─────────────────────────────────────────────────────────────
-- CONTINUOUS AGGREGATES — Pre-computed hourly averages
-- ─────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    zone_id,
    crop,
    AVG(vwc)             AS vwc_avg,
    MIN(vwc)             AS vwc_min,
    MAX(vwc)             AS vwc_max,
    AVG(temp_air)        AS temp_air_avg,
    MAX(temp_air)        AS temp_air_max,
    MIN(temp_air)        AS temp_air_min,
    AVG(humidity_rh)     AS humidity_avg,
    AVG(solar_radiation) AS radiation_avg,
    AVG(co2_ppm)         AS co2_avg,
    COUNT(*)             AS sample_count
FROM sensor_readings
GROUP BY bucket, zone_id, crop
WITH NO DATA;

-- Refresh policy: update hourly aggregate every 30 minutes
SELECT add_continuous_aggregate_policy(
    'sensor_hourly',
    start_offset  => INTERVAL '3 hours',
    end_offset    => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);

-- Daily aggregate for season analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    zone_id,
    crop,
    AVG(vwc)             AS vwc_avg,
    MIN(vwc)             AS vwc_min,
    MAX(vwc)             AS vwc_max,
    AVG(temp_air)        AS temp_air_avg,
    MAX(temp_air)        AS temp_air_max,
    MIN(temp_air)        AS temp_air_min,
    AVG(humidity_rh)     AS humidity_avg,
    SUM(solar_radiation) AS radiation_sum,
    AVG(co2_ppm)         AS co2_avg
FROM sensor_readings
GROUP BY bucket, zone_id, crop
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'sensor_daily',
    start_offset  => INTERVAL '3 days',
    end_offset    => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);


-- ─────────────────────────────────────────────────────────────
-- ZONE METADATA — Static crop/zone information
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zone_metadata (
    zone_id         VARCHAR(10) PRIMARY KEY,
    zone_name       VARCHAR(100),
    crop            VARCHAR(50),
    crop_scientific VARCHAR(100),
    area_m2         DOUBLE PRECISION,
    plant_count     INTEGER,
    planting_date   DATE,
    expected_harvest DATE,
    notes           TEXT
);

INSERT INTO zone_metadata (zone_id, zone_name, crop, crop_scientific, area_m2, plant_count, notes)
VALUES
    ('zone_a', 'Zona A — Invernadero Norte', 'tomato',   'Solanum lycopersicum', 800,  1200, 'Variedad Raf, cultivo en coco'),
    ('zone_b', 'Zona B — Invernadero Centro', 'pepper',  'Capsicum annuum',      600,   950, 'Pimiento rojo California'),
    ('zone_c', 'Zona C — Invernadero Sur',   'cucumber', 'Cucumis sativus',      700,   800, 'Pepino largo tipo holandés')
ON CONFLICT (zone_id) DO NOTHING;
