CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry (
  ts TIMESTAMPTZ NOT NULL,
  device_id TEXT NOT NULL,
  pgn INT NOT NULL,
  src INT NOT NULL,
  dst INT NOT NULL,
  fields JSONB,
  ingest_ts TIMESTAMPTZ DEFAULT now(),
  broker_topic TEXT
);

SELECT create_hypertable('telemetry','ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- indexes
CREATE INDEX IF NOT EXISTS idx_tel_ts_desc_pgn_dev ON telemetry (ts DESC, pgn, device_id);
CREATE INDEX IF NOT EXISTS idx_tel_dev_ts ON telemetry (device_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_tel_pgn_ts ON telemetry (pgn, ts DESC);

-- retention: keep 30 days
SELECT add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);

-- DLQ for bad payloads
CREATE TABLE IF NOT EXISTS dlq (
  id BIGSERIAL PRIMARY KEY,
  ingest_ts TIMESTAMPTZ DEFAULT now(),
  broker_topic TEXT,
  -- payload JSONB,
  payload BYTEA,
  error TEXT
);

-- Optional continuous aggregate (1m)
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket,
       pgn, device_id,
       avg( (fields->>'temperature')::float ) AS avg_temp,
       avg( (fields->>'voltage')::float )     AS avg_voltage,
       count(*) AS samples
FROM telemetry
GROUP BY bucket, pgn, device_id;

SELECT add_continuous_aggregate_policy(
  'telemetry_1m',
  start_offset => INTERVAL '2 hours',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute'
);
