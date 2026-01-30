CREATE TABLE IF NOT EXISTS telemetry_events (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  temperature_c DOUBLE PRECISION NOT NULL,
  vibration DOUBLE PRECISION NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_ts ON telemetry_events(device_id, ts DESC);
CREATE INDEX IF NOT EXISTS brin_telemetry_ts ON telemetry_events USING BRIN(ts);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  reason TEXT NOT NULL,
  source_ts TIMESTAMPTZ NOT NULL,
  alert_ts TIMESTAMPTZ NOT NULL,
  temperature_c DOUBLE PRECISION NOT NULL,
  vibration DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_device_ts ON alerts(device_id, alert_ts DESC);

CREATE TABLE IF NOT EXISTS actions (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  decision_reason TEXT NOT NULL,
  action_ts TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
);

CREATE INDEX IF NOT EXISTS idx_actions_device_ts ON actions(device_id, action_ts DESC);

CREATE TABLE IF NOT EXISTS device_state (
  device_id TEXT PRIMARY KEY,
  last_seen_ts TIMESTAMPTZ,
  severity TEXT,
  last_reason TEXT,
  temperature_c DOUBLE PRECISION,
  vibration DOUBLE PRECISION,
  last_alert_ts TIMESTAMPTZ,
  last_action_ts TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
