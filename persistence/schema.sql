CREATE TABLE IF NOT EXISTS telemetry_events (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS alert_events (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    alert_type TEXT,
    severity TEXT,
    reason TEXT,
    telemetry_event_id INT REFERENCES telemetry_events(id)
);

CREATE TABLE IF NOT EXISTS action_events (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    action_type TEXT,
    decision_reason TEXT,
    confidence DOUBLE PRECISION,
    alert_event_id INT REFERENCES alert_events(id)
);

CREATE TABLE IF NOT EXISTS latest_state (
    device_id TEXT PRIMARY KEY,
    last_seen BIGINT,
    last_temperature DOUBLE PRECISION,
    last_vibration DOUBLE PRECISION,
    status TEXT
);
