CREATE TABLE IF NOT EXISTS event (
    event_id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    event_timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert (
    alert_id SERIAL PRIMARY KEY,
    event_id INT REFERENCES event(event_id),
    severity TEXT,
    alert_timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS action (
    action_id SERIAL PRIMARY KEY,
    alert_id INT REFERENCES alert(alert_id),
    agent_id TEXT,
    action_taken TEXT,
    action_timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS latest_state (
    device_id TEXT PRIMARY KEY,
    last_seen BIGINT,
    last_temperature DOUBLE PRECISION,
    last_vibration DOUBLE PRECISION,
    status TEXT
);
