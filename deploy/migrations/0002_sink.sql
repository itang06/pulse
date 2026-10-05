-- Durable idempotency ledger. Kept as an ordinary table so event_id can be
-- globally unique independently of the event-time partitioning below.
CREATE TABLE IF NOT EXISTS event_receipts (
    event_id UUID PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id UUID NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    service_name TEXT NOT NULL,
    route TEXT NOT NULL,
    latency_us BIGINT NOT NULL,
    status_code INTEGER NOT NULL,
    trace_id TEXT NOT NULL,
    attributes JSONB NOT NULL,
    ewma_mean_us DOUBLE PRECISION NOT NULL,
    ewma_stddev_us DOUBLE PRECISION NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('telemetry_events', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS telemetry_events_service_route_time_idx
    ON telemetry_events (service_name, route, event_time DESC);
