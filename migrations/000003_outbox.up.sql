-- Schema v3 — scan_events outbox (M4.3).
-- Guarantees no-loss aggregation when Redis is unavailable mid-pipeline:
-- after a scan_events row is durably committed to Postgres, a failed Redis
-- SADD is parked here (idempotent on trace_id) and retried by a drain worker
-- until Redis accepts it. The durable source of truth stays scan_events; the
-- outbox only ever carries the aggregation intent that is pending Redis.
CREATE TABLE IF NOT EXISTS scan_events_outbox (
    id BIGSERIAL PRIMARY KEY,
    trace_id VARCHAR(64) NOT NULL UNIQUE,
    user_email VARCHAR(255) NOT NULL,
    ride_id VARCHAR(64) NOT NULL,
    scanned_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON scan_events_outbox(status, created_at);
