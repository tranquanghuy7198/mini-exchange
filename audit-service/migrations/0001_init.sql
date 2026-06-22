-- Audit Service schema (Task 5).
-- Owns: audit_events. `event_id` is UNIQUE so re-delivered Kafka events are
-- recorded at most once (idempotent consumption).

CREATE TABLE IF NOT EXISTS audit_events (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID NOT NULL UNIQUE,
    order_id     UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_order_id ON audit_events (order_id);
