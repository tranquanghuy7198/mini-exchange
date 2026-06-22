-- Idempotency ledger for the saga consumers (Task 9).
-- Each consumed Kafka event's `event_id` is recorded here inside the same
-- transaction as the state change, so a redelivered event is a no-op.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
