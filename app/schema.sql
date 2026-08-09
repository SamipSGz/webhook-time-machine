-- Inbound endpoints: each has a token senders POST to, and a destination URL we forward to.
CREATE TABLE IF NOT EXISTS endpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    token           TEXT NOT NULL UNIQUE,
    destination_url TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every captured webhook event (raw body archived in S3, referenced by body_key).
CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    idempotency_key TEXT,
    headers         JSONB NOT NULL DEFAULT '{}',
    body_key        TEXT,               -- S3 object key for the raw payload
    body_size       INTEGER NOT NULL DEFAULT 0,
    is_duplicate    BOOLEAN NOT NULL DEFAULT FALSE,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_endpoint ON events(endpoint_id, received_at DESC);

-- One delivery row per event; tracks lifecycle across retries.
-- status: pending | success | failed | dead
CREATE TABLE IF NOT EXISTS deliveries (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id          UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id       UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status            TEXT NOT NULL DEFAULT 'pending',
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_status_code  INTEGER,
    last_error        TEXT,
    last_attempt_at   TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_deliveries_endpoint ON deliveries(endpoint_id, created_at DESC);
