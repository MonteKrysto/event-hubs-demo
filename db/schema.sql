-- Local R1DP substitute. PostgreSQL owns persistence; Kafka offsets are diagnostic.
CREATE TABLE IF NOT EXISTS events (
    ingest_id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    source_account_id TEXT NOT NULL,
    source_encounter_id TEXT NOT NULL,
    source_sequence BIGINT NOT NULL CHECK (source_sequence > 0),
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    payload JSONB NOT NULL,
    UNIQUE (tenant_id, source_account_id, source_encounter_id, source_sequence)
);
CREATE TABLE IF NOT EXISTS rejections (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    reason TEXT NOT NULL,
    raw_base64 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
    id BIGSERIAL PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    event_id UUID,
    result TEXT NOT NULL CHECK (result IN ('stored', 'duplicate', 'rejected'))
);
CREATE TABLE IF NOT EXISTS flow_batches (
    id BIGSERIAL PRIMARY KEY,
    pulled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    through_ingest_id BIGINT NOT NULL,
    report JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS demo_scenarios (
    name TEXT PRIMARY KEY,
    events JSONB NOT NULL
);
-- Browser demo state is separate from the event history and the CLI fixtures.
CREATE TABLE IF NOT EXISTS ui_scenarios (
    id UUID PRIMARY KEY,
    number BIGSERIAL UNIQUE NOT NULL,
    identity JSONB NOT NULL,
    claim JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS ui_state (
    name TEXT PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id)
);
-- Keep existing scenarios and their immutable event history when adding context.
ALTER TABLE ui_scenarios ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}';
CREATE UNIQUE INDEX IF NOT EXISTS ui_sample_encounter_key ON ui_scenarios ((details->>'sample_key')) WHERE details ? 'sample_key';
CREATE TABLE IF NOT EXISTS ui_publications (
    event_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id),
    source_sequence BIGINT NOT NULL,
    event JSONB NOT NULL,
    publication_state TEXT NOT NULL CHECK (publication_state IN ('pending', 'published', 'unconfirmed')),
    acknowledgment JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (scenario_id, source_sequence)
);
CREATE TABLE IF NOT EXISTS ui_pulls (
    id BIGSERIAL PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id),
    events JSONB NOT NULL,
    pulled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS ui_reviews (
    id BIGSERIAL PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id),
    pull_id BIGINT NOT NULL REFERENCES ui_pulls(id),
    decision JSONB NOT NULL,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS service_heartbeats (
    name TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- The browser's simulated vendor and our receiver share a local PostgreSQL
-- instance, but communicate over HTTP and commit separate transactions.
-- ui_publications is the simulated Humata change history. Only the Kafka sink
-- writes the downstream events table, including during API recovery.
CREATE TABLE IF NOT EXISTS integration_state (
    scenario_id UUID PRIMARY KEY REFERENCES ui_scenarios(id),
    receiver_available BOOLEAN NOT NULL DEFAULT TRUE,
    publisher_paused BOOLEAN NOT NULL DEFAULT FALSE,
    api_available BOOLEAN NOT NULL DEFAULT TRUE,
    skip_next_webhook BOOLEAN NOT NULL DEFAULT FALSE,
    automatic_recovery BOOLEAN NOT NULL DEFAULT FALSE,
    recovery_cursor BIGINT NOT NULL DEFAULT 0,
    last_recovery_at TIMESTAMPTZ,
    last_recovery_attempt_at TIMESTAMPTZ,
    recovery_error TEXT,
    recovery_token UUID,
    recovery_until TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS integration_webhooks (
    event_id UUID PRIMARY KEY REFERENCES ui_publications(event_id),
    state TEXT NOT NULL CHECK (state IN ('queued','delivered','failed','skipped')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS integration_inbox (
    event_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id),
    event JSONB NOT NULL,
    received_via TEXT NOT NULL CHECK (received_via IN ('webhook','api')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX IF NOT EXISTS integration_inbox_revision ON integration_inbox
    ((event->>'tenant_id'),(event->>'source_account_id'),(event->>'source_encounter_id'),((event->>'source_sequence')::BIGINT));
CREATE TABLE IF NOT EXISTS integration_outbox (
    event_id UUID PRIMARY KEY REFERENCES integration_inbox(event_id),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','sent')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error TEXT,
    sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS integration_outbox_pending ON integration_outbox(next_attempt_at) WHERE state='pending';
CREATE TABLE IF NOT EXISTS integration_activity (
    id BIGSERIAL PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES ui_scenarios(id),
    event_id UUID,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS integration_activity_scenario ON integration_activity(scenario_id,id DESC);
