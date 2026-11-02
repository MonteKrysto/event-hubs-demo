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
