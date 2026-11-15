CREATE TABLE IF NOT EXISTS volcano_ref (
    volcano_id TEXT PRIMARY KEY,
    name       TEXT             NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id     UUID PRIMARY KEY,
    source        TEXT             NOT NULL CONSTRAINT hazard_events_source_check CHECK (source IN ('BMKG', 'PVMBG')),
    source_ref_id TEXT             NOT NULL,
    hazard_type   TEXT             NOT NULL CONSTRAINT hazard_events_hazard_type_check CHECK (hazard_type IN ('SEISMIC', 'VOLCANIC')),
    severity      TEXT             NOT NULL CONSTRAINT hazard_events_severity_check CHECK (severity IN ('NORMAL', 'WASPADA', 'SIAGA', 'AWAS')),
    area_name     TEXT             NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    occurred_at   TIMESTAMPTZ      NOT NULL,
    ingested_at   TIMESTAMPTZ      NOT NULL,
    attributes    JSONB            NOT NULL DEFAULT '{}',
    UNIQUE (source, source_ref_id)
);

CREATE INDEX IF NOT EXISTS hazard_events_ingested_at_idx ON hazard_events (ingested_at DESC, hazard_id);
CREATE INDEX IF NOT EXISTS hazard_events_type_ingested_at_idx ON hazard_events (hazard_type, ingested_at DESC, hazard_id);

CREATE TABLE IF NOT EXISTS outbox (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    hazard_id      UUID        NOT NULL UNIQUE REFERENCES hazard_events (hazard_id),
    event_type     TEXT        NOT NULL CONSTRAINT outbox_event_type_check CHECK (event_type IN ('HAZARD_CREATED')),
    payload        JSONB       NOT NULL,
    correlation_id TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS source_status (
    source          TEXT PRIMARY KEY CONSTRAINT source_status_source_check CHECK (source IN ('BMKG', 'PVMBG')),
    status          TEXT NOT NULL CONSTRAINT source_status_status_check CHECK (status IN ('OK', 'STALE', 'UNAVAILABLE')),
    breaker_state   TEXT NOT NULL CONSTRAINT source_status_breaker_state_check CHECK (breaker_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    last_success_at TIMESTAMPTZ,
    stale_since     TIMESTAMPTZ,
    last_error      TEXT
);

CREATE TABLE IF NOT EXISTS poll_cursor (
    source     TEXT        NOT NULL CONSTRAINT poll_cursor_source_check CHECK (source IN ('BMKG', 'PVMBG')),
    endpoint   TEXT        NOT NULL,
    last_since TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, endpoint)
);

CREATE TABLE IF NOT EXISTS rejected_records (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source        TEXT        NOT NULL CONSTRAINT rejected_records_source_check CHECK (source IN ('BMKG', 'PVMBG')),
    endpoint      TEXT        NOT NULL,
    source_ref_id TEXT,
    reason        TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL
);
