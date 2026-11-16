BEGIN;

CREATE TABLE volcano_ref (
    volcano_id TEXT PRIMARY KEY,
    name       TEXT             NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL
);

CREATE TABLE hazard_events (
    hazard_id     UUID PRIMARY KEY,
    source        TEXT             NOT NULL CHECK (source IN ('BMKG', 'PVMBG')),
    source_ref_id TEXT             NOT NULL,
    hazard_type   TEXT             NOT NULL CHECK (hazard_type IN ('SEISMIC', 'VOLCANIC')),
    severity      TEXT             NOT NULL CHECK (severity IN ('NORMAL', 'WASPADA', 'SIAGA', 'AWAS')),
    area_name     TEXT             NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    occurred_at   TIMESTAMPTZ      NOT NULL,
    ingested_at   TIMESTAMPTZ      NOT NULL,
    attributes    JSONB            NOT NULL DEFAULT '{}',
    UNIQUE (source, source_ref_id)
);

CREATE INDEX hazard_events_ingested_at_idx ON hazard_events (ingested_at DESC, hazard_id);
CREATE INDEX hazard_events_type_ingested_at_idx ON hazard_events (hazard_type, ingested_at DESC, hazard_id);

CREATE TABLE outbox (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    hazard_id      UUID        NOT NULL UNIQUE REFERENCES hazard_events (hazard_id),
    event_type     TEXT        NOT NULL CHECK (event_type IN ('HAZARD_CREATED')),
    payload        JSONB       NOT NULL,
    correlation_id TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE source_status (
    source          TEXT PRIMARY KEY CHECK (source IN ('BMKG', 'PVMBG')),
    status          TEXT NOT NULL CHECK (status IN ('OK', 'STALE', 'UNAVAILABLE')),
    breaker_state   TEXT NOT NULL CHECK (breaker_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    last_success_at TIMESTAMPTZ,
    stale_since     TIMESTAMPTZ,
    last_error      TEXT
);

CREATE TABLE poll_cursor (
    source     TEXT        NOT NULL CHECK (source IN ('BMKG', 'PVMBG')),
    endpoint   TEXT        NOT NULL,
    last_since TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, endpoint)
);

CREATE TABLE rejected_records (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source        TEXT        NOT NULL CHECK (source IN ('BMKG', 'PVMBG')),
    endpoint      TEXT        NOT NULL,
    source_ref_id TEXT,
    reason        TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL
);

COMMIT;
