ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_source_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));

ALTER TABLE hazard_events DROP CONSTRAINT IF EXISTS hazard_events_hazard_type_check;
ALTER TABLE hazard_events ADD CONSTRAINT hazard_events_hazard_type_check CHECK (hazard_type IN ('SEISMIC', 'VOLCANIC', 'WILDFIRE', 'COASTAL'));

ALTER TABLE source_status DROP CONSTRAINT IF EXISTS source_status_source_check;
ALTER TABLE source_status ADD CONSTRAINT source_status_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));

ALTER TABLE poll_cursor DROP CONSTRAINT IF EXISTS poll_cursor_source_check;
ALTER TABLE poll_cursor ADD CONSTRAINT poll_cursor_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));

ALTER TABLE rejected_records DROP CONSTRAINT IF EXISTS rejected_records_source_check;
ALTER TABLE rejected_records ADD CONSTRAINT rejected_records_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI'));

CREATE TABLE IF NOT EXISTS unprocessed_messages (
    message_id      TEXT PRIMARY KEY,
    source          TEXT        NOT NULL CONSTRAINT unprocessed_messages_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')),
    endpoint        TEXT,
    source_ref_id   TEXT,
    received_at     TIMESTAMPTZ NOT NULL,
    reason          TEXT        NOT NULL,
    retryable       BOOLEAN     NOT NULL,
    attempts        INT         NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL CONSTRAINT unprocessed_messages_status_check CHECK (status IN ('retrying', 'parked', 'recovered')),
    payload         BYTEA       NOT NULL,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS unprocessed_messages_status_received_idx ON unprocessed_messages (status, received_at);
