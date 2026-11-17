CREATE TABLE IF NOT EXISTS satellite_feed_state (
    id             BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    last_offset    BIGINT      NOT NULL,
    max_event_time TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO satellite_feed_state (id, last_offset) VALUES (true, -1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS satellite_detection (
    detection_key TEXT PRIMARY KEY,
    satellite     TEXT             NOT NULL,
    acq_time      TIMESTAMPTZ      NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    confidence    TEXT             NOT NULL,
    frp           DOUBLE PRECISION NOT NULL,
    retracted     BOOLEAN          NOT NULL DEFAULT false,
    first_offset  BIGINT           NOT NULL,
    last_offset   BIGINT           NOT NULL,
    received_at   TIMESTAMPTZ      NOT NULL
);

CREATE INDEX IF NOT EXISTS satellite_detection_acq_time_idx ON satellite_detection (acq_time);

CREATE TABLE IF NOT EXISTS satellite_late (
    feed_offset   BIGINT PRIMARY KEY,
    detection_key TEXT        NOT NULL,
    acq_time      TIMESTAMPTZ NOT NULL,
    watermark     TIMESTAMPTZ NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL
);
