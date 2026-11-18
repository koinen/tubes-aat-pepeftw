-- C7: field reports from topic field-reports, stored once per (subject, Idempotency-Key).

CREATE TABLE IF NOT EXISTS field_reports (
    -- UUIDv5(subject, idempotency_key), made by client-api; also the Kafka record key.
    report_id       UUID             PRIMARY KEY,
    subject         TEXT             NOT NULL,
    idempotency_key TEXT             NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    reported_at     TIMESTAMPTZ      NOT NULL,
    description     TEXT             NOT NULL,
    hazard_id       UUID,
    -- sha256 of the report's canonical JSON: a resend with another body under the same key is
    -- told apart from a duplicate and parked as idempotency_conflict.
    body_sha256     TEXT             NOT NULL,
    accepted_at     TIMESTAMPTZ      NOT NULL,
    correlation_id  TEXT,
    -- Where the stored copy was read: field-reports/<partition>/<offset>.
    kafka_partition INT4             NOT NULL,
    kafka_offset    INT8             NOT NULL,
    stored_at       TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT field_reports_subject_key_unique UNIQUE (subject, idempotency_key)
);

-- Unprocessed messages may now come from the field team (TIM_LAPANGAN) and, in C4b, from our own
-- Kafka consumers (INTERNAL). Hazard events keep their four sources.
ALTER TABLE unprocessed_messages DROP CONSTRAINT IF EXISTS unprocessed_messages_source_check;
ALTER TABLE unprocessed_messages ADD CONSTRAINT unprocessed_messages_source_check
    CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI', 'TIM_LAPANGAN', 'INTERNAL'));
