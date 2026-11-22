ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_event_type_check;

ALTER TABLE outbox ADD CONSTRAINT outbox_event_type_check CHECK (event_type IN ('HAZARD_CREATED', 'HAZARD_UPDATED'));

DROP INDEX IF EXISTS outbox@outbox_hazard_id_key;

CREATE INDEX IF NOT EXISTS satellite_detection_last_offset_idx ON satellite_detection (last_offset);
