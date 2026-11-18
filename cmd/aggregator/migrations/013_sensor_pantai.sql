-- C9a: Sensor Pantai pages with an opaque cursor, and critical events are published first.

-- The sensor's last non-null next_cursor. For SENSOR_PANTAI, last_since holds the sensor's own
-- generated_at (sensor time), used to notice a sensor restart. NULL for the time-cursor sources.
ALTER TABLE poll_cursor ADD COLUMN IF NOT EXISTS cursor TEXT;

-- 1 for AWAS, else 0. The relay publishes ORDER BY priority DESC, id.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS priority INT2 NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS outbox_unpublished_priority_idx ON outbox (priority DESC, id) WHERE published_at IS NULL;

DROP INDEX IF EXISTS outbox@outbox_unpublished_idx;
