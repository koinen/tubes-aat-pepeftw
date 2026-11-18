-- C3: the write side of unprocessed_messages, which replaces rejected_records.

ALTER TABLE unprocessed_messages ADD COLUMN IF NOT EXISTS payload_size INT8;
ALTER TABLE unprocessed_messages ADD COLUMN IF NOT EXISTS payload_sha256 TEXT;
ALTER TABLE unprocessed_messages ADD COLUMN IF NOT EXISTS hazard JSONB;
ALTER TABLE unprocessed_messages ADD COLUMN IF NOT EXISTS correlation_id TEXT;

CREATE INDEX IF NOT EXISTS unprocessed_messages_retry_due_idx ON unprocessed_messages (next_attempt_at) WHERE status = 'retrying';

DROP TABLE IF EXISTS rejected_records;
