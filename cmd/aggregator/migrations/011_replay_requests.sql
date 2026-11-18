-- C5: replay jobs for parked unprocessed messages.

CREATE TABLE IF NOT EXISTS replay_requests (
    job_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source           TEXT        NOT NULL CONSTRAINT replay_requests_source_check CHECK (source IN ('BMKG', 'PVMBG', 'SATELIT', 'SENSOR_PANTAI')),
    status           TEXT        NOT NULL DEFAULT 'queued' CONSTRAINT replay_requests_status_check CHECK (status IN ('queued', 'running', 'done', 'failed')),
    requested_by     TEXT        NOT NULL,
    -- The canonical rule string of the pod that accepted the request; a leader with other rules waits.
    adaptations      TEXT        NOT NULL,
    -- The job's created_at: only rows received until then are selected, so the job always ends.
    cutoff           TIMESTAMPTZ NOT NULL,
    -- The last row the job finished, in (received_at, message_id) order; NULL before the first.
    last_received_at TIMESTAMPTZ,
    last_message_id  TEXT,
    selected         INT8        NOT NULL DEFAULT 0,
    recovered        INT8        NOT NULL DEFAULT 0,
    still_parked     INT8        NOT NULL DEFAULT 0,
    handed_to_retry  INT8        NOT NULL DEFAULT 0,
    not_replayable   INT8        NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

-- One active job per source.
CREATE UNIQUE INDEX IF NOT EXISTS replay_requests_one_active_idx ON replay_requests (source) WHERE status IN ('queued', 'running');

-- A replay job's keyset pages over one source's parked rows.
CREATE INDEX IF NOT EXISTS unprocessed_messages_parked_replay_idx ON unprocessed_messages (source, received_at, message_id) WHERE status = 'parked';
