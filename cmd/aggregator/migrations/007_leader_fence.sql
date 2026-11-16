CREATE TABLE IF NOT EXISTS leader_fence (
    id    BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    epoch BIGINT  NOT NULL
);

INSERT INTO leader_fence (id, epoch) VALUES (true, 0) ON CONFLICT DO NOTHING;
