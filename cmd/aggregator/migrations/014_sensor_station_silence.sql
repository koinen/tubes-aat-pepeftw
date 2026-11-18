-- Sensor Pantai's latest station-silence snapshot, shared across aggregator replicas.
ALTER TABLE source_status ADD COLUMN IF NOT EXISTS station_silence JSONB;
