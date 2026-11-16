ALTER INDEX hazard_events@hazard_events_ingested_at_hidx SPLIT AT VALUES (0), (1), (2), (3), (4), (5), (6), (7);

ALTER INDEX hazard_events@hazard_events_type_ingested_at_hidx SPLIT AT VALUES (0), (1), (2), (3), (4), (5), (6), (7);

ALTER INDEX hazard_events@hazard_events_source_ref_hidx SPLIT AT VALUES (0), (1), (2), (3), (4), (5), (6), (7);

ALTER TABLE outbox SPLIT AT VALUES (0), (1), (2), (3), (4), (5), (6), (7);

ALTER TABLE hazard_events SCATTER;

ALTER TABLE outbox SCATTER;
