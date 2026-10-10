package main

import (
	"encoding/json"
	"log/slog"
	"time"
)

type outcome string

const (
	outcomeRecorded  outcome = "recorded"
	outcomeDuplicate outcome = "duplicate"
	outcomeInvalid   outcome = "invalid"
)

type auditor struct {
	seen *seenStore
	log  *slog.Logger
}

func newAuditor(seen *seenStore, log *slog.Logger) *auditor {
	return &auditor{seen: seen, log: log}
}

func (a *auditor) handle(value []byte, correlationID string, partition int32, offset int64) outcome {
	where := []any{"correlation_id", correlationID, "partition", partition, "offset", offset}

	var msg hazardMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		a.log.Error("message_invalid", append(where, "error", err.Error())...)
		return outcomeInvalid
	}
	if msg.HazardID == "" {
		a.log.Error("message_invalid", append(where, "error", "missing hazard_id")...)
		return outcomeInvalid
	}
	var p hazardPayload
	if len(msg.Payload) > 0 {
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			a.log.Error("message_invalid", append(where, "error", "payload: "+err.Error())...)
			return outcomeInvalid
		}
	}

	attrs := append(where, "hazard_id", msg.HazardID, "event_id", msg.EventID)
	if a.seen.Has(msg.HazardID) {
		a.log.Info("duplicate_skipped", attrs...)
		return outcomeDuplicate
	}
	a.log.Info("audit_recorded", append(attrs,
		"event_type", msg.EventType,
		"severity", p.Severity,
		"source", p.Source,
		"source_ref_id", p.SourceRefID,
		"hazard_type", p.HazardType,
		"area_name", p.AreaName,
		"occurred_at", p.OccurredAt.UTC().Format(time.RFC3339),
		"emitted_at", msg.EmittedAt.UTC().Format(time.RFC3339),
		"payload", msg.Payload,
	)...)

	if err := a.seen.Add(msg.HazardID); err != nil {
		a.log.Error("dedup_persist_failed", append(attrs, "error", err.Error())...)
	}
	return outcomeRecorded
}
