package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/internal/poison"
)

type outcome string

const (
	outcomeRecorded  outcome = "recorded"
	outcomeDuplicate outcome = "duplicate"
)

type auditor struct {
	seen *seenStore
	log  *slog.Logger
}

func newAuditor(seen *seenStore, log *slog.Logger) *auditor {
	return &auditor{seen: seen, log: log}
}

// decode reads a hazard-events value and its payload. A value it refuses goes to topic
// unprocessed, under the reason code poison.Reason gives its error.
func decode(value []byte) (hazardMessage, hazardPayload, error) {
	var (
		msg hazardMessage
		p   hazardPayload
	)
	if err := poison.Check(value); err != nil {
		return msg, p, err
	}
	if err := json.Unmarshal(value, &msg); err != nil {
		return msg, p, err
	}
	if msg.HazardID == "" {
		return msg, p, errors.New("missing hazard_id")
	}
	if len(msg.Payload) > 0 {
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return msg, p, fmt.Errorf("payload: %w", err)
		}
	}
	return msg, p, nil
}

func (a *auditor) handle(msg hazardMessage, p hazardPayload, correlationID string, partition int32, offset int64) outcome {
	where := []any{"correlation_id", correlationID, "partition", partition, "offset", offset}

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
