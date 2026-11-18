package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/internal/poison"
)

type outcome string

const (
	outcomeSent      outcome = "sent"
	outcomeIgnored   outcome = "ignored"
	outcomeDuplicate outcome = "duplicate"
)

type notifier struct {
	seen *seenStore
	log  *slog.Logger
	now  func() time.Time
}

func newNotifier(seen *seenStore, log *slog.Logger) *notifier {
	return &notifier{seen: seen, log: log, now: time.Now}
}

// decode reads a hazard-events value. A value it refuses goes to topic unprocessed, under the
// reason code poison.Reason gives its error.
func decode(value []byte) (hazardMessage, error) {
	var msg hazardMessage
	if err := poison.Check(value); err != nil {
		return msg, err
	}
	if err := json.Unmarshal(value, &msg); err != nil {
		return msg, err
	}
	if msg.HazardID == "" {
		return msg, errors.New("missing hazard_id")
	}
	return msg, nil
}

func (n *notifier) handle(msg hazardMessage, correlationID string) outcome {
	attrs := []any{
		"correlation_id", correlationID, "hazard_id", msg.HazardID,
		"event_id", msg.EventID, "severity", msg.Payload.Severity,
	}
	if n.seen.Has(msg.HazardID) {
		n.log.Info("duplicate_skipped", attrs...)
		return outcomeDuplicate
	}

	result := outcomeIgnored
	if isAlert(msg.Payload.Severity) {
		n.log.Info("notification_sent", append(attrs,
			"hazard_type", msg.Payload.HazardType,
			"area_name", msg.Payload.AreaName,
			"source", msg.Payload.Source,
			"occurred_at", msg.Payload.OccurredAt.UTC().Format(time.RFC3339),
			"channel", "field-team",
			"delivery_lag_ms", n.now().Sub(msg.EmittedAt).Milliseconds(),
		)...)
		result = outcomeSent
	} else {
		n.log.Info("hazard_ignored", attrs...)
	}

	if err := n.seen.Add(msg.HazardID); err != nil {
		n.log.Error("dedup_persist_failed", append(attrs, "error", err.Error())...)
	}
	return result
}

func isAlert(severity string) bool {
	return severity == "SIAGA" || severity == "AWAS"
}
