package main

import (
	"encoding/json"
	"log/slog"
	"time"
)

type outcome string

const (
	outcomeSent      outcome = "sent"
	outcomeIgnored   outcome = "ignored"
	outcomeDuplicate outcome = "duplicate"
	outcomeInvalid   outcome = "invalid"
)

type notifier struct {
	seen map[string]struct{}
	log  *slog.Logger
	now  func() time.Time
}

func newNotifier(log *slog.Logger) *notifier {
	return &notifier{seen: map[string]struct{}{}, log: log, now: time.Now}
}

func (n *notifier) handle(value []byte, correlationID string) outcome {
	var msg hazardMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		n.log.Error("message_invalid", "correlation_id", correlationID, "error", err.Error())
		return outcomeInvalid
	}
	if msg.HazardID == "" {
		n.log.Error("message_invalid", "correlation_id", correlationID, "error", "missing hazard_id")
		return outcomeInvalid
	}

	attrs := []any{
		"correlation_id", correlationID, "hazard_id", msg.HazardID,
		"event_id", msg.EventID, "severity", msg.Payload.Severity,
	}
	if _, seen := n.seen[msg.HazardID]; seen {
		n.log.Info("duplicate_skipped", attrs...)
		return outcomeDuplicate
	}
	n.seen[msg.HazardID] = struct{}{}

	if !isAlert(msg.Payload.Severity) {
		n.log.Info("hazard_ignored", attrs...)
		return outcomeIgnored
	}

	n.log.Info("notification_sent", append(attrs,
		"hazard_type", msg.Payload.HazardType,
		"area_name", msg.Payload.AreaName,
		"source", msg.Payload.Source,
		"occurred_at", msg.Payload.OccurredAt.UTC().Format(time.RFC3339),
		"channel", "field-team",
		"delivery_lag_ms", n.now().Sub(msg.EmittedAt).Milliseconds(),
	)...)
	return outcomeSent
}

func isAlert(severity string) bool {
	return severity == "SIAGA" || severity == "AWAS"
}
