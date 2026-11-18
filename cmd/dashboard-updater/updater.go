package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/internal/poison"
)

type updater struct {
	recent *recentHazards
	live   *broadcaster
	log    *slog.Logger
	now    func() time.Time
}

func newUpdater(recent *recentHazards, live *broadcaster, log *slog.Logger) *updater {
	return &updater{recent: recent, live: live, log: log, now: time.Now}
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

func (u *updater) handle(msg hazardMessage, correlationID string) {
	now := u.now()
	view := hazardView{
		HazardID:      msg.HazardID,
		Source:        msg.Payload.Source,
		HazardType:    msg.Payload.HazardType,
		Severity:      msg.Payload.Severity,
		AreaName:      msg.Payload.AreaName,
		OccurredAt:    msg.Payload.OccurredAt.UTC(),
		ReceivedAt:    now.UTC().Truncate(time.Second),
		CorrelationID: correlationID,
		DeliveryLagMs: now.Sub(msg.EmittedAt).Milliseconds(),
	}

	attrs := []any{"correlation_id", correlationID, "hazard_id", msg.HazardID, "severity", view.Severity}
	if !u.recent.add(view) {
		u.log.Info("duplicate_skipped", attrs...)
		return
	}
	u.live.broadcast(view)
	u.log.Info("dashboard_updated", append(attrs, "delivery_lag_ms", view.DeliveryLagMs)...)
}
