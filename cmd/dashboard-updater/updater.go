package main

import (
	"encoding/json"
	"log/slog"
	"time"
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

func (u *updater) handle(value []byte, correlationID string) {
	var msg hazardMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		u.log.Error("message_invalid", "correlation_id", correlationID, "error", err.Error())
		return
	}
	if msg.HazardID == "" {
		u.log.Error("message_invalid", "correlation_id", correlationID, "error", "missing hazard_id")
		return
	}

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
