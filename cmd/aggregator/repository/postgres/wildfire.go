package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"

	"tubes-aat-pepeftw/internal/telemetry"
)

type WildfireOutcome string

const (
	WildfireCreated   WildfireOutcome = "created"
	WildfireUpdated   WildfireOutcome = "updated"
	WildfireUnchanged WildfireOutcome = "unchanged"
)

const selectHazardForUpdate = selectHazardColumns + `
WHERE hazard_id = $1
FOR UPDATE`

const updateWildfire = `
UPDATE hazard_events
SET severity = $2, area_name = $3, latitude = $4, longitude = $5, occurred_at = $6, attributes = $7
WHERE hazard_id = $1`

func (p *PostgresRepository) SaveWildfire(ctx context.Context, h model.HazardEvent) (WildfireOutcome, error) {
	h = normalize(h)

	var outcome WildfireOutcome
	err := withIdempotentRetry(ctx, func() error {
		outcome = WildfireUnchanged
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			cur, err := scanHazard(tx.QueryRow(ctx, selectHazardForUpdate, h.HazardID))
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				if _, err := tx.Exec(ctx, insertHazard,
					h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
					h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.Attributes); err != nil {
					return err
				}
				if err := p.emitWildfire(ctx, tx, h, model.EventHazardCreated); err != nil {
					return err
				}
				outcome = WildfireCreated
				return nil
			case err != nil:
				return err
			}

			if sameWildfire(cur, h) {
				return nil
			}
			h.IngestedAt = cur.IngestedAt
			if _, err := tx.Exec(ctx, updateWildfire,
				h.HazardID, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.Attributes); err != nil {
				return err
			}
			if err := p.emitWildfire(ctx, tx, h, model.EventHazardUpdated); err != nil {
				return err
			}
			outcome = WildfireUpdated
			return nil
		})
	})
	if err != nil {
		return "", fmt.Errorf("save wildfire %s: %w", h.HazardID, err)
	}
	return outcome, nil
}

func (p *PostgresRepository) emitWildfire(ctx context.Context, tx pgx.Tx, h model.HazardEvent, typ model.EventType) error {
	msg := model.HazardMessage{
		EventID:   uuid.NewString(),
		EventType: typ,
		HazardID:  h.HazardID,
		EmittedAt: p.now(),
		Payload:   h,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, insertOutbox,
		msg.EventID, h.HazardID, msg.EventType, payload, nullable(correlation.From(ctx)), nullable(telemetry.TraceParent(ctx)),
		outboxPriority(h))
	return err
}

// sameWildfire membandingkan isi yang bisa berubah antara dua versi kebakaran yang sama.
func sameWildfire(a, b model.HazardEvent) bool {
	if a.Severity != b.Severity || a.AreaName != b.AreaName ||
		a.Latitude != b.Latitude || a.Longitude != b.Longitude || !a.OccurredAt.Equal(b.OccurredAt) {
		return false
	}
	aj, err1 := json.Marshal(a.Attributes)
	bj, err2 := json.Marshal(b.Attributes)
	return err1 == nil && err2 == nil && bytes.Equal(aj, bj)
}
