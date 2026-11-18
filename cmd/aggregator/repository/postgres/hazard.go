package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"

	"tubes-aat-pepeftw/internal/telemetry"
)

const insertHazard = `
INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name,
                           latitude, longitude, occurred_at, ingested_at, attributes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT DO NOTHING`

const insertOutbox = `
INSERT INTO outbox (event_id, hazard_id, event_type, payload, correlation_id, trace_parent)
VALUES ($1, $2, $3, $4, $5, $6)`

const selectHazardColumns = `
SELECT hazard_id::text, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events`

const getHazard = selectHazardColumns + `
WHERE hazard_id = $1`

func (p *PostgresRepository) SaveHazard(ctx context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	h, msg, payload, err := p.hazardMessage(h)
	if err != nil {
		return "", fmt.Errorf("save hazard %s: %w", h.HazardID, err)
	}

	var outcome repository.SaveOutcome
	err = withIdempotentRetry(ctx, func() error {
		outcome = repository.OutcomeUnchanged
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			created, err := insertHazardWithOutbox(ctx, tx, h, msg, payload)
			if created {
				outcome = repository.OutcomeCreated
			}
			return err
		})
	})
	if err != nil {
		return "", fmt.Errorf("save hazard %s: %w", h.HazardID, storeError(ctx, err))
	}
	return outcome, nil
}

// hazardMessage normalizes h and builds its HAZARD_CREATED outbox event.
func (p *PostgresRepository) hazardMessage(h model.HazardEvent) (model.HazardEvent, model.HazardMessage, []byte, error) {
	h = normalize(h)
	msg := model.HazardMessage{
		EventID:   uuid.NewString(),
		EventType: model.EventHazardCreated,
		HazardID:  h.HazardID,
		EmittedAt: p.now(),
		Payload:   h,
	}
	payload, err := json.Marshal(msg)
	return h, msg, payload, err
}

// insertHazardWithOutbox inserts h and, only if it is new, its outbox event. created reports which.
func insertHazardWithOutbox(ctx context.Context, tx pgx.Tx, h model.HazardEvent, msg model.HazardMessage, payload []byte) (bool, error) {
	tag, err := tx.Exec(ctx, insertHazard,
		h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
		h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.Attributes)
	if err != nil || tag.RowsAffected() == 0 {
		return false, err
	}
	if _, err := tx.Exec(ctx, insertOutbox,
		msg.EventID, h.HazardID, msg.EventType, payload, nullable(correlation.From(ctx)), nullable(telemetry.TraceParent(ctx))); err != nil {
		return false, err
	}
	return true, nil
}

func (p *PostgresRepository) GetHazard(ctx context.Context, hazardID string) (model.HazardEvent, error) {
	h, err := scanHazard(p.pool.QueryRow(ctx, getHazard, hazardID))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.HazardEvent{}, repository.ErrNotFound
	}
	if err != nil {
		return model.HazardEvent{}, fmt.Errorf("get hazard %s: %w", hazardID, err)
	}
	return h, nil
}

func scanHazard(row pgx.Row) (model.HazardEvent, error) {
	var h model.HazardEvent
	err := row.Scan(&h.HazardID, &h.Source, &h.SourceRefID, &h.HazardType, &h.Severity, &h.AreaName,
		&h.Latitude, &h.Longitude, &h.OccurredAt, &h.IngestedAt, &h.Attributes)
	h.OccurredAt, h.IngestedAt = h.OccurredAt.UTC(), h.IngestedAt.UTC()
	return h, err
}

func normalize(h model.HazardEvent) model.HazardEvent {
	h.OccurredAt = h.OccurredAt.UTC().Truncate(time.Second)
	h.IngestedAt = h.IngestedAt.UTC().Truncate(time.Second)
	if h.Attributes == nil {
		h.Attributes = map[string]any{}
	}
	return h
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
