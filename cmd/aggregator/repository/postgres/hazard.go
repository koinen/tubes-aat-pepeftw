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
)

const insertHazard = `
INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name,
                           latitude, longitude, occurred_at, ingested_at, attributes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT DO NOTHING`

const insertOutbox = `
INSERT INTO outbox (event_id, hazard_id, event_type, payload, correlation_id)
VALUES ($1, $2, $3, $4, $5)`

const selectHazardColumns = `
SELECT hazard_id::text, source, source_ref_id, hazard_type, severity, area_name,
       latitude, longitude, occurred_at, ingested_at, attributes
FROM hazard_events`

const listHazards = selectHazardColumns + `
WHERE ($1::text IS NULL OR hazard_type = $1)
  AND ($2::timestamptz IS NULL OR ingested_at >= $2)
ORDER BY ingested_at DESC, hazard_id
LIMIT $3`

const getHazard = selectHazardColumns + `
WHERE hazard_id = $1`

func (p *PostgresRepository) SaveHazard(ctx context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	h = normalize(h)
	msg := model.HazardMessage{
		EventID:   uuid.NewString(),
		EventType: model.EventHazardCreated,
		HazardID:  h.HazardID,
		EmittedAt: p.now(),
		Payload:   h,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("save hazard %s: %w", h.HazardID, err)
	}

	outcome := repository.OutcomeUnchanged
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, insertHazard,
			h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity, h.AreaName,
			h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.Attributes)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx, insertOutbox,
			msg.EventID, h.HazardID, msg.EventType, payload, nullable(correlation.From(ctx))); err != nil {
			return err
		}
		outcome = repository.OutcomeCreated
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("save hazard %s: %w", h.HazardID, err)
	}
	return outcome, nil
}

func (p *PostgresRepository) ListHazards(ctx context.Context, f repository.HazardFilter) ([]model.HazardEvent, error) {
	var hazardType *string
	if f.Type != nil {
		t := string(*f.Type)
		hazardType = &t
	}

	rows, err := p.pool.Query(ctx, listHazards, hazardType, f.Since, f.EffectiveLimit())
	if err != nil {
		return nil, fmt.Errorf("list hazards: %w", err)
	}
	hazards, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.HazardEvent, error) {
		return scanHazard(row)
	})
	if err != nil {
		return nil, fmt.Errorf("list hazards: %w", err)
	}
	if hazards == nil {
		hazards = []model.HazardEvent{}
	}
	return hazards, nil
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
