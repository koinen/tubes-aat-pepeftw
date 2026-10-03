package postgres

import (
	"context"
	"encoding/json"
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
