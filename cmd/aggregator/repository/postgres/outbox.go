package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const selectPendingOutbox = `
SELECT id, event_id::text, hazard_id::text, payload, COALESCE(correlation_id, '')
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1`

const markOutboxPublished = `
UPDATE outbox SET published_at = now()
WHERE id = ANY($1) AND published_at IS NULL`

func (p *PostgresRepository) PendingOutbox(ctx context.Context, limit int) ([]model.OutboxRecord, error) {
	rows, err := p.pool.Query(ctx, selectPendingOutbox, limit)
	if err != nil {
		return nil, fmt.Errorf("pending outbox: %w", err)
	}
	records, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.OutboxRecord, error) {
		var r model.OutboxRecord
		err := row.Scan(&r.ID, &r.EventID, &r.HazardID, &r.Payload, &r.CorrelationID)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("pending outbox: %w", err)
	}
	return records, nil
}

func (p *PostgresRepository) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := p.pool.Exec(ctx, markOutboxPublished, ids); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}
