package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const selectCursor = `SELECT last_since FROM poll_cursor WHERE source = $1 AND endpoint = $2`

const upsertCursor = `
INSERT INTO poll_cursor (source, endpoint, last_since, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (source, endpoint) DO UPDATE SET last_since = EXCLUDED.last_since, updated_at = now()`

func (p *PostgresRepository) GetCursor(ctx context.Context, source model.Source, endpoint string) (time.Time, bool, error) {
	var since time.Time
	err := p.pool.QueryRow(ctx, selectCursor, source, endpoint).Scan(&since)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("get cursor %s/%s: %w", source, endpoint, err)
	}
	return since.UTC(), true, nil
}

func (p *PostgresRepository) SetCursor(ctx context.Context, source model.Source, endpoint string, since time.Time) error {
	if _, err := p.pool.Exec(ctx, upsertCursor, source, endpoint, since.UTC()); err != nil {
		return fmt.Errorf("set cursor %s/%s: %w", source, endpoint, err)
	}
	return nil
}
