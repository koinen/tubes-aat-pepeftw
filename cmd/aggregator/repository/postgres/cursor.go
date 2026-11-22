package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
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
	if _, err := p.fencedExecRetry(ctx, upsertCursor, source, endpoint, since.UTC()); err != nil {
		return fmt.Errorf("set cursor %s/%s: %w", source, endpoint, err)
	}
	return nil
}

const selectStreamPosition = `SELECT COALESCE(cursor, ''), last_since FROM poll_cursor WHERE source = $1 AND endpoint = $2`

const upsertStreamPosition = `
INSERT INTO poll_cursor (source, endpoint, last_since, cursor, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (source, endpoint) DO UPDATE SET last_since = EXCLUDED.last_since, cursor = EXCLUDED.cursor, updated_at = now()`

func (p *PostgresRepository) GetStreamPosition(ctx context.Context, source model.Source, endpoint string) (repository.StreamPosition, bool, error) {
	var pos repository.StreamPosition
	err := p.pool.QueryRow(ctx, selectStreamPosition, source, endpoint).Scan(&pos.Cursor, &pos.GeneratedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return repository.StreamPosition{}, false, nil
	}
	if err != nil {
		return repository.StreamPosition{}, false, fmt.Errorf("get stream position %s/%s: %w", source, endpoint, err)
	}
	pos.GeneratedAt = pos.GeneratedAt.UTC()
	return pos, true, nil
}

func (p *PostgresRepository) SetStreamPosition(ctx context.Context, source model.Source, endpoint string, pos repository.StreamPosition) error {
	if _, err := p.fencedExecRetry(ctx, upsertStreamPosition, source, endpoint, pos.GeneratedAt.UTC(), nullable(pos.Cursor)); err != nil {
		return fmt.Errorf("set stream position %s/%s: %w", source, endpoint, err)
	}
	return nil
}
