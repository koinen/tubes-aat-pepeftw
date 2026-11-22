package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const upsertSourceStatus = `
INSERT INTO source_status (source, status, breaker_state, last_success_at, stale_since, last_error, station_silence)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (source) DO UPDATE SET
    status          = EXCLUDED.status,
    breaker_state   = EXCLUDED.breaker_state,
    last_success_at = EXCLUDED.last_success_at,
    stale_since     = EXCLUDED.stale_since,
    last_error      = EXCLUDED.last_error,
    station_silence = EXCLUDED.station_silence`

const selectSourceStatuses = `
SELECT source, status, breaker_state, last_success_at, stale_since, COALESCE(last_error, ''), station_silence
FROM source_status
ORDER BY source`

func (p *PostgresRepository) SaveSourceStatus(ctx context.Context, s model.SourceStatus) error {
	_, err := p.fencedExecRetry(ctx, upsertSourceStatus,
		s.Source, s.Status, s.BreakerState, utc(s.LastSuccessAt), utc(s.StaleSince), nullable(s.LastError), s.StationSilence)
	if err != nil {
		return fmt.Errorf("save source status %s: %w", s.Source, err)
	}
	return nil
}

func (p *PostgresRepository) ListSourceStatus(ctx context.Context) ([]model.SourceStatus, error) {
	rows, err := p.pool.Query(ctx, selectSourceStatuses)
	if err != nil {
		return nil, fmt.Errorf("list source status: %w", err)
	}
	statuses, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.SourceStatus, error) {
		var s model.SourceStatus
		err := row.Scan(&s.Source, &s.Status, &s.BreakerState, &s.LastSuccessAt, &s.StaleSince, &s.LastError, &s.StationSilence)
		s.LastSuccessAt, s.StaleSince = utc(s.LastSuccessAt), utc(s.StaleSince)
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("list source status: %w", err)
	}
	return statuses, nil
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
