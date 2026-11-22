package migrate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	createLedger = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

	selectApplied = `SELECT version FROM schema_migrations`

	recordApplied = `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`
)

func ensureLedger(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, createLedger); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, selectApplied)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}

	applied := make(map[string]bool, len(versions))
	for _, v := range versions {
		applied[v] = true
	}
	return applied, nil
}

func markApplied(ctx context.Context, pool *pgxpool.Pool, version string) error {
	if _, err := pool.Exec(ctx, recordApplied, version); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}
	return nil
}
