package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, pool *pgxpool.Pool, files fs.FS, log *slog.Logger) error {
	if err := ensureLedger(ctx, pool); err != nil {
		return err
	}

	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return err
	}

	versions, err := listVersions(files)
	if err != nil {
		return err
	}

	for _, version := range versions {
		if applied[version] {
			log.Info("migration_skipped", "version", version)
			continue
		}
		if err := apply(ctx, pool, files, version); err != nil {
			return err
		}
		if err := markApplied(ctx, pool, version); err != nil {
			return err
		}
		log.Info("migration_applied", "version", version)
	}
	return nil
}

func listVersions(files fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	var versions []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			versions = append(versions, e.Name())
		}
	}
	sort.Strings(versions)
	return versions, nil
}

func apply(ctx context.Context, pool *pgxpool.Pool, files fs.FS, version string) error {
	script, err := fs.ReadFile(files, version)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}

	for i, stmt := range splitStatements(string(script)) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration %s statement %d: %w", version, i+1, err)
		}
	}
	return nil
}
