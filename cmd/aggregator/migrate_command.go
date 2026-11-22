package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/config"
	"tubes-aat-pepeftw/cmd/aggregator/migrate"
	"tubes-aat-pepeftw/cmd/aggregator/migrations"
)

func runMigrate(log *slog.Logger) error {
	dbURL, err := config.DatabaseURL()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := waitForDB(ctx, pool, log); err != nil {
		return err
	}

	if err := migrate.Run(ctx, pool, migrations.Files, log); err != nil {
		return err
	}
	log.Info("migrate_done")
	return nil
}
