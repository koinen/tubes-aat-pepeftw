package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/catalog"
	"tubes-aat-pepeftw/cmd/aggregator/config"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
)

func runLoadCatalog(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("load-catalog", flag.ContinueOnError)
	workers := fs.Int("workers", 16, "concurrent writers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workers < 1 {
		return errors.New("workers must be at least 1")
	}

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

	reader, err := catalog.NewReader(os.Stdin)
	if err != nil {
		return err
	}

	res, err := catalog.Loader{
		Hazards: postgres.New(pool),
		Workers: *workers,
		Log:     log,
		Now:     time.Now,
	}.Run(ctx, reader)
	log.Info("catalog_done", "rows", res.Rows, "created", res.Created, "unchanged", res.Unchanged,
		"rejected", res.Rejected, "failed", res.Failed, "duration_ms", res.Duration.Milliseconds())
	if err != nil {
		return err
	}
	if res.Failed > 0 {
		return errors.New("some catalog rows failed to save")
	}
	return nil
}
