package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/config"
	"tubes-aat-pepeftw/cmd/aggregator/httpapi"
	"tubes-aat-pepeftw/cmd/aggregator/kafka"
	"tubes-aat-pepeftw/cmd/aggregator/logging"
	"tubes-aat-pepeftw/cmd/aggregator/poller"
	"tubes-aat-pepeftw/cmd/aggregator/relay"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
)

const (
	dbWaitTimeout  = 30 * time.Second
	dbPingTimeout  = 2 * time.Second
	relayBatchSize = 100
)

func main() {
	log := logging.New("aggregator")
	if err := run(log); err != nil {
		log.Error("aggregator_stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pollerCfg, err := poller.LoadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := waitForDB(ctx, pool, log); err != nil {
		return err
	}
	log.Info("database_connected")

	repo := postgres.New(pool)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.HazardTopic)
	if err != nil {
		return err
	}
	defer producer.Close()

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.New(repo, producer, log, cfg.RelayInterval, relayBatchSize).Run(ctx)
	}()
	defer func() {
		stop()
		<-relayDone
	}()

	if pollerCfg.Enabled {
		pollerDone := make(chan struct{})
		go func() {
			defer close(pollerDone)
			poller.New(pollerCfg, poller.Deps{
				Hazards:   repo,
				Rejected:  repo,
				Cursors:   repo,
				Statuses:  repo,
				Volcanoes: repo,
				Log:       log,
			}).Run(ctx)
		}()
		defer func() {
			stop()
			<-pollerDone
		}()
		log.Info("poller_started")
	}

	app := httpapi.New(httpapi.Deps{
		Hazards:      repo,
		Statuses:     repo,
		PostgresPing: pool.Ping,
		KafkaPing:    producer.Ping,
		InternalKey:  cfg.InternalAPIKey,
		Log:          log,
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(shutdownCtx); err != nil {
			log.Error("shutdown_failed", "error", err.Error())
		}
	}()

	log.Info("aggregator_started", "port", cfg.Port)
	return app.Listen(":"+cfg.Port, fiber.ListenConfig{DisableStartupMessage: true})
}

func waitForDB(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	deadline := time.Now().Add(dbWaitTimeout)
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("database unreachable after %s: %w", dbWaitTimeout, err)
		}
		log.Warn("database_unavailable", "attempt", attempt, "error", err.Error())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
