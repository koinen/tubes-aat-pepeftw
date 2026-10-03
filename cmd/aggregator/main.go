package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/aggregator/config"
	"tubes-aat-pepeftw/cmd/aggregator/fixture"
	"tubes-aat-pepeftw/cmd/aggregator/httpapi"
	"tubes-aat-pepeftw/cmd/aggregator/logging"
	"tubes-aat-pepeftw/cmd/aggregator/repository/memory"
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

	repo := memory.New()
	if cfg.SeedFixtures {
		if err := fixture.Run(context.Background(), repo, repo, time.Now().UTC().Truncate(time.Second)); err != nil {
			return err
		}
		log.Info("fixtures_seeded")
	}

	app := httpapi.New(httpapi.Deps{
		Hazards:     repo,
		Statuses:    repo,
		InternalKey: cfg.InternalAPIKey,
		Log:         log,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
