package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/internal/telemetry"
)

func main() {
	log := newLogger("analytics-dashboard")
	cfg, err := loadConfig()
	if err != nil {
		log.Error("config_invalid", "error", err.Error())
		os.Exit(1)
	}

	app := newServer(newAggregatorClient(cfg), log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := telemetry.Init(ctx, "analytics-dashboard")
	if err != nil {
		log.Error("tracing_init_failed", "error", err.Error())
		os.Exit(1)
	}
	defer shutdownTracing(context.Background())
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = app.ShutdownWithContext(shutdownCtx)
	}()

	log.Info("analytics_dashboard_started", "port", cfg.port)
	if err := app.Listen(":"+cfg.port, fiber.ListenConfig{DisableStartupMessage: true}); err != nil {
		log.Error("analytics_dashboard_stopped", "error", err.Error())
		os.Exit(1)
	}
}
