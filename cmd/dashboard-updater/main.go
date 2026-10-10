package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
)

const shutdownTimeout = 5 * time.Second

func main() {
	log := newLogger("dashboard-updater")
	if err := run(log); err != nil {
		log.Error("dashboard_stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := newConsumerClient(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	recent := newRecentHazards(recentCapacity)
	live := newBroadcaster()

	app := newServer(ctx, recent, live, client.Ping)
	serverErr := make(chan error, 1)
	go func() {
		if err := app.Listen(":"+cfg.port, fiber.ListenConfig{DisableStartupMessage: true}); err != nil {
			serverErr <- err
			stop()
		}
	}()

	log.Info("dashboard_started", "topic", cfg.topic, "group", cfg.groupID, "port", cfg.port)
	consume(ctx, client, newUpdater(recent, live, log), log)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("shutdown_failed", "error", err.Error())
	}

	select {
	case err := <-serverErr:
		return err
	default:
		return nil
	}
}
