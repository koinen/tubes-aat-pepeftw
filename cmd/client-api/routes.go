package main

import (
	"log/slog"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/common"
	"tubes-aat-pepeftw/cmd/client-api/internal/hazard"
	"tubes-aat-pepeftw/cmd/client-api/internal/health"
	"tubes-aat-pepeftw/cmd/client-api/internal/ops"
	"tubes-aat-pepeftw/cmd/client-api/internal/source"

	"tubes-aat-pepeftw/internal/telemetry"
)

type appDeps struct {
	Agg *aggregator.Client
	// Protect runs before every protected route, in order: concurrency cap, JWT verifier, rate limit.
	Protect []any
	Log     *slog.Logger
}

func newApp(d appDeps) *fiber.App {
	onError := errorHandler(d.Log)
	app := fiber.New(fiber.Config{ErrorHandler: onError})

	app.Use(telemetry.Middleware(), common.CorrelationID, common.RequestLogger(d.Log, onError), recoverer.New())

	// Health Check (Public, no auth, no rate limiting)
	healthHandler := health.NewHandler(d.Agg, true)
	app.Get("/health", healthHandler.HealthCheck)
	app.Get("/metrics", telemetry.MetricsHandler())

	// Protected Routes
	api := app.Group("", d.Protect...)

	hazardHandler := hazard.NewHandler(d.Agg)
	sourceHandler := source.NewHandler(d.Agg)
	opsHandler := ops.NewHandler(d.Agg, d.Log)

	api.Get("/hazards", hazardHandler.ListHazards)
	api.Get("/hazards/:hazard_id", hazardHandler.GetHazard)
	api.Get("/sources/status", sourceHandler.GetSourcesStatus)
	read := auth.RequireScope(ops.ScopeReadUnprocessed)
	replay := auth.RequireScope(ops.ScopeReplayUnprocessed)
	api.Get("/unprocessed", read, opsHandler.Configured, opsHandler.ListUnprocessed)
	api.Post("/unprocessed/replay", replay, opsHandler.Configured, opsHandler.RequestReplay)
	api.Get("/unprocessed/replay/:job_id", replay, opsHandler.Configured, opsHandler.GetReplayJob)

	return app
}
