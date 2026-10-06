package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/common"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
	"tubes-aat-pepeftw/cmd/client-api/internal/hazard"
	"tubes-aat-pepeftw/cmd/client-api/internal/health"
	"tubes-aat-pepeftw/cmd/client-api/internal/kafka"
	"tubes-aat-pepeftw/cmd/client-api/internal/ratelimit"
	redisConn "tubes-aat-pepeftw/cmd/client-api/internal/redis"
	"tubes-aat-pepeftw/cmd/client-api/internal/source"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	ctx := context.Background()

	// --- 1. Redis Connection (Rate Limiting) ---
	redisClient, err := redisConn.Connect(ctx, cfg.Redis)
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	defer redisClient.Close()

	// --- 2. Upstream Aggregator Client ---
	aggClient := aggregator.NewClient(cfg.Aggregator)

	// --- 3. Fetch JWKS from Auth Service on Startup (with brief startup retry) ---
	var jwks *auth.JWKS
	for attempts := 0; attempts < 5; attempts++ {
		jwks, err = auth.FetchJWKS(cfg.Auth.JWKSURL)
		if err == nil {
			break
		}
		logger.Warn("waiting for JWKS from auth service...", "attempt", attempts+1, "error", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("Failed to fetch JWKS on startup:", err)
	}
	logger.Info("JWKS loaded successfully", "kid", jwks.Kid)

	// --- 4. In-Memory Denylist and Kafka Consumer ---
	denylist := auth.NewDenylist()

	kafkaClient, err := kafka.StartConsumer(ctx, cfg.Kafka, denylist, logger)
	if err != nil {
		log.Fatal("Failed to start Kafka consumer:", err)
	}
	defer kafkaClient.Close()

	// --- 5. Fiber Application Setup ---
	onError := errorHandler(logger)
	app := fiber.New(fiber.Config{ErrorHandler: onError})

	app.Use(common.CorrelationID, common.RequestLogger(logger, onError), recoverer.New())

	// Health Check (Public, no auth, no rate limiting)
	healthHandler := health.NewHandler(aggClient, true)
	app.Get("/health", healthHandler.HealthCheck)

	// Authentication & Rate Limiting Middlewares
	verifier := auth.NewVerifier(jwks, cfg.Auth.JWTIssuer, cfg.Auth.JWTAudience, denylist)
	limiter := ratelimit.New(redisClient, cfg.RateLimit)

	// Protected Routes
	api := app.Group("", verifier.Middleware, limiter.Middleware)

	hazardHandler := hazard.NewHandler(aggClient)
	sourceHandler := source.NewHandler(aggClient)

	api.Get("/hazards", hazardHandler.ListHazards)
	api.Get("/hazards/:hazard_id", hazardHandler.GetHazard)
	api.Get("/sources/status", sourceHandler.GetSourcesStatus)

	logger.Info("Starting Client API service", "port", cfg.Server.Port)
	if err := app.Listen(":" + cfg.Server.Port); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

func errorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		var fe *fiber.Error
		if errors.As(err, &fe) && fe.Code < fiber.StatusInternalServerError {
			code := common.CodeValidation
			if fe.Code == fiber.StatusNotFound {
				code = common.CodeNotFound
			}
			return common.WriteError(c, fe.Code, code, fe.Message)
		}
		log.Error("unhandled_error",
			"correlation_id", common.CorrelationIDFrom(c),
			"error", err.Error(),
		)
		return common.WriteError(c, fiber.StatusInternalServerError, common.CodeInternal, "internal error")
	}
}
