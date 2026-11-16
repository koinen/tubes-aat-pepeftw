package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"tubes-aat-pepeftw/cmd/auth/internal/common"
	"tubes-aat-pepeftw/cmd/auth/internal/config"
	"tubes-aat-pepeftw/cmd/auth/internal/health"
	"tubes-aat-pepeftw/cmd/auth/internal/kafka"
	"tubes-aat-pepeftw/cmd/auth/internal/redis"
	"tubes-aat-pepeftw/cmd/auth/internal/token"

	"github.com/gofiber/fiber/v3"
	recoverer "github.com/gofiber/fiber/v3/middleware/recover"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "auth")
	onError := common.ErrorHandler(logger)

	ctx := context.Background()

	app := fiber.New(fiber.Config{ErrorHandler: onError})
	app.Use(common.CorrelationID, common.RequestLogger(logger, onError), recoverer.New())

	redisClient, err := redis.Connect(ctx, config.Redis)
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	defer redisClient.Close()

	kafkaClient, err := kafka.InitKafka(ctx, config.Kafka)
	if err != nil {
		log.Fatal("Failed to connect to Kafka:", err)
	}
	defer kafkaClient.Close()

	// handlers
	healthHandler := health.NewHealthHandler()
	tokenHandler, err := token.NewTokenHandler(config.Token, redisClient, kafkaClient, logger)
	if err != nil {
		log.Fatal("Failed to init token handler:", err)
	}

	// routes
	app.Post("/token", tokenHandler.IssueToken)
	app.Get("/.well-known/jwks.json", tokenHandler.JWKS)

	app.Get("/health", healthHandler.HealthCheck)

	logger.Info("Starting Auth service", "port", config.Server.Port)
	if err := app.Listen(":" + config.Server.Port); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
