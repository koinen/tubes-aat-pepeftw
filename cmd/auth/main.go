package main

import (
	"context"
	"log"
	"tubes-aat-pepeftw/cmd/auth/internal/config"
	"tubes-aat-pepeftw/cmd/auth/internal/health"
	"tubes-aat-pepeftw/cmd/auth/internal/redis"

	"github.com/gofiber/fiber/v3"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	ctx := context.Background()

	app := fiber.New()

	redisClient, err := redis.Connect(ctx, config.Redis)
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	defer redisClient.Close()

	// handlers
	healthHandler := health.NewHealthHandler()

	app.Get("/health", healthHandler.HealthCheck)

	app.Listen(":" + config.Server.Port)
}
