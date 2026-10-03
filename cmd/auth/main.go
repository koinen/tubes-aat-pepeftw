package main

import (
	"log"
	"tubes-aat-pepeftw/cmd/auth/internal/config"
	"tubes-aat-pepeftw/cmd/auth/internal/health"

	"github.com/gofiber/fiber/v3"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	app := fiber.New()

	// handlers
	healthHandler := health.NewHealthHandler()

	app.Get("/health", healthHandler.HealthCheck)

	app.Listen(":" + config.Server.Port)
}
