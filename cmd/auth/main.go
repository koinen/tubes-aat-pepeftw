package main

import (
	"log"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/gofiber/fiber/v3"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	app := fiber.New()

	app.Get("/", func(c fiber.Ctx) {
		c.Send([]byte("Hello, World!"))
	})

	log.Printf("Starting Auth Service...")
	app.Listen(":" + config.Server.Port)
}
