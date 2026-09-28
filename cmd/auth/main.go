package main

import (
	"log"

	"github.com/gofiber/fiber/v3"
)

func main() {
	app := fiber.New()

	app.Get("/", func(c fiber.Ctx) {
		c.Send([]byte("Hello, World!"))
	})

	log.Printf("Starting Aggregator Service...")
	app.Listen(":8080")
}
