package main

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
)

const healthTimeout = 2 * time.Second

func newHealthApp(ping func(context.Context) error) *fiber.App {
	app := fiber.New()
	app.Get("/health", func(c fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), healthTimeout)
		defer cancel()

		status, httpStatus := "ok", fiber.StatusOK
		if err := ping(ctx); err != nil {
			status, httpStatus = "unavailable", fiber.StatusServiceUnavailable
		}
		return c.Status(httpStatus).JSON(fiber.Map{
			"status":  status,
			"service": "notifier",
			"checks":  fiber.Map{"kafka": status},
		})
	})
	return app
}
