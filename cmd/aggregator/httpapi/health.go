package httpapi

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
)

const healthTimeout = 2 * time.Second

func health(ping func(context.Context) error) fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), healthTimeout)
		defer cancel()

		status, httpStatus := "ok", fiber.StatusOK
		if err := ping(ctx); err != nil {
			status, httpStatus = "unavailable", fiber.StatusServiceUnavailable
		}
		return c.Status(httpStatus).JSON(fiber.Map{
			"status":  status,
			"service": "aggregator",
			"checks":  fiber.Map{"postgres": status},
		})
	}
}
