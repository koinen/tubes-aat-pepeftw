package httpapi

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
)

const healthTimeout = 2 * time.Second

type pingFunc func(context.Context) error

func health(postgres, kafka pingFunc) fiber.Handler {
	return func(c fiber.Ctx) error {
		postgresStatus, kafkaStatus := checkStatus(c.Context(), postgres), checkStatus(c.Context(), kafka)

		status, httpStatus := "ok", fiber.StatusOK
		if postgresStatus != "ok" {
			status, httpStatus = "unavailable", fiber.StatusServiceUnavailable
		}
		return c.Status(httpStatus).JSON(fiber.Map{
			"status":  status,
			"service": "aggregator",
			"checks":  fiber.Map{"postgres": postgresStatus, "kafka": kafkaStatus},
		})
	}
}

func checkStatus(parent context.Context, ping pingFunc) string {
	ctx, cancel := context.WithTimeout(parent, healthTimeout)
	defer cancel()
	if err := ping(ctx); err != nil {
		return "unavailable"
	}
	return "ok"
}
