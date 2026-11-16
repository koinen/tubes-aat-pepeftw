package main

import (
	"context"
	_ "embed"
	"log/slog"
	"net/url"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const healthTimeout = 2 * time.Second

//go:embed web/index.html
var indexHTML []byte

var forwardedParams = []string{"from", "to", "bucket"}

func newServer(agg *aggregatorClient, log *slog.Logger) *fiber.App {
	app := fiber.New()

	app.Get("/", func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.Send(indexHTML)
	})

	app.Get("/api/analytics", func(c fiber.Ctx) error {
		correlationID := c.Get(headerCorrelationID)
		if _, err := uuid.Parse(correlationID); err != nil {
			correlationID = uuid.NewString()
		}
		c.Set(headerCorrelationID, correlationID)

		query := url.Values{}
		for _, key := range forwardedParams {
			if v := c.Query(key); v != "" {
				query.Set(key, v)
			}
		}

		start := time.Now()
		status, body, err := agg.analytics(c.Context(), query, correlationID)
		if err != nil {
			log.Warn("aggregator_unreachable", "correlation_id", correlationID, "error", err.Error())
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": fiber.Map{"code": "UPSTREAM_UNAVAILABLE", "message": "aggregator tidak terjangkau", "correlation_id": correlationID},
			})
		}
		log.Info("analytics_served", "correlation_id", correlationID, "status", status, "latency_ms", time.Since(start).Milliseconds())
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		return c.Status(status).Send(body)
	})

	app.Get("/health", func(c fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), healthTimeout)
		defer cancel()
		status, httpStatus := "ok", fiber.StatusOK
		if err := agg.health(ctx); err != nil {
			status, httpStatus = "unavailable", fiber.StatusServiceUnavailable
		}
		return c.Status(httpStatus).JSON(fiber.Map{
			"status":  status,
			"service": "analytics-dashboard",
			"checks":  fiber.Map{"aggregator": status},
		})
	})

	return app
}
