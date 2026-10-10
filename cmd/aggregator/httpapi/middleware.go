package httpapi

import (
	"crypto/subtle"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const (
	headerCorrelationID = "X-Correlation-ID"
	headerInternalKey   = "X-Internal-Key"
)

type localsKey int

const correlationIDKey localsKey = iota

func correlationID(c fiber.Ctx) error {
	id := c.Get(headerCorrelationID)
	if _, err := uuid.Parse(id); err != nil {
		id = uuid.NewString()
	}
	c.Locals(correlationIDKey, id)
	c.Set(headerCorrelationID, id)
	return c.Next()
}

func correlationIDFrom(c fiber.Ctx) string {
	id, _ := c.Locals(correlationIDKey).(string)
	return id
}

func requestLogger(log *slog.Logger, onError fiber.ErrorHandler) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		if err := c.Next(); err != nil {
			if err := onError(c, err); err != nil {
				return err
			}
		}
		log.Info("http_request",
			"correlation_id", correlationIDFrom(c),
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}
}

func requireInternalKey(key string) fiber.Handler {
	want := []byte(key)
	return func(c fiber.Ctx) error {
		got := []byte(c.Get(headerInternalKey))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			return writeError(c, fiber.StatusUnauthorized, codeUnauthorized, "missing or invalid X-Internal-Key")
		}
		return c.Next()
	}
}
