package common

import (
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const (
	HeaderCorrelationID = "X-Correlation-ID"
)

type localsKey string

const (
	correlationIDKey localsKey = "correlation_id"
	scopeKey         localsKey = "jwt_scope"
	subKey           localsKey = "jwt_sub"
)

func CorrelationID(c fiber.Ctx) error {
	id := c.Get(HeaderCorrelationID)
	if _, err := uuid.Parse(id); err != nil {
		id = uuid.NewString()
	}
	c.Locals(correlationIDKey, id)
	c.Set(HeaderCorrelationID, id)
	return c.Next()
}

func CorrelationIDFrom(c fiber.Ctx) string {
	if id, ok := c.Locals(correlationIDKey).(string); ok && id != "" {
		return id
	}
	id := c.Get(HeaderCorrelationID)
	if _, err := uuid.Parse(id); err == nil {
		return id
	}
	return uuid.NewString()
}

func RequestLogger(log *slog.Logger, onError fiber.ErrorHandler) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		if err := c.Next(); err != nil {
			if err := onError(c, err); err != nil {
				return err
			}
		}
		log.Info("http_request",
			"correlation_id", CorrelationIDFrom(c),
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}
}

func SetScope(c fiber.Ctx, scope string) {
	c.Locals(scopeKey, scope)
}

func GetScope(c fiber.Ctx) string {
	s, _ := c.Locals(scopeKey).(string)
	return s
}

func SetSub(c fiber.Ctx, sub string) {
	c.Locals(subKey, sub)
}

func GetSub(c fiber.Ctx) string {
	s, _ := c.Locals(subKey).(string)
	return s
}
