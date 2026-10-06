package common

import (
	"errors"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const (
	HeaderCorrelationID = "X-Correlation-ID"
	HeaderInternalKey   = "X-Internal-Key"

	headerCorrelationID = HeaderCorrelationID
	headerInternalKey   = HeaderInternalKey
)

type localsKey int

const correlationIDKey localsKey = iota

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
	id, _ := c.Locals(correlationIDKey).(string)
	if id == "" {
		id = c.Get(HeaderCorrelationID)
	}
	return id
}

func correlationID(c fiber.Ctx) error {
	return CorrelationID(c)
}

func correlationIDFrom(c fiber.Ctx) string {
	return CorrelationIDFrom(c)
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

func requestLogger(log *slog.Logger, onError fiber.ErrorHandler) fiber.Handler {
	return RequestLogger(log, onError)
}

func ErrorHandler(log *slog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		var fe *fiber.Error
		if errors.As(err, &fe) && fe.Code < fiber.StatusInternalServerError {
			code := codeValidation
			if fe.Code == fiber.StatusNotFound {
				code = codeNotFound
			}
			return WriteError(c, fe.Code, code, fe.Message)
		}
		log.Error("unhandled_error",
			"correlation_id", CorrelationIDFrom(c),
			"error", err.Error(),
		)
		return WriteError(c, fiber.StatusInternalServerError, codeInternal, "internal error")
	}
}
