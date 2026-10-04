package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

const (
	headerCorrelationID = "X-Correlation-ID"
	localsCorrelationID = "correlation_id"
)

func newLogger(service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.String("ts", a.Value.Time().UTC().Format(time.RFC3339))
			}
			return a
		},
	})
	return slog.New(correlationHandler{h}).With("service", service)
}

type correlationHandler struct{ slog.Handler }

func (h correlationHandler) Handle(ctx context.Context, r slog.Record) error {
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "correlation_id" {
			found = true
			return false
		}
		return true
	})
	if !found {
		r.AddAttrs(slog.String("correlation_id", ""))
	}
	return h.Handler.Handle(ctx, r)
}

func (h correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return correlationHandler{h.Handler.WithAttrs(attrs)}
}

func (h correlationHandler) WithGroup(name string) slog.Handler {
	return correlationHandler{h.Handler.WithGroup(name)}
}

func correlationMiddleware(log *slog.Logger) fiber.Handler {
	return func(c fiber.Ctx) error {
		cid := c.Get(headerCorrelationID)
		if _, err := uuid.Parse(cid); err != nil {
			cid = uuid.NewString()
		}
		c.Locals(localsCorrelationID, cid)
		c.Set(headerCorrelationID, cid)

		start := time.Now()
		err := c.Next()
		if err != nil {
			if hErr := c.App().ErrorHandler(c, err); hErr != nil {
				_ = c.SendStatus(fiber.StatusInternalServerError)
			}
		}
		log.Info("http_request",
			"correlation_id", cid,
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}
}

func correlationID(c fiber.Ctx) string {
	if v, ok := c.Locals(localsCorrelationID).(string); ok {
		return v
	}
	return ""
}

func errorBody(c fiber.Ctx, code, message string) fiber.Map {
	return fiber.Map{"error": fiber.Map{
		"code":           code,
		"message":        message,
		"correlation_id": correlationID(c),
	}}
}

func sendError(c fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(errorBody(c, code, message))
}