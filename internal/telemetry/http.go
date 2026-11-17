package telemetry

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var untraced = map[string]bool{"/health": true, "/metrics": true, "/events": true}

type fiberCarrier struct{ c fiber.Ctx }

func (f fiberCarrier) Get(key string) string { return f.c.Get(key) }
func (f fiberCarrier) Set(key, value string) { f.c.Set(key, value) }
func (f fiberCarrier) Keys() []string        { return nil }

func Middleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		if untraced[c.Path()] {
			return c.Next()
		}
		start := time.Now()
		ctx := propagator.Extract(c.Context(), fiberCarrier{c})
		ctx, span := tracer().Start(ctx, c.Method()+" "+c.Path(), trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		c.SetContext(ctx)

		err := c.Next()

		status := c.Response().StatusCode()
		var fe *fiber.Error
		if errors.As(err, &fe) {
			status = fe.Code
		} else if err != nil && status < http.StatusBadRequest {
			status = http.StatusInternalServerError
		}
		route := c.Route().Path
		span.SetName(c.Method() + " " + route)
		span.SetAttributes(
			attribute.String("http.route", route),
			attribute.Int("http.status_code", status),
			attribute.String("correlation_id", c.GetRespHeader("X-Correlation-ID")),
		)
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		httpRequests.WithLabelValues(serviceName, c.Method(), route, strconv.Itoa(status)).Inc()
		httpDuration.WithLabelValues(serviceName, c.Method(), route).Observe(time.Since(start).Seconds())
		return err
	}
}

func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}
