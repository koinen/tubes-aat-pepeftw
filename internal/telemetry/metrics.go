package telemetry

import (
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var serviceName = "unknown"

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bnpb_http_requests_total",
		Help: "HTTP requests handled, by route and status.",
	}, []string{"service", "method", "route", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bnpb_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"service", "method", "route"})

	kafkaMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bnpb_kafka_messages_total",
		Help: "Kafka messages produced or consumed, by result.",
	}, []string{"service", "topic", "direction", "result"})

	kafkaDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bnpb_kafka_handle_duration_seconds",
		Help:    "Time to handle one consumed Kafka message.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"service", "topic"})
)

func MetricsHandler() fiber.Handler {
	return adaptor.HTTPHandler(promhttp.Handler())
}
