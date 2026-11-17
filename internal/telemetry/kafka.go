package telemetry

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const traceParentKey = "traceparent"

func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get(traceParentKey)
}

func FromTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{traceParentKey: traceParent})
}

func Produce(ctx context.Context, rec *kgo.Record) (context.Context, trace.Span) {
	ctx, span := tracer().Start(ctx, "publish "+rec.Topic, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.destination", rec.Topic)))
	if tp := TraceParent(ctx); tp != "" {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: traceParentKey, Value: []byte(tp)})
	}
	return ctx, span
}

func Produced(span trace.Span, topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
		span.SetStatus(codes.Error, err.Error())
	}
	kafkaMessages.WithLabelValues(serviceName, topic, "produce", result).Inc()
	span.End()
}

func Consume(ctx context.Context, rec *kgo.Record) (context.Context, func(err error)) {
	parent := ctx
	for _, h := range rec.Headers {
		if h.Key == traceParentKey {
			parent = FromTraceParent(ctx, string(h.Value))
		}
	}
	ctx, span := tracer().Start(parent, "consume "+rec.Topic, trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.source", rec.Topic)))
	start := time.Now()
	return ctx, func(err error) {
		result := "ok"
		if err != nil {
			result = "error"
			span.SetStatus(codes.Error, err.Error())
		}
		kafkaMessages.WithLabelValues(serviceName, rec.Topic, "consume", result).Inc()
		kafkaDuration.WithLabelValues(serviceName, rec.Topic).Observe(time.Since(start).Seconds())
		span.End()
	}
}
