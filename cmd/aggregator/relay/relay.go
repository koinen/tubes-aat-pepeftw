package relay

import (
	"context"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	publishTimeout = 10 * time.Second
	markTimeout    = 5 * time.Second
)

type Publisher interface {
	Publish(ctx context.Context, records []model.OutboxRecord) []error
	Topic() string
}

type Relay struct {
	store     repository.OutboxRepository
	publisher Publisher
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

func New(store repository.OutboxRepository, publisher Publisher, log *slog.Logger, interval time.Duration, batchSize int) *Relay {
	return &Relay{store: store, publisher: publisher, log: log, interval: interval, batchSize: batchSize}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		for r.tick(ctx) == r.batchSize && ctx.Err() == nil {
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) tick(ctx context.Context) int {
	records, err := r.store.PendingOutbox(ctx, r.batchSize)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Error("outbox_fetch_failed", "correlation_id", "", "error", err.Error())
		}
		return 0
	}
	if len(records) == 0 {
		return 0
	}

	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	start := time.Now()
	errs := r.publisher.Publish(publishCtx, records)
	cancel()
	latency := time.Since(start).Milliseconds()

	published := make([]int64, 0, len(records))
	for i, rec := range records {
		attrs := []any{
			"correlation_id", rec.CorrelationID, "target", "kafka", "method", "PRODUCE",
			"path", r.publisher.Topic(), "latency_ms", latency, "hazard_id", rec.HazardID, "event_id", rec.EventID,
		}
		if errs[i] != nil {
			r.log.Error("kafka_publish_failed", append(attrs, "status", "error", "error", errs[i].Error())...)
			continue
		}
		r.log.Info("kafka_published", append(attrs, "status", "ok")...)
		published = append(published, rec.ID)
	}

	markCtx, cancelMark := context.WithTimeout(context.WithoutCancel(ctx), markTimeout)
	defer cancelMark()
	if err := r.store.MarkPublished(markCtx, published); err != nil {
		r.log.Error("outbox_mark_failed", "correlation_id", "", "error", err.Error())
		return 0
	}
	return len(published)
}
