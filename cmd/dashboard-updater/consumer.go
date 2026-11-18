package main

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/internal/poison"
	"tubes-aat-pepeftw/internal/telemetry"
)

func newConsumerClient(cfg config) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.groupID),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// The same client forwards unprocessed values, which may be up to the source topic's ~1 MiB.
		kgo.ProducerBatchMaxBytes(poison.MaxForwardBytes),
	)
}

// consume never commits: on every start the updater reads hazard-events from the beginning to
// rebuild its recent list. A value it cannot read is forwarded again then; the aggregator keeps
// one row per record.
func consume(ctx context.Context, client *kgo.Client, u *updater, fwd *poison.Forwarder, log *slog.Logger) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Error("kafka_fetch_failed", "correlation_id", "", "topic", topic, "partition", partition, "error", err.Error())
		})
		for _, r := range fetches.Records() {
			_, done := telemetry.Consume(ctx, r)
			err := handleRecord(ctx, r, u, fwd)
			done(err)
			if err != nil {
				return // ctx ended before the value reached topic unprocessed
			}
		}
	}
}

// handleRecord shows r on the dashboard, or forwards a value it cannot read to topic unprocessed.
// It returns an error only when ctx ends before the forward succeeds.
func handleRecord(ctx context.Context, r *kgo.Record, u *updater, fwd *poison.Forwarder) error {
	msg, err := decode(r.Value)
	if err != nil {
		return fwd.Forward(ctx, r, err)
	}
	u.handle(msg, header(r, "correlation_id"))
	return nil
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
