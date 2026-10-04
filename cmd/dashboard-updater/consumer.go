package main

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

func newConsumerClient(cfg config) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.groupID),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
}

func consume(ctx context.Context, client *kgo.Client, u *updater, log *slog.Logger) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Error("kafka_fetch_failed", "correlation_id", "", "topic", topic, "partition", partition, "error", err.Error())
		})
		fetches.EachRecord(func(r *kgo.Record) {
			u.handle(r.Value, header(r, "correlation_id"))
		})
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
