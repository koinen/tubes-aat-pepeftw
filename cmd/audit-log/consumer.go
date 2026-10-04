package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const commitTimeout = 5 * time.Second

func newConsumerClient(cfg config) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.groupID),
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
}

func consume(ctx context.Context, client *kgo.Client, a *auditor, log *slog.Logger) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			client.AllowRebalance()
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Error("kafka_fetch_failed", "correlation_id", "", "topic", topic, "partition", partition, "error", err.Error())
		})
		fetches.EachRecord(func(r *kgo.Record) {
			a.handle(r.Value, header(r, "correlation_id"), r.Partition, r.Offset)
		})

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		if err := client.CommitUncommittedOffsets(commitCtx); err != nil {
			log.Error("kafka_commit_failed", "correlation_id", "", "error", err.Error())
		}
		cancel()
		client.AllowRebalance()
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
