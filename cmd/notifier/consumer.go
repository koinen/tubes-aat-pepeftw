package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/internal/poison"
	"tubes-aat-pepeftw/internal/telemetry"
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
		// The same client forwards unprocessed values, which may be up to the source topic's ~1 MiB.
		kgo.ProducerBatchMaxBytes(poison.MaxForwardBytes),
	)
}

func consume(ctx context.Context, client *kgo.Client, n *notifier, fwd *poison.Forwarder, log *slog.Logger) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			client.AllowRebalance()
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Error("kafka_fetch_failed", "correlation_id", "", "topic", topic, "partition", partition, "error", err.Error())
		})
		for _, r := range fetches.Records() {
			_, done := telemetry.Consume(ctx, r)
			err := handleRecord(ctx, r, n, fwd)
			done(err)
			if err != nil {
				// ctx ended before the value reached topic unprocessed: commit nothing, read it again.
				client.AllowRebalance()
				return
			}
		}

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		if err := client.CommitUncommittedOffsets(commitCtx); err != nil {
			log.Error("kafka_commit_failed", "correlation_id", "", "error", err.Error())
		}
		cancel()
		client.AllowRebalance()
	}
}

// handleRecord notifies about r, or forwards a value it cannot read to topic unprocessed. It
// returns an error only when ctx ends before the forward succeeds.
func handleRecord(ctx context.Context, r *kgo.Record, n *notifier, fwd *poison.Forwarder) error {
	msg, err := decode(r.Value)
	if err != nil {
		return fwd.Forward(ctx, r, err)
	}
	n.handle(msg, header(r, "correlation_id"))
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
