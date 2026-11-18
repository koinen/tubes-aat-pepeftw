package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/internal/telemetry"
)

const (
	pollLimit    = 100
	commitOnStop = 5 * time.Second
)

// HandleFunc finishes one record and returns nil once its offset may be committed. It returns an
// error only when ctx ends first; the record then stays uncommitted and is read again.
type HandleFunc func(ctx context.Context, r *kgo.Record) error

// Consumer reads one topic in a consumer group into a HandleFunc, one record at a time.
type Consumer struct {
	client *kgo.Client
	handle HandleFunc
	group  string
	log    *slog.Logger
}

// NewConsumer joins group on topic. Offsets are committed only for records handle finished: after
// each poll, and before a partition is taken away in a rebalance.
func NewConsumer(brokers []string, group, topic string, handle HandleFunc, log *slog.Logger) (*Consumer, error) {
	log = log.With("group", group, "topic", topic)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Commit only what was marked, and mark a record only once it is handled.
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				log.Warn("kafka_commit_failed", "on", "revoke", "error", err.Error())
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: cl, handle: handle, group: group, log: log}, nil
}

// Run consumes until ctx ends, then commits what is finished and leaves the group.
func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()
	defer func() {
		// ctx is over; give the last commit its own short deadline.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitOnStop)
		defer cancel()
		if err := c.client.CommitMarkedOffsets(commitCtx); err != nil {
			c.log.Warn("kafka_commit_failed", "on", "stop", "error", err.Error())
		}
	}()
	c.log.Info("kafka_consumer_started")
	for {
		fetches := c.client.PollRecords(ctx, pollLimit)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("kafka_fetch_error", "partition", partition, "error", err.Error())
		})
		for _, rec := range fetches.Records() {
			recCtx, done := telemetry.Consume(ctx, rec)
			err := c.handle(recCtx, rec)
			done(err)
			if err != nil {
				return // ctx ended; the record stays uncommitted and is read again
			}
			c.client.MarkCommitRecords(rec)
		}
		if err := c.client.CommitMarkedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("kafka_commit_failed", "on", "poll", "error", err.Error())
		}
	}
}
