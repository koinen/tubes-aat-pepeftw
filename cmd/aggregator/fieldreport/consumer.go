package fieldreport

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/internal/telemetry"
)

// GroupID is the consumer group every aggregator pod joins for topic field-reports.
const GroupID = "aggregator-field-reports"

const (
	pollLimit    = 100
	commitOnStop = 5 * time.Second
)

var stored = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bnpb_field_reports_stored_total",
	Help: "Laporan lapangan yang dibaca dari Kafka: inserted (baru disimpan) atau duplicate (sudah ada, body sama).",
}, []string{"result"})

// Consumer reads topic field-reports into a Handler.
type Consumer struct {
	client  *kgo.Client
	handler *Handler
	log     *slog.Logger
}

// NewConsumer joins GroupID on topic. Offsets are committed only for records the handler finished:
// after each poll, and before a partition is taken away in a rebalance.
func NewConsumer(brokers []string, topic string, handler *Handler, log *slog.Logger) (*Consumer, error) {
	c := &Consumer{handler: handler, log: log}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(GroupID),
		kgo.ConsumeTopics(topic),
		// Commit only what was marked, and mark a record only once it is stored or parked.
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				log.Warn("field_report_commit_failed", "on", "revoke", "error", err.Error())
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	c.client = cl
	return c, nil
}

// Run consumes until ctx ends, then commits what is finished and leaves the group.
func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()
	defer func() {
		// ctx is over; give the last commit its own short deadline.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitOnStop)
		defer cancel()
		if err := c.client.CommitMarkedOffsets(commitCtx); err != nil {
			c.log.Warn("field_report_commit_failed", "on", "stop", "error", err.Error())
		}
	}()
	c.log.Info("field_report_consumer_started", "group", GroupID)
	for {
		fetches := c.client.PollRecords(ctx, pollLimit)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("kafka_fetch_error", "topic", topic, "partition", partition, "error", err.Error())
		})
		for _, rec := range fetches.Records() {
			recCtx, done := telemetry.Consume(ctx, rec)
			err := c.handler.Handle(recCtx, Record{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Value: rec.Value})
			done(err)
			if err != nil {
				return // ctx ended; the record stays uncommitted and is read again
			}
			c.client.MarkCommitRecords(rec)
		}
		if err := c.client.CommitMarkedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("field_report_commit_failed", "on", "poll", "error", err.Error())
		}
	}
}
