package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

func StartConsumer(
	ctx context.Context,
	cfg config.KafkaConfig,
	denylist *auth.Denylist,
	log *slog.Logger,
) (*kgo.Client, error) {
	brokers := strings.Split(cfg.Brokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}

	go func() {
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					log.Warn("kafka_fetch_error", "topic", err.Topic, "partition", err.Partition, "error", err.Err)
				}
			}
			fetches.EachRecord(func(r *kgo.Record) {
				jti := string(r.Value)
				if jti != "" {
					denylist.Add(jti)
					log.Info("token_revoked", "jti", jti, "topic", r.Topic, "offset", r.Offset)
				}
			})
		}
	}()

	return cl, nil
}
