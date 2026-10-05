package kafka

import (
	"context"
	"fmt"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/twmb/franz-go/pkg/kgo"
)

func InitKafka(ctx context.Context, cfg config.KafkaConfig) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers),
		kgo.ConsumeTopics(cfg.Topic),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}

	return cl, nil
}
