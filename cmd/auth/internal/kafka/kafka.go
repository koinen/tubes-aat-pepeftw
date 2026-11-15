package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/twmb/franz-go/pkg/kgo"
)

const deliveryTimeout = 5 * time.Second

func InitKafka(ctx context.Context, cfg config.KafkaConfig) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitBrokers(cfg.Brokers)...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
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

func splitBrokers(raw string) []string {
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}
