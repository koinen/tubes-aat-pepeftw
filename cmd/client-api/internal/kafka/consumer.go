package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

// startupPing is how long StartConsumer waits to learn whether Kafka is reachable.
const startupPing = 5 * time.Second

// StartConsumer follows topic auth-revocations into denylist. Kafka being down does not stop
// client-api from starting: the client keeps reconnecting, and until it does, revocations
// published meanwhile are applied late.
func StartConsumer(
	ctx context.Context,
	cfg config.KafkaConfig,
	denylist *auth.Denylist,
	log *slog.Logger,
) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitBrokers(cfg.Brokers)...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, startupPing)
	if err := cl.Ping(pingCtx); err != nil {
		log.Warn("kafka_unreachable_at_startup", "topic", cfg.Topic, "error", err.Error(),
			"effect", "revocations wait until kafka is reachable; POST /field-reports returns 503")
	}
	cancel()

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

func splitBrokers(s string) []string {
	brokers := strings.Split(s, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}
