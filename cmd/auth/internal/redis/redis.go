package redis

import (
	"context"
	"fmt"
	"tubes-aat-pepeftw/cmd/auth/internal/config"

	"github.com/redis/go-redis/v9"
)

// Connect opens a connection to Redis and verifies it with a ping.
func Connect(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password, // no password set
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}
