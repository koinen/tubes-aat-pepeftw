package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

func Connect(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
