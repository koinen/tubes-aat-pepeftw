package ratelimit

import (
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"tubes-aat-pepeftw/cmd/client-api/internal/common"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

//go:embed lua/sliding_window.lua
var slidingWindowScript string

var script = redis.NewScript(slidingWindowScript)

type Limiter struct {
	redisClient  *redis.Client
	maxRequests  int
	windowMillis int64
}

func New(redisClient *redis.Client, cfg config.RateLimitConfig) *Limiter {
	return &Limiter{
		redisClient:  redisClient,
		maxRequests:  cfg.Max,
		windowMillis: int64(cfg.WindowSeconds) * 1000,
	}
}

func (l *Limiter) Middleware(c fiber.Ctx) error {
	sub := common.GetSub(c)
	if sub == "" {
		return c.Next()
	}

	key := fmt.Sprintf("rl:%s", sub)
	now := time.Now().UnixMilli()
	member := uuid.NewString()

	res, err := script.Run(
		c.Context(),
		l.redisClient,
		[]string{key},
		now,
		l.windowMillis,
		l.maxRequests,
		member,
	).Slice()

	if err != nil || len(res) < 2 {
		// Redis error or script failure: fail-open
		return c.Next()
	}

	allowed, ok1 := res[0].(int64)
	retryAfter, ok2 := res[1].(int64)
	if !ok1 || !ok2 {
		return c.Next()
	}

	if allowed == 0 {
		c.Set("Retry-After", strconv.FormatInt(retryAfter, 10))
		return common.WriteError(c, fiber.StatusTooManyRequests, common.CodeRateLimited,
			fmt.Sprintf("rate limit exceeded, retry after %d seconds", retryAfter))
	}

	return c.Next()
}
