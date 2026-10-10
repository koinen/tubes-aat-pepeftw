package concurrency

import (
	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

type Limiter struct {
	slots chan struct{}
}

func New(maxConcurrent int) *Limiter {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &Limiter{slots: make(chan struct{}, maxConcurrent)}
}

func (l *Limiter) Middleware(c fiber.Ctx) error {
	select {
	case l.slots <- struct{}{}:
		defer func() { <-l.slots }()
		return c.Next()
	default:
		c.Set("Retry-After", "1")
		return common.WriteError(c, fiber.StatusTooManyRequests, common.CodeRateLimited,
			"server is at capacity, retry shortly")
	}
}
