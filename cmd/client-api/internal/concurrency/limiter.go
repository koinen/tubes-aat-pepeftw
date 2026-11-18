package concurrency

import (
	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/client-api/internal/common"
)

type Limiter struct {
	reserved chan struct{}
	media    chan struct{}
}

func New(maxConcurrent int) *Limiter {
	reserved := max(1, maxConcurrent/4)
	return &Limiter{
		reserved: make(chan struct{}, reserved),
		media:    make(chan struct{}, maxConcurrent-reserved),
	}
}

func (l *Limiter) Middleware(c fiber.Ctx) error {
	switch common.GetSub(c) {
	case "field-team", "bnpb-ops":
		if take(l.reserved) {
			defer func() { <-l.reserved }()
			return c.Next()
		}
	}
	if take(l.media) {
		defer func() { <-l.media }()
		return c.Next()
	}

	c.Set("Retry-After", "2")
	return common.WriteError(c, fiber.StatusServiceUnavailable, common.CodeOverloaded,
		"server is at capacity, retry shortly")
}

func take(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}
