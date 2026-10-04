package poller

import (
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type Breaker struct {
	Failures int
	OpenFor  time.Duration

	state    model.BreakerState
	failures int
	openedAt time.Time
}

func NewBreaker(failures int, openFor time.Duration) *Breaker {
	return &Breaker{Failures: failures, OpenFor: openFor, state: model.BreakerClosed}
}

func (b *Breaker) Allow(now time.Time) bool {
	if b.state == model.BreakerOpen && now.Sub(b.openedAt) >= b.OpenFor {
		b.state = model.BreakerHalfOpen
	}
	return b.state != model.BreakerOpen
}

func (b *Breaker) Success() {
	b.state = model.BreakerClosed
	b.failures = 0
}

func (b *Breaker) Failure(now time.Time) {
	b.failures++
	if b.state == model.BreakerHalfOpen || b.failures >= b.Failures {
		b.state = model.BreakerOpen
		b.openedAt = now
	}
}

func (b *Breaker) State() model.BreakerState { return b.state }
