package model

import "time"

type Status string

const (
	StatusOK          Status = "OK"
	StatusStale       Status = "STALE"
	StatusUnavailable Status = "UNAVAILABLE"
)

type BreakerState string

const (
	BreakerClosed   BreakerState = "CLOSED"
	BreakerOpen     BreakerState = "OPEN"
	BreakerHalfOpen BreakerState = "HALF_OPEN"
)

type SourceStatus struct {
	Source        Source       `json:"source"`
	Status        Status       `json:"status"`
	BreakerState  BreakerState `json:"breaker_state"`
	LastSuccessAt *time.Time   `json:"last_success_at"`
	StaleSince    *time.Time   `json:"stale_since"`
	LastError     string       `json:"last_error,omitempty"`
}
