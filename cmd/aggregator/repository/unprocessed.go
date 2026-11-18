package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type UnprocessedWriter interface {
	// SaveUnprocessed inserts m unless its message_id is already stored. inserted reports which.
	SaveUnprocessed(ctx context.Context, m model.UnprocessedMessage) (inserted bool, err error)
}

// RetryQueue is the retry worker's view of retrying unprocessed messages.
type RetryQueue interface {
	// DueRetries returns up to limit retrying messages whose next attempt is due, oldest due first.
	// A message whose stored hazard cannot be read is returned with Hazard nil.
	DueRetries(ctx context.Context, limit int) ([]model.UnprocessedMessage, error)
	// RecoverRetry saves m.Hazard (with its outbox event) and marks m recovered, in one transaction.
	RecoverRetry(ctx context.Context, m model.UnprocessedMessage) error
	// RetryFailed counts a failed attempt and schedules the next one with capped backoff.
	RetryFailed(ctx context.Context, messageID string) error
	// ParkRetry moves a retrying message to parked with the given reason.
	ParkRetry(ctx context.Context, messageID, reason string) error
}
