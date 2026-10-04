package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type OutboxRepository interface {
	PendingOutbox(ctx context.Context, limit int) ([]model.OutboxRecord, error)
	MarkPublished(ctx context.Context, ids []int64) error
}
