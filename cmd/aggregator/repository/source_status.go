package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type SourceStatusRepository interface {
	SaveSourceStatus(ctx context.Context, s model.SourceStatus) error
	ListSourceStatus(ctx context.Context) ([]model.SourceStatus, error)
}
