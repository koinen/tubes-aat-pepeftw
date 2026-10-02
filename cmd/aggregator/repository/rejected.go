package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type RejectedRepository interface {
	SaveRejected(ctx context.Context, r model.RejectedRecord) error
}
