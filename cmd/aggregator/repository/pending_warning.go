package repository

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type PendingWarningRepository interface {
	SavePendingWarning(ctx context.Context, w model.PendingWarning) error
	// TakePendingWarnings returns and removes every warning waiting for the seismic event.
	TakePendingWarnings(ctx context.Context, relatedEventID string) ([]model.PendingWarning, error)
}
