package repository

import (
	"context"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type CursorRepository interface {
	// GetCursor returns found=false when the endpoint has never been polled.
	GetCursor(ctx context.Context, source model.Source, endpoint string) (since time.Time, found bool, err error)
	SetCursor(ctx context.Context, source model.Source, endpoint string, since time.Time) error
}
