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

// StreamPosition is where a poller stands in a source that pages with an opaque cursor.
type StreamPosition struct {
	Cursor      string    // the source's last non-null next_cursor; empty means from the start
	GeneratedAt time.Time // the source's own clock when the position was taken
}

type StreamCursorRepository interface {
	// GetStreamPosition returns found=false when the endpoint has never been polled.
	GetStreamPosition(ctx context.Context, source model.Source, endpoint string) (p StreamPosition, found bool, err error)
	SetStreamPosition(ctx context.Context, source model.Source, endpoint string, p StreamPosition) error
}
