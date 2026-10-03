package repository

import (
	"context"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// What SaveHazard did
type SaveOutcome string

const (
	OutcomeCreated   SaveOutcome = "CREATED"
	OutcomeUnchanged SaveOutcome = "UNCHANGED"
)

// Hazard Write
type HazardWriter interface {
	SaveHazard(ctx context.Context, h model.HazardEvent) (SaveOutcome, error)
}

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// Hazard Read
// HazardFilter selects hazards with IngestedAt >= Since
type HazardFilter struct {
	Type  *model.HazardType
	Since *time.Time
	Limit int
}

func (f HazardFilter) EffectiveLimit() int {
	switch {
	case f.Limit <= 0:
		return DefaultLimit
	case f.Limit > MaxLimit:
		return MaxLimit
	default:
		return f.Limit
	}
}

type HazardReader interface {
	ListHazards(ctx context.Context, f HazardFilter) ([]model.HazardEvent, error)
	GetHazard(ctx context.Context, hazardID string) (model.HazardEvent, error)
}
