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

// HazardBatchStore writes and reads many hazards per round trip, for a source that delivers
// thousands of records per poll.
type HazardBatchStore interface {
	HazardGetter
	// SaveHazards stores hs in one transaction and returns one outcome per element. A hazard already
	// stored, or repeated earlier in hs, is OutcomeUnchanged.
	SaveHazards(ctx context.Context, hs []model.HazardEvent) ([]SaveOutcome, error)
	// GetHazards returns the stored hazards among ids, keyed by hazard_id.
	GetHazards(ctx context.Context, ids []string) (map[string]model.HazardEvent, error)
}

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// Hazard Read
// HazardFilter selects hazards with IngestedAt >= Since
type HazardFilter struct {
	Type   *model.HazardType
	Since  *time.Time
	Limit  int
	Cursor *Keyset
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
	ListHazards(ctx context.Context, f HazardFilter) (Page[model.HazardEvent], error)
	HazardGetter
}

type HazardGetter interface {
	GetHazard(ctx context.Context, hazardID string) (model.HazardEvent, error)
}
