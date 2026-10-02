package memory

import (
	"sync"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.HazardWriter             = (*MemoryRepository)(nil)
	_ repository.HazardReader             = (*MemoryRepository)(nil)
	_ repository.CursorRepository         = (*MemoryRepository)(nil)
	_ repository.SourceStatusRepository   = (*MemoryRepository)(nil)
	_ repository.PendingWarningRepository = (*MemoryRepository)(nil)
	_ repository.RejectedRepository       = (*MemoryRepository)(nil)
)

type MemoryRepository struct {
	mu       sync.RWMutex
	now      func() time.Time
	hazards  map[string]model.HazardEvent
	cursors  map[cursorKey]time.Time
	statuses map[model.Source]model.SourceStatus
	pending  map[string][]model.PendingWarning
	rejected []model.RejectedRecord
}

func New() *MemoryRepository {
	return &MemoryRepository{
		now:      func() time.Time { return time.Now().UTC().Truncate(time.Second) },
		hazards:  map[string]model.HazardEvent{},
		cursors:  map[cursorKey]time.Time{},
		statuses: map[model.Source]model.SourceStatus{},
		pending:  map[string][]model.PendingWarning{},
	}
}
