package memory

import (
	"sync"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.HazardWriter           = (*MemoryRepository)(nil)
	_ repository.HazardReader           = (*MemoryRepository)(nil)
	_ repository.CursorRepository       = (*MemoryRepository)(nil)
	_ repository.SourceStatusRepository = (*MemoryRepository)(nil)
	_ repository.RejectedRepository     = (*MemoryRepository)(nil)
)

type MemoryRepository struct {
	mu       sync.RWMutex
	hazards  map[string]model.HazardEvent
	cursors  map[cursorKey]time.Time
	statuses map[model.Source]model.SourceStatus
	rejected []model.RejectedRecord
}

func New() *MemoryRepository {
	return &MemoryRepository{
		hazards:  map[string]model.HazardEvent{},
		cursors:  map[cursorKey]time.Time{},
		statuses: map[model.Source]model.SourceStatus{},
	}
}
