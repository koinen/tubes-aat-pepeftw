package memory

import (
	"cmp"
	"context"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

func (m *MemoryRepository) SaveHazard(_ context.Context, h model.HazardEvent) (repository.SaveResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h = clone(h)
	old, exists := m.hazards[h.HazardID]

	switch {
	case !exists:
		h.Version = 1
		h.UpdatedAt = m.now()
		m.hazards[h.HazardID] = h
		return repository.SaveResult{Outcome: repository.OutcomeCreated, Version: h.Version}, nil
	case sameContent(old, h):
		return repository.SaveResult{Outcome: repository.OutcomeUnchanged, Version: old.Version}, nil
	default:
		h.Version = old.Version + 1
		h.IngestedAt = old.IngestedAt
		h.UpdatedAt = m.now()
		m.hazards[h.HazardID] = h
		return repository.SaveResult{Outcome: repository.OutcomeUpdated, Version: h.Version}, nil
	}

}

func (m *MemoryRepository) ListHazards(_ context.Context, f repository.HazardFilter) ([]model.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]model.HazardEvent, 0, len(m.hazards))
	for _, h := range m.hazards {
		if f.Type != nil && h.HazardType != *f.Type {
			continue
		}
		if f.Since != nil && h.UpdatedAt.Before(*f.Since) {
			continue
		}
		out = append(out, clone(h))
	}

	slices.SortFunc(out, func(a, b model.HazardEvent) int {
		return cmp.Or(b.UpdatedAt.Compare(a.UpdatedAt), strings.Compare(a.HazardID, b.HazardID))
	})

	if limit := f.EffectiveLimit(); len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}

func (m *MemoryRepository) GetHazard(_ context.Context, hazardID string) (model.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h, ok := m.hazards[hazardID]

	if !ok {
		return model.HazardEvent{}, repository.ErrNotFound
	}

	return clone(h), nil
}

// Helper Functions
func sameContent(a, b model.HazardEvent) bool {
	a.Version, b.Version = 0, 0
	a.IngestedAt, b.IngestedAt = time.Time{}, time.Time{}
	a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}

// clone copies Attributes so callers cannot mutate stored hazards.
func clone(h model.HazardEvent) model.HazardEvent {
	h.Attributes = maps.Clone(h.Attributes)
	return h
}
