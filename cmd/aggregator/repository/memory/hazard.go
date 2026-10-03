package memory

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

func (m *MemoryRepository) SaveHazard(_ context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.hazards[h.HazardID]; exists {
		return repository.OutcomeUnchanged, nil
	}

	m.hazards[h.HazardID] = clone(h)
	return repository.OutcomeCreated, nil
}

func (m *MemoryRepository) ListHazards(_ context.Context, f repository.HazardFilter) ([]model.HazardEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]model.HazardEvent, 0, len(m.hazards))
	for _, h := range m.hazards {
		if f.Type != nil && h.HazardType != *f.Type {
			continue
		}
		if f.Since != nil && h.IngestedAt.Before(*f.Since) {
			continue
		}
		out = append(out, clone(h))
	}

	slices.SortFunc(out, func(a, b model.HazardEvent) int {
		return cmp.Or(b.IngestedAt.Compare(a.IngestedAt), strings.Compare(a.HazardID, b.HazardID))
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

// clone copies Attributes so callers cannot mutate stored hazards.
func clone(h model.HazardEvent) model.HazardEvent {
	h.Attributes = maps.Clone(h.Attributes)
	return h
}
