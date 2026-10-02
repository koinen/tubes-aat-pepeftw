package memory

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func (m *MemoryRepository) SavePendingWarning(_ context.Context, w model.PendingWarning) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pending[w.RelatedEventID] = append(m.pending[w.RelatedEventID], w)
	return nil
}

func (m *MemoryRepository) TakePendingWarnings(_ context.Context, relatedEventID string) ([]model.PendingWarning, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	warnings := m.pending[relatedEventID]
	delete(m.pending, relatedEventID)
	return warnings, nil
}
