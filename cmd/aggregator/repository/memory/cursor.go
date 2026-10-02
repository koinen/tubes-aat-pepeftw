package memory

import (
	"context"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type cursorKey struct {
	source   model.Source
	endpoint string
}

func (m *MemoryRepository) GetCursor(_ context.Context, source model.Source, endpoint string) (time.Time, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	since, found := m.cursors[cursorKey{source, endpoint}]
	return since, found, nil
}

func (m *MemoryRepository) SetCursor(_ context.Context, source model.Source, endpoint string, since time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cursors[cursorKey{source, endpoint}] = since
	return nil
}
