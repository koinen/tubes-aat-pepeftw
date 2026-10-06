package memory

import (
	"context"
	"slices"
	"strings"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func (m *MemoryRepository) SaveSourceStatus(_ context.Context, st model.SourceStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.statuses[st.Source] = st
	return nil
}

func (m *MemoryRepository) ListSourceStatus(_ context.Context) ([]model.SourceStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]model.SourceStatus, 0, len(m.statuses))
	for _, st := range m.statuses {
		out = append(out, st)
	}
	slices.SortFunc(out, func(a, b model.SourceStatus) int {
		return strings.Compare(string(a.Source), string(b.Source))
	})
	return out, nil
}
