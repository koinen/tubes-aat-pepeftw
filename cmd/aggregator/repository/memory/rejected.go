package memory

import (
	"context"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func (m *MemoryRepository) SaveRejected(_ context.Context, r model.RejectedRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rejected = append(m.rejected, r)
	return nil
}
