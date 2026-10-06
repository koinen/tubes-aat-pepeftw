package main

import (
	"slices"
	"sync"
	"time"
)

const recentCapacity = 200

type hazardView struct {
	HazardID      string    `json:"hazard_id"`
	Source        string    `json:"source"`
	HazardType    string    `json:"hazard_type"`
	Severity      string    `json:"severity"`
	AreaName      string    `json:"area_name"`
	OccurredAt    time.Time `json:"occurred_at"`
	ReceivedAt    time.Time `json:"received_at"`
	CorrelationID string    `json:"correlation_id"`
	DeliveryLagMs int64     `json:"delivery_lag_ms"`
}

type recentHazards struct {
	mu       sync.RWMutex
	seen     map[string]struct{}
	newest   []hazardView
	capacity int
}

func newRecentHazards(capacity int) *recentHazards {
	return &recentHazards{seen: map[string]struct{}{}, capacity: capacity}
}

func (r *recentHazards) add(v hazardView) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, seen := r.seen[v.HazardID]; seen {
		return false
	}
	r.seen[v.HazardID] = struct{}{}

	r.newest = append([]hazardView{v}, r.newest...)
	if len(r.newest) > r.capacity {
		r.newest = r.newest[:r.capacity]
	}
	return true
}

func (r *recentHazards) snapshot() []hazardView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Clone(r.newest)
}
