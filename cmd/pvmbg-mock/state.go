package main

import (
	"context"
	"sync"
	"time"
)

const (
	outageModeHang  = "hang"
	outageModeError = "error"
)

type State struct {
	mu            sync.RWMutex
	schemaVersion int
	outageEnabled bool
	outageMode    string
	recovered     chan struct{}
	corrupt       adminCorrupt
}

func newState(schemaVersion int) *State {
	return &State{schemaVersion: schemaVersion, outageMode: outageModeHang, recovered: make(chan struct{})}
}

func (s *State) SchemaVersion() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schemaVersion
}

func (s *State) SetSchemaVersion(v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaVersion = v
}

func (s *State) Outage() (enabled bool, mode string, recovered <-chan struct{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.outageEnabled, s.outageMode, s.recovered
}

func (s *State) SetOutage(enabled bool, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if enabled {
		if !s.outageEnabled {
			s.recovered = make(chan struct{})
		}
		s.outageEnabled = true
		s.outageMode = mode
		return
	}
	if s.outageEnabled {
		close(s.recovered)
	}
	s.outageEnabled = false
}

// SetCorrupt corrupts the next count responses with kind. Count 0 turns corruption off.
func (s *State) SetCorrupt(kind string, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count == 0 {
		s.corrupt = adminCorrupt{}
		return
	}
	s.corrupt = adminCorrupt{Kind: kind, Remaining: count}
}

// TakeCorrupt uses up one corrupted response, if any is left.
func (s *State) TakeCorrupt() (kind string, remaining int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.corrupt.Remaining == 0 {
		return "", 0, false
	}
	kind = s.corrupt.Kind
	s.corrupt.Remaining--
	if s.corrupt.Remaining == 0 {
		s.corrupt.Kind = ""
	}
	return kind, s.corrupt.Remaining, true
}

type adminOutage struct {
	Enabled bool   `json:"enabled"`
	Mode    string `json:"mode,omitempty"`
}

type adminState struct {
	SchemaVersion int          `json:"schema_version"`
	SchemaMode    string       `json:"schema_mode"`
	Outage        adminOutage  `json:"outage"`
	Corrupt       adminCorrupt `json:"corrupt"`
}

func (s *State) Snapshot() adminState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return adminState{SchemaVersion: s.schemaVersion, SchemaMode: schemaModes[s.schemaVersion], Outage: adminOutage{Enabled: s.outageEnabled, Mode: s.outageMode}, Corrupt: s.corrupt}
}

func waitWhileHanging(ctx context.Context, recovered <-chan struct{}, max time.Duration) bool {
	t := time.NewTimer(max)
	defer t.Stop()
	select {
	case <-recovered:
		return true
	case <-t.C:
		return false
	case <-ctx.Done():
		return false
	}
}
