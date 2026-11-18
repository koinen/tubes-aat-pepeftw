package unprocessed

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

func TestUnavailableStoreIsRetrying(t *testing.T) {
	cause := fmt.Errorf("save hazard abc: %w: range unavailable", repository.ErrUnavailable)
	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, []byte(`{}`), cause, now)

	if m.Status != model.StatusRetrying || !m.Retryable || m.Code != model.ReasonStoreUnavailable {
		t.Errorf("status %q retryable %v code %q, want retrying/true/store_unavailable", m.Status, m.Retryable, m.Code)
	}
}

func TestStoreRejectingTheValueIsParked(t *testing.T) {
	cause := fmt.Errorf("save hazard abc: %w: numeric field overflow", repository.ErrRejected)
	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, []byte(`{}`), cause, now)

	if m.Status != model.StatusParked || m.Retryable || m.Code != model.ReasonStoreRejected {
		t.Errorf("status %q retryable %v code %q, want parked/false/store_rejected", m.Status, m.Retryable, m.Code)
	}
}

func TestRetryingRecordOverOneMiBStaysRetrying(t *testing.T) {
	raw := make([]byte, MaxIntact+1)
	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, repository.ErrUnavailable, now)
	if m.Status != model.StatusRetrying || m.Code != model.ReasonStoreUnavailable || len(m.Payload) != PrefixSize {
		t.Errorf("status %q code %q stored %d, want retrying/store_unavailable/65536", m.Status, m.Code, len(m.Payload))
	}
}

func TestOnlyStoreVerdictsAreRecorded(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", fmt.Errorf("save: %w", repository.ErrUnavailable), true},
		{"rejected value", fmt.Errorf("save: %w", repository.ErrRejected), true},
		{"lost leadership", fmt.Errorf("save: %w: epoch 3", fence.ErrStaleEpoch), false},
		{"shutting down", fmt.Errorf("save: %w", context.Canceled), false},
		{"our SQL bug", errors.New("save: column \"x\" does not exist"), false},
	}
	for _, tt := range tests {
		if got := Recordable(tt.err); got != tt.want {
			t.Errorf("%s: Recordable = %v, want %v", tt.name, got, tt.want)
		}
	}
}
