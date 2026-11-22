package httpapi

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

var knownSources = []model.Source{model.SourceBMKG, model.SourcePVMBG}

type sourceEntry struct {
	Status        model.Status `json:"status"`
	LastSuccessAt *time.Time   `json:"last_success_at"`
	StaleSince    *time.Time   `json:"stale_since"`
}

func (h handlers) sourcesStatus(c fiber.Ctx) error {
	statuses, err := h.statusBySource(c.Context())
	if err != nil {
		return err
	}
	return writeData(c, statuses)
}

func (h handlers) sourcesBlock(ctx context.Context) (map[model.Source]sourceEntry, error) {
	statuses, err := h.statusBySource(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[model.Source]sourceEntry, len(statuses))
	for src, st := range statuses {
		out[src] = sourceEntry{Status: st.Status, LastSuccessAt: st.LastSuccessAt, StaleSince: st.StaleSince}
	}
	return out, nil
}

func (h handlers) statusBySource(ctx context.Context) (map[model.Source]model.SourceStatus, error) {
	list, err := h.statuses.ListSourceStatus(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[model.Source]model.SourceStatus, len(knownSources))
	for _, src := range knownSources {
		out[src] = model.SourceStatus{Source: src, Status: model.StatusUnavailable, BreakerState: model.BreakerClosed}
	}
	if h.sensorPantaiEnabled {
		out[model.SourceSensorPantai] = model.SourceStatus{
			Source: model.SourceSensorPantai, Status: model.StatusUnavailable, BreakerState: model.BreakerClosed,
		}
	}
	for _, st := range list {
		if st.Source == model.SourceSensorPantai && !h.sensorPantaiEnabled {
			continue
		}
		out[st.Source] = st
	}
	return out, nil
}
