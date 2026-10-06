package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type pvmbgPoller struct {
	f         *fetcher
	hazards   repository.HazardWriter
	rejected  repository.RejectedRepository
	cursors   repository.CursorRepository
	volcanoes repository.VolcanoRefRepository
	overlap   time.Duration
	log       *slog.Logger
	now       func() time.Time

	refs map[string]mapper.Volcano
}

func newPVMBGPoller(f *fetcher, d Deps, cfg Config) *pvmbgPoller {
	return &pvmbgPoller{
		f: f, hazards: d.Hazards, rejected: d.Rejected, cursors: d.Cursors, volcanoes: d.Volcanoes,
		overlap: cfg.CursorOverlap, log: d.Log, now: d.Now,
	}
}

func (p *pvmbgPoller) loadRefs(ctx context.Context) error {
	list, err := p.volcanoes.ListVolcanoRefs(ctx)
	if err != nil {
		return fmt.Errorf("load volcano_ref: %w", err)
	}
	refs := make(map[string]mapper.Volcano, len(list))
	for _, v := range list {
		refs[v.VolcanoID] = mapper.Volcano{Name: v.Name, Latitude: v.Latitude, Longitude: v.Longitude}
	}
	p.refs = refs
	p.log.Info("volcano_ref_loaded", "count", len(refs))
	return nil
}

func (p *pvmbgPoller) lookup(id string) (mapper.Volcano, bool) {
	v, ok := p.refs[id]
	return v, ok
}

func (p *pvmbgPoller) cycle(ctx context.Context) error {
	cid := correlation.From(ctx)
	now := p.now().UTC()

	if p.refs == nil {
		if err := p.loadRefs(ctx); err != nil {
			return err
		}
	}

	cursor, found, err := p.cursors.GetCursor(ctx, model.SourcePVMBG, mapper.EndpointVolcanicReports)
	if err != nil {
		return err
	}
	var since *time.Time
	if found {
		s := cursor.Add(-p.overlap)
		since = &s
	}

	body, err := p.f.get(ctx, "/volcanic-reports", since)
	if err != nil {
		return err
	}
	var resp struct {
		Reports []json.RawMessage `json:"reports"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("volcanic-reports: respons bukan object {reports: [...]}: %w", err)
	}

	newCursor := cursor
	for _, raw := range resp.Reports {
		var meta struct {
			ReportedAt string `json:"reported_at"`
		}
		_ = json.Unmarshal(raw, &meta)
		if t, err := time.Parse(time.RFC3339, meta.ReportedAt); err == nil && t.After(newCursor) {
			newCursor = t.UTC()
		}

		h, err := mapper.Volcanic(raw, p.lookup, now)
		if err != nil {
			saveRejected(ctx, p.rejected, p.log, model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, err, now)
			continue
		}
		outcome, err := p.hazards.SaveHazard(ctx, h)
		if err != nil {
			return err
		}
		if outcome == repository.OutcomeCreated {
			_, hasConfidence := h.Attributes["confidence_level"]
			p.log.Info("hazard_saved", "correlation_id", cid, "hazard_id", h.HazardID, "source", string(h.Source),
				"source_ref_id", h.SourceRefID, "severity", string(h.Severity), "confidence_level", hasConfidence)
		}
	}

	if !newCursor.IsZero() && (!found || !newCursor.Equal(cursor)) {
		if err := p.cursors.SetCursor(ctx, model.SourcePVMBG, mapper.EndpointVolcanicReports, newCursor); err != nil {
			return err
		}
	}
	return nil
}
