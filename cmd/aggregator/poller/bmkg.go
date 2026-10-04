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

const cacheTTL = time.Hour

type bmkgPoller struct {
	f        *fetcher
	hazards  repository.HazardWriter
	rejected repository.RejectedRepository
	cursors  repository.CursorRepository
	overlap  time.Duration
	grace    time.Duration
	log      *slog.Logger
	now      func() time.Time

	warnings map[string]map[string]cachedWarning
	pending  map[string]pendingEvent
	saved    map[string]time.Time
}

type cachedWarning struct {
	w      mapper.TsunamiWarning
	seenAt time.Time
}

type pendingEvent struct {
	raw        json.RawMessage
	occurredAt time.Time
	firstSeen  time.Time
}

type seismicMeta struct {
	EventID          string `json:"event_id"`
	OccurredAt       string `json:"occurred_at"`
	PotentialTsunami bool   `json:"potential_tsunami"`
}

func newBMKGPoller(f *fetcher, d Deps, cfg Config) *bmkgPoller {
	return &bmkgPoller{
		f: f, hazards: d.Hazards, rejected: d.Rejected, cursors: d.Cursors,
		overlap: cfg.CursorOverlap, grace: cfg.TsunamiGrace, log: d.Log, now: d.Now,
		warnings: map[string]map[string]cachedWarning{},
		pending:  map[string]pendingEvent{},
		saved:    map[string]time.Time{},
	}
}

func (p *bmkgPoller) cycle(ctx context.Context) error {
	cid := correlation.From(ctx)
	now := p.now().UTC()

	cursor, found, err := p.cursors.GetCursor(ctx, model.SourceBMKG, mapper.EndpointSeismicEvents)
	if err != nil {
		return err
	}
	var since *time.Time
	if found {
		s := cursor.Add(-p.overlap)
		since = &s
	}

	// 1. TsunamiWarning
	body, err := p.f.get(ctx, "/tsunami-warnings", since)
	if err != nil {
		return err
	}
	var rawWarnings []json.RawMessage
	if err := json.Unmarshal(body, &rawWarnings); err != nil {
		return fmt.Errorf("tsunami-warnings: respons bukan array JSON: %w", err)
	}
	for _, raw := range rawWarnings {
		w, err := mapper.ParseTsunamiWarning(raw)
		if err != nil {
			p.reject(ctx, mapper.EndpointTsunamiWarnings, raw, err, now)
			continue
		}
		byEvent := p.warnings[w.RelatedEventID]
		if byEvent == nil {
			byEvent = map[string]cachedWarning{}
			p.warnings[w.RelatedEventID] = byEvent
		}
		if _, known := byEvent[w.WarningID]; !known {
			if _, alreadySaved := p.saved[w.RelatedEventID]; alreadySaved {
				p.log.Warn("tsunami_warning_too_late", "correlation_id", cid,
					"warning_id", w.WarningID, "related_event_id", w.RelatedEventID)
			}
		}
		byEvent[w.WarningID] = cachedWarning{w: w, seenAt: now}
	}

	// 2. SeismicEvent
	body, err = p.f.get(ctx, "/seismic-events", since)
	if err != nil {
		return err
	}
	var rawEvents []json.RawMessage
	if err := json.Unmarshal(body, &rawEvents); err != nil {
		return fmt.Errorf("seismic-events: respons bukan array JSON: %w", err)
	}

	newCursor := cursor
	candidates := map[string]pendingEvent{}
	var order []string
	for id, pe := range p.pending {
		candidates[id] = pe
		order = append(order, id)
	}
	for _, raw := range rawEvents {
		var meta seismicMeta
		_ = json.Unmarshal(raw, &meta)
		occurred, terr := time.Parse(time.RFC3339, meta.OccurredAt)
		if terr == nil && occurred.After(newCursor) {
			newCursor = occurred.UTC()
		}
		if meta.EventID == "" || terr != nil {
			_, err := mapper.Seismic(raw, nil, now)
			p.reject(ctx, mapper.EndpointSeismicEvents, raw, err, now)
			continue
		}
		pe, wasPending := candidates[meta.EventID]
		if !wasPending {
			pe.firstSeen = now
			order = append(order, meta.EventID)
		}
		pe.raw, pe.occurredAt = raw, occurred.UTC()
		candidates[meta.EventID] = pe
	}

	// 3. Petakan dan simpan
	for _, id := range order {
		pe := candidates[id]
		var meta seismicMeta
		_ = json.Unmarshal(pe.raw, &meta)
		warnings := p.warningsFor(id)

		if meta.PotentialTsunami && len(warnings) == 0 && now.Sub(pe.firstSeen) < p.grace {
			if _, already := p.pending[id]; !already {
				p.log.Info("seismic_event_pending_warning", "correlation_id", cid, "event_id", id)
			}
			p.pending[id] = pe
			continue
		}
		delete(p.pending, id)

		h, err := mapper.Seismic(pe.raw, warnings, now)
		if err != nil {
			p.reject(ctx, mapper.EndpointSeismicEvents, pe.raw, err, now)
			continue
		}
		outcome, err := p.hazards.SaveHazard(ctx, h)
		if err != nil {
			p.pending[id] = pe
			return err
		}
		p.saved[id] = now
		if outcome == repository.OutcomeCreated {
			if meta.PotentialTsunami && len(warnings) == 0 {
				p.log.Warn("tsunami_warning_timeout", "correlation_id", cid, "event_id", id,
					"waited", now.Sub(pe.firstSeen).String())
			}
			p.log.Info("hazard_saved", "correlation_id", cid, "hazard_id", h.HazardID, "source", string(h.Source),
				"source_ref_id", h.SourceRefID, "severity", string(h.Severity), "tsunami_warnings", len(warnings))
		}
	}

	for _, pe := range p.pending {
		if pe.occurredAt.Before(newCursor) {
			newCursor = pe.occurredAt
		}
	}
	if !newCursor.IsZero() && (!found || !newCursor.Equal(cursor)) {
		for _, ep := range []string{mapper.EndpointSeismicEvents, mapper.EndpointTsunamiWarnings} {
			if err := p.cursors.SetCursor(ctx, model.SourceBMKG, ep, newCursor); err != nil {
				return err
			}
		}
	}

	p.evict(now)
	return nil
}

func (p *bmkgPoller) warningsFor(eventID string) []mapper.TsunamiWarning {
	var out []mapper.TsunamiWarning
	for _, cw := range p.warnings[eventID] {
		out = append(out, cw.w)
	}
	return out
}

func (p *bmkgPoller) evict(now time.Time) {
	for eventID, byEvent := range p.warnings {
		for wid, cw := range byEvent {
			if now.Sub(cw.seenAt) > cacheTTL {
				delete(byEvent, wid)
			}
		}
		if len(byEvent) == 0 {
			delete(p.warnings, eventID)
		}
	}
	for id, at := range p.saved {
		if now.Sub(at) > cacheTTL {
			delete(p.saved, id)
		}
	}
}

func (p *bmkgPoller) reject(ctx context.Context, endpoint string, raw json.RawMessage, cause error, now time.Time) {
	saveRejected(ctx, p.rejected, p.log, model.SourceBMKG, endpoint, raw, cause, now)
}

func saveRejected(ctx context.Context, repo repository.RejectedRepository, log *slog.Logger,
	source model.Source, endpoint string, raw json.RawMessage, cause error, now time.Time) {
	r := mapper.Rejected(source, endpoint, raw, cause, now)
	log.Warn("record_rejected", "correlation_id", correlation.From(ctx), "source", string(source),
		"endpoint", endpoint, "source_ref_id", r.SourceRefID, "reason", r.Reason)
	if err := repo.SaveRejected(ctx, r); err != nil {
		log.Error("rejected_save_failed", "correlation_id", correlation.From(ctx), "error", err.Error())
	}
}
