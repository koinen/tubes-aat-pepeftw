package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"
)

const (
	EndpointObservations = "observations"

	sensorPageLimit = 1000
	sensorMaxPages  = 20

	// stationsMaxAge is how long the station list is used before it is fetched again.
	stationsMaxAge = 5 * time.Minute
	// stationsRetry is the shortest gap between two fetches of the station list after the first.
	stationsRetry = 30 * time.Second
)

type sensorPantaiPoller struct {
	f         *fetcher
	hazards   repository.HazardWriter
	existing  repository.HazardGetter
	dlq       repository.UnprocessedWriter
	positions repository.StreamCursorRepository
	log       *slog.Logger
	now       func() time.Time

	pageLimit int
	maxPages  int

	stations         map[string]mapper.Station
	stationsLoadedAt time.Time // last successful fetch
	stationsTriedAt  time.Time // last fetch attempt
}

func newSensorPantaiPoller(f *fetcher, d Deps) *sensorPantaiPoller {
	return &sensorPantaiPoller{
		f: f, hazards: d.Hazards, existing: d.Existing, dlq: d.Unprocessed, positions: d.Positions,
		log: d.Log, now: d.Now, pageLimit: sensorPageLimit, maxPages: sensorMaxPages,
	}
}

// sensorPage is one /v1/observations response.
type sensorPage struct {
	meta  mapper.CoastalPage
	items []json.RawMessage
}

func (p *sensorPantaiPoller) cycle(ctx context.Context) error {
	if err := p.refreshStations(ctx); err != nil {
		return err
	}
	pos, _, err := p.positions.GetStreamPosition(ctx, model.SourceSensorPantai, EndpointObservations)
	if err != nil {
		return err
	}
	rec := newRecorder(p.dlq, p.log, model.SourceSensorPantai, p.now().UTC())

	pages, next, err := p.fetch(ctx, rec, pos)
	if err != nil {
		return err
	}
	trusted := p.checkUnknownStations(ctx, pages)
	lookup := func(id string) (mapper.Station, bool) { s, ok := p.stations[id]; return s, ok }
	var criticals, rest []mapped
	held := 0
	for _, pg := range pages {
		for _, raw := range pg.items {
			if id := stationOf(raw); !trusted && id != "" {
				if _, known := lookup(id); !known {
					held++ // maybe a station added since our list; read it again once the list can be refreshed
					continue
				}
			}
			m := mapped{raw: raw, CoastalResult: mapper.Coastal(raw, pg.meta, lookup)}
			if m.Hazard != nil && m.Hazard.Severity == model.SeverityAwas {
				criticals = append(criticals, m)
			} else {
				rest = append(rest, m)
			}
		}
	}
	// Criticals go first so their outbox rows exist before a large backlog's.
	for _, m := range append(criticals, rest...) {
		if err := p.write(ctx, rec, m); err != nil {
			return err
		}
	}
	if rec.err != nil {
		return rec.err
	}
	if held > 0 {
		return fmt.Errorf("%d observations held: unknown station_id and the station list could not be refreshed", held)
	}
	return p.positions.SetStreamPosition(ctx, model.SourceSensorPantai, EndpointObservations, next)
}

type mapped struct {
	raw json.RawMessage
	mapper.CoastalResult
}

// write stores one mapped observation: its HazardEvent, its parked violation, or both for an unverified
// alert. It returns an error only when the cycle must stop.
func (p *sensorPantaiPoller) write(ctx context.Context, rec *recorder, m mapped) error {
	if m.Hazard != nil {
		outcome, err := p.hazards.SaveHazard(ctx, *m.Hazard)
		if err != nil {
			if err := rec.storeFailed(ctx, EndpointObservations, m.raw, *m.Hazard, err); err != nil {
				return err
			}
		}
		if outcome == repository.OutcomeUnchanged && p.conflicts(ctx, *m.Hazard) {
			rec.park(ctx, EndpointObservations, m.raw, mapper.IDConflict(m.Hazard.SourceRefID))
		}
	}
	if m.Violation != nil {
		rec.park(ctx, EndpointObservations, m.raw, m.Violation)
	}
	return nil
}

// conflicts reports whether h's observation_id is already stored with different values. A failed
// read is logged and treated as no conflict, so it never holds up the cycle.
func (p *sensorPantaiPoller) conflicts(ctx context.Context, h model.HazardEvent) bool {
	stored, err := p.existing.GetHazard(ctx, h.HazardID)
	if err != nil {
		p.log.Warn("id_conflict_check_failed", "correlation_id", correlation.From(ctx), "hazard_id", h.HazardID,
			"source_ref_id", h.SourceRefID, "error", err.Error())
		return false
	}
	return !mapper.SameObservation(stored, h)
}

// fetch reads pages from pos until the sensor has nothing more, and returns them with the position
// after the last one.
func (p *sensorPantaiPoller) fetch(ctx context.Context, rec *recorder, pos repository.StreamPosition) ([]sensorPage, repository.StreamPosition, error) {
	var pages []sensorPage
	for range p.maxPages {
		q := url.Values{"limit": {strconv.Itoa(p.pageLimit)}}
		if pos.Cursor != "" {
			q.Set("cursor", pos.Cursor)
		}
		body, err := p.f.get(ctx, "/v1/observations", q)
		fetchedAt := p.now().UTC()
		var status *StatusError
		if errors.As(err, &status) && status.Code == http.StatusBadRequest && pos.Cursor != "" {
			p.restarted(ctx, "cursor_rejected", pos.GeneratedAt, time.Time{})
			pos = repository.StreamPosition{}
			continue
		}
		var resp struct {
			GeneratedAt time.Time         `json:"generated_at"`
			Items       []json.RawMessage `json:"items"`
			NextCursor  *string           `json:"next_cursor"`
		}
		if err := rec.decode(ctx, EndpointObservations, body, err, &resp); err != nil {
			return nil, pos, err
		}
		if resp.GeneratedAt.IsZero() {
			// Without the sensor's clock no observation can be checked against it.
			cause := &mapper.RejectError{Code: model.ReasonContractViolation, Reason: "respons tanpa generated_at"}
			rec.save(ctx, unprocessed.BuildResponse(model.SourceSensorPantai, EndpointObservations, body, cause, rec.now))
			return nil, pos, fmt.Errorf("GET /v1/observations: %w", cause)
		}
		generatedAt := resp.GeneratedAt.UTC()
		if pos.Cursor != "" && generatedAt.Before(pos.GeneratedAt) {
			// The old cursor was accepted by a new run; this page is from the middle of it.
			p.restarted(ctx, "generated_at_backwards", pos.GeneratedAt, generatedAt)
			pos = repository.StreamPosition{}
			continue
		}
		pages = append(pages, sensorPage{
			meta:  mapper.CoastalPage{GeneratedAt: generatedAt, FetchedAt: fetchedAt},
			items: resp.Items,
		})
		pos.GeneratedAt = generatedAt
		if resp.NextCursor == nil {
			break
		}
		pos.Cursor = *resp.NextCursor
	}
	return pages, pos, nil
}

// restarted logs a sensor restart. The poller then reads the stream from the start; observations it
// already has are deduplicated by hazard_id. A restart is not a failure.
func (p *sensorPantaiPoller) restarted(ctx context.Context, signal string, before, after time.Time) {
	attrs := []any{"correlation_id", correlation.From(ctx), "target", string(model.SourceSensorPantai), "signal", signal,
		"generated_at_before", before.Format(time.RFC3339)}
	if !after.IsZero() {
		attrs = append(attrs, "generated_at_after", after.Format(time.RFC3339))
	}
	p.log.Warn("sensor_restart_detected", attrs...)
}

// refreshStations fetches the station list when there is none or it is older than stationsMaxAge.
// Without a list the cycle cannot check station_id, so a failure then fails the cycle. With one, a
// failed refresh is logged, the old list is kept, and the next try waits stationsRetry.
func (p *sensorPantaiPoller) refreshStations(ctx context.Context) error {
	now := p.now()
	if p.stations != nil && (now.Sub(p.stationsLoadedAt) < stationsMaxAge || now.Sub(p.stationsTriedAt) < stationsRetry) {
		return nil
	}
	err := p.loadStations(ctx, now)
	if err != nil && p.stations != nil {
		p.log.Warn("stations_refresh_failed", "correlation_id", correlation.From(ctx), "target", string(model.SourceSensorPantai),
			"error", err.Error(), "stations_age_s", int(now.Sub(p.stationsLoadedAt).Seconds()))
		return nil
	}
	return err
}

// checkUnknownStations refreshes the station list, at most once per stationsRetry, when pages name a
// station_id it does not have. It reports whether the list is fresh enough to park an unknown id.
func (p *sensorPantaiPoller) checkUnknownStations(ctx context.Context, pages []sensorPage) bool {
	unknown := false
	for _, pg := range pages {
		for _, raw := range pg.items {
			if _, ok := p.stations[stationOf(raw)]; !ok {
				unknown = true
			}
		}
	}
	now := p.now()
	if unknown && now.Sub(p.stationsTriedAt) >= stationsRetry {
		if err := p.loadStations(ctx, now); err != nil {
			p.log.Warn("stations_refresh_failed", "correlation_id", correlation.From(ctx), "target", string(model.SourceSensorPantai),
				"reason", "unknown_station_id", "error", err.Error())
		}
	}
	return p.now().Sub(p.stationsLoadedAt) < stationsRetry
}

// stationOf returns an observation's station_id, or "" when it has none that is a string.
func stationOf(raw json.RawMessage) string {
	var o struct {
		StationID any `json:"station_id"`
	}
	_ = json.Unmarshal(raw, &o)
	id, _ := o.StationID.(string)
	return id
}

func (p *sensorPantaiPoller) loadStations(ctx context.Context, now time.Time) error {
	p.stationsTriedAt = now
	body, err := p.f.get(ctx, "/v1/stations", nil)
	if err != nil {
		return err
	}
	var resp struct {
		Items []struct {
			StationID string  `json:"station_id"`
			Name      string  `json:"name"`
			Province  string  `json:"province"`
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("GET /v1/stations: %w", err)
	}
	stations := make(map[string]mapper.Station, len(resp.Items))
	for _, s := range resp.Items {
		stations[s.StationID] = mapper.Station{Name: s.Name, Province: s.Province, Latitude: s.Latitude, Longitude: s.Longitude}
	}
	p.stations = stations
	p.stationsLoadedAt = now
	return nil
}
