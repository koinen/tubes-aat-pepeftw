package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var sensorStart = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

// fakeSensor plays Sensor Pantai's observations API. Its cursor is "at:<offset>"; a cursor past the
// end of the stream gets 400, like the real sensor's "cursor tidak valid".
type fakeSensor struct {
	mu           sync.Mutex
	items        []string
	generatedAt  time.Time
	stations     []string
	stationsDown bool
	// omitGeneratedAt leaves generated_at out of observation responses.
	omitGeneratedAt bool
	requests        []string // query strings of observation requests, in order
	stationCalls    int
}

func newFakeSensor(t *testing.T) (*fakeSensor, *httptest.Server) {
	s := &fakeSensor{generatedAt: sensorStart.Add(time.Hour), stations: []string{"stn-padang-01"}}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *fakeSensor) add(items ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, items...)
}

func (s *fakeSensor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.URL.Path {
	case "/v1/stations":
		s.stationCalls++
		if s.stationsDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var items []string
		for _, id := range s.stations {
			items = append(items, fmt.Sprintf(`{"station_id":%q,"name":"Padang","province":"Sumatera Barat","latitude":-0.9492,"longitude":100.3543}`, id))
		}
		io.WriteString(w, `{"items":[`+strings.Join(items, ",")+`]}`)
	case "/v1/observations":
		s.requests = append(s.requests, r.URL.RawQuery)
		offset := 0
		if c := r.URL.Query().Get("cursor"); c != "" {
			n, err := strconv.Atoi(strings.TrimPrefix(c, "at:"))
			if err != nil || !strings.HasPrefix(c, "at:") || n > len(s.items) {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"error":{"code":"bad_request","message":"cursor tidak valid"}}`)
				return
			}
			offset = n
		}
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit < 1 || limit > 1000 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		end := min(offset+limit, len(s.items))
		next := "null"
		if end > offset {
			next = strconv.Quote("at:" + strconv.Itoa(end))
		}
		generated := fmt.Sprintf(`"generated_at":%q,`, s.generatedAt.Format(time.RFC3339))
		if s.omitGeneratedAt {
			generated = ""
		}
		fmt.Fprintf(w, `{%s"items":[%s],"next_cursor":%s}`, generated, strings.Join(s.items[offset:end], ","), next)
	default:
		http.NotFound(w, r)
	}
}

// observation returns a valid observation with id obs-<n>, overridden by extra (JSON object members).
func observation(n int, extra string) string {
	o := map[string]any{
		"observation_id": fmt.Sprintf("obs-%06d", n), "station_id": "stn-padang-01",
		"observed_at":         sensorStart.Add(time.Duration(n) * 15 * time.Second).Format(time.RFC3339),
		"sea_level_anomaly_m": 0.2, "wave_height_m": 1.1, "quality": "ok", "critical": false,
	}
	if extra != "" {
		if err := json.Unmarshal([]byte(extra), &o); err != nil {
			panic(err)
		}
	}
	b, _ := json.Marshal(o)
	return string(b)
}

// sensorStore is the poller's view of the database.
type sensorStore struct {
	saved       []model.HazardEvent
	unprocessed []model.UnprocessedMessage
	position    repository.StreamPosition
	hasPosition bool
	positions   int // SetStreamPosition calls
	saveErr     error
}

func (s *sensorStore) SaveHazard(_ context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	if s.saveErr != nil {
		return "", s.saveErr
	}
	for _, x := range s.saved {
		if x.HazardID == h.HazardID {
			return repository.OutcomeUnchanged, nil
		}
	}
	s.saved = append(s.saved, h)
	return repository.OutcomeCreated, nil
}

func (s *sensorStore) GetHazard(_ context.Context, id string) (model.HazardEvent, error) {
	for _, x := range s.saved {
		if x.HazardID == id {
			return x, nil
		}
	}
	return model.HazardEvent{}, repository.ErrNotFound
}

func (s *sensorStore) SaveUnprocessed(_ context.Context, m model.UnprocessedMessage) (bool, error) {
	for _, x := range s.unprocessed {
		if x.MessageID == m.MessageID {
			return false, nil
		}
	}
	s.unprocessed = append(s.unprocessed, m)
	return true, nil
}

func (s *sensorStore) GetStreamPosition(context.Context, model.Source, string) (repository.StreamPosition, bool, error) {
	return s.position, s.hasPosition, nil
}

func (s *sensorStore) SetStreamPosition(_ context.Context, _ model.Source, _ string, p repository.StreamPosition) error {
	s.position, s.hasPosition = p, true
	s.positions++
	return nil
}

func (s *sensorStore) refs() []string {
	var out []string
	for _, h := range s.saved {
		out = append(out, h.SourceRefID)
	}
	return out
}

// sensorHarness runs sensor poll cycles against a fake sensor with a clock that advances 10 ms per call.
type sensorHarness struct {
	t      *testing.T
	sensor *fakeSensor
	store  *sensorStore
	poller *sensorPantaiPoller
	clock  time.Time
}

func newSensorHarness(t *testing.T) *sensorHarness {
	h := &sensorHarness{t: t, store: &sensorStore{}, clock: time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)}
	var srv *httptest.Server
	h.sensor, srv = newFakeSensor(t)
	now := func() time.Time {
		h.clock = h.clock.Add(10 * time.Millisecond)
		return h.clock
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	f := newFetcher(model.SourceSensorPantai, srv.URL, time.Second, func(*http.Request) {}, log, now)
	deps := Deps{Hazards: h.store, Existing: h.store, Unprocessed: h.store, Positions: h.store, Log: log, Now: now}
	h.poller = newSensorPantaiPoller(f, deps)
	return h
}

func (h *sensorHarness) cycle() error {
	h.t.Helper()
	return h.poller.cycle(correlation.With(context.Background(), "cid-sensor"))
}

func (h *sensorHarness) mustCycle() {
	h.t.Helper()
	if err := h.cycle(); err != nil {
		h.t.Fatalf("cycle: %v", err)
	}
}

func TestSensorCycleStoresObservationsAndKeepsLastCursorWhenCaughtUp(t *testing.T) {
	h := newSensorHarness(t)
	h.sensor.add(observation(0, ""), observation(1, ""))

	h.mustCycle()
	if got := h.store.refs(); strings.Join(got, ",") != "obs-000000,obs-000001" {
		t.Fatalf("saved %v, want obs-000000 and obs-000001", got)
	}
	if h.saved(0).Source != model.SourceSensorPantai || h.saved(0).Attributes["fetched_at"] == nil {
		t.Errorf("saved %+v, want a SENSOR_PANTAI hazard with fetched_at", h.saved(0))
	}
	want := repository.StreamPosition{Cursor: "at:2", GeneratedAt: sensorStart.Add(time.Hour)}
	if h.store.position != want {
		t.Fatalf("position %+v, want %+v", h.store.position, want)
	}

	// Caught up: the sensor answers next_cursor null, so the poller keeps its last real cursor.
	h.sensor.generatedAt = h.sensor.generatedAt.Add(time.Minute)
	h.mustCycle()
	want.GeneratedAt = h.sensor.generatedAt
	if h.store.position != want {
		t.Fatalf("after caught-up cycle position %+v, want %+v", h.store.position, want)
	}
	if last := h.sensor.requests[len(h.sensor.requests)-1]; !strings.Contains(last, "cursor=at%3A2") {
		t.Errorf("second cycle asked %q, want it to resume from at:2", last)
	}
}

func (h *sensorHarness) saved(i int) model.HazardEvent {
	h.t.Helper()
	if i >= len(h.store.saved) {
		h.t.Fatalf("only %d hazards saved", len(h.store.saved))
	}
	return h.store.saved[i]
}

func TestSensorCycleWritesCriticalsFirstAcrossPages(t *testing.T) {
	h := newSensorHarness(t)
	h.poller.pageLimit = 2
	h.sensor.add(observation(0, ""), observation(1, ""), observation(2, ""),
		observation(3, `{"critical":true}`), observation(4, ""), observation(5, `{"critical":true}`))

	h.mustCycle()
	want := "obs-000003,obs-000005,obs-000000,obs-000001,obs-000002,obs-000004"
	if got := strings.Join(h.store.refs(), ","); got != want {
		t.Fatalf("write order %s, want %s", got, want)
	}
	if h.saved(0).Severity != model.SeverityAwas {
		t.Errorf("first write severity %s, want AWAS", h.saved(0).Severity)
	}
	if h.store.positions != 1 || h.store.position.Cursor != "at:6" {
		t.Errorf("position saved %d times as %+v, want once at at:6", h.store.positions, h.store.position)
	}
}

func TestSensorCycleParksViolationsAndDeliversUnverifiedCriticals(t *testing.T) {
	h := newSensorHarness(t)
	bad := observation(1, `{"wave_height_m":-3}`)
	badCritical := observation(2, `{"wave_height_m":-3,"critical":true}`)
	h.sensor.add(observation(0, ""), bad, badCritical)

	h.mustCycle()
	if got := strings.Join(h.store.refs(), ","); got != "obs-000002,obs-000000" {
		t.Fatalf("saved %s, want the unverified critical first, then obs-000000", got)
	}
	if v := h.saved(0).Attributes["verified"]; v != false {
		t.Errorf("unverified critical has verified=%#v", v)
	}
	if len(h.store.unprocessed) != 2 {
		t.Fatalf("parked %d records, want 2", len(h.store.unprocessed))
	}
	for i, wantPayload := range []string{badCritical, bad} {
		m := h.store.unprocessed[i]
		if m.Source != model.SourceSensorPantai || m.Status != model.StatusParked || m.Code != model.ReasonContractViolation {
			t.Errorf("parked[%d] = %s %s %s, want SENSOR_PANTAI parked contract_violation", i, m.Source, m.Status, m.Code)
		}
		if string(m.Payload) != wantPayload {
			t.Errorf("parked[%d] payload %s, want %s", i, m.Payload, wantPayload)
		}
	}
}

// restartSensor replaces the sensor's stream, as after a reschedule that starts sensor time again.
func (h *sensorHarness) restartSensor(generatedAt time.Duration, items ...string) {
	h.sensor.mu.Lock()
	defer h.sensor.mu.Unlock()
	h.sensor.items = items
	h.sensor.generatedAt = sensorStart.Add(generatedAt)
}

func TestSensorRestartIsReadFromTheStart(t *testing.T) {
	newCritical := observation(100, `{"critical":true}`)
	tests := []struct {
		name     string
		newRun   []string // the restarted sensor's stream when we next poll; our cursor is at:3
		distinct int      // distinct observations across both runs
	}{
		// at:3 is past the new run's end, so the sensor rejects it.
		{"old cursor rejected", []string{observation(0, ""), newCritical}, 4},
		// The new run is past offset 3, so at:3 is accepted and would skip newCritical; only
		// generated_at going backwards shows the restart.
		{"old cursor accepted, generated_at went backwards",
			[]string{observation(0, ""), newCritical, observation(1, ""), observation(2, ""), observation(3, "")}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newSensorHarness(t)
			h.sensor.add(observation(0, ""), observation(1, ""), observation(2, ""))
			h.mustCycle()

			h.restartSensor(2*time.Minute, tt.newRun...)
			if err := h.cycle(); err != nil {
				t.Fatalf("a restart must not fail the cycle: %v", err)
			}
			if !slices.Contains(h.store.refs(), "obs-000100") {
				t.Errorf("the new run's critical obs-000100 was not saved: %v", h.store.refs())
			}
			if n := len(h.store.saved); n != tt.distinct {
				t.Errorf("saved %d hazards %v, want %d (the replay deduplicated)", n, h.store.refs(), tt.distinct)
			}
			want := repository.StreamPosition{Cursor: "at:" + strconv.Itoa(len(tt.newRun)), GeneratedAt: sensorStart.Add(2 * time.Minute)}
			if h.store.position != want {
				t.Errorf("position %+v, want %+v", h.store.position, want)
			}
		})
	}
}

func TestSensorRepeatedObservationIdWithNewValuesIsParked(t *testing.T) {
	h := newSensorHarness(t)
	h.sensor.add(observation(5, ""))
	h.mustCycle()

	changed := observation(5, `{"wave_height_m":4.2,"critical":true}`)
	h.sensor.add(observation(5, ""), changed) // the sensor re-sends obs-000005, then sends it changed
	h.mustCycle()

	if len(h.store.saved) != 1 || h.saved(0).Attributes["wave_height_m"] != 1.1 {
		t.Fatalf("saved %v, want only the first obs-000005", h.store.refs())
	}
	if len(h.store.unprocessed) != 1 {
		t.Fatalf("parked %d, want only the changed one", len(h.store.unprocessed))
	}
	m := h.store.unprocessed[0]
	if string(m.Payload) != changed || m.Code != model.ReasonContractViolation || !strings.Contains(m.Reason, "id_conflict") {
		t.Errorf("parked %s %q %s, want the changed payload as contract_violation id_conflict", m.Code, m.Reason, m.Payload)
	}
}

func (h *sensorHarness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

func TestSensorWithoutStationListFailsAndParksNothing(t *testing.T) {
	h := newSensorHarness(t)
	h.sensor.stationsDown = true
	h.sensor.add(observation(0, ""))

	if err := h.cycle(); err == nil {
		t.Fatal("want the cycle to fail without a station list")
	}
	if len(h.store.saved)+len(h.store.unprocessed) != 0 || h.store.hasPosition {
		t.Errorf("saved %d, parked %d, position %v; want nothing written", len(h.store.saved), len(h.store.unprocessed), h.store.hasPosition)
	}
}

func TestSensorStationListRefreshesEveryFiveMinutesAndSurvivesFailure(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	h.advance(4 * time.Minute)
	h.mustCycle()
	if h.sensor.stationCalls != 1 {
		t.Fatalf("stations fetched %d times in 4 min, want 1", h.sensor.stationCalls)
	}

	h.advance(time.Minute)
	h.sensor.stationsDown = true
	h.sensor.add(observation(0, ""))
	h.mustCycle() // the refresh fails; the cached list is kept
	if h.sensor.stationCalls != 2 || len(h.store.saved) != 1 {
		t.Fatalf("stations fetched %d times, saved %d; want a refresh attempt at 5 min and the observation saved", h.sensor.stationCalls, len(h.store.saved))
	}
	h.mustCycle()
	if h.sensor.stationCalls != 2 {
		t.Errorf("stations fetched %d times, want no retry within 30 s of a failed refresh", h.sensor.stationCalls)
	}
	h.advance(30 * time.Second)
	h.mustCycle()
	if h.sensor.stationCalls != 3 {
		t.Errorf("stations fetched %d times, want a retry 30 s after the failed refresh", h.sensor.stationCalls)
	}
}

func TestSensorNewStationIsLearnedFromARefresh(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	h.advance(time.Minute)

	h.sensor.stations = append(h.sensor.stations, "stn-cilacap-01")
	h.sensor.add(observation(0, `{"station_id":"stn-cilacap-01"}`))
	h.mustCycle()
	if len(h.store.saved) != 1 || len(h.store.unprocessed) != 0 || h.sensor.stationCalls != 2 {
		t.Errorf("saved %d, parked %d, stations fetched %d; want the new station learned and its observation saved",
			len(h.store.saved), len(h.store.unprocessed), h.sensor.stationCalls)
	}
}

func TestSensorUnknownStationIsParkedAgainstAFreshList(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	h.advance(time.Minute)

	h.sensor.add(observation(0, `{"station_id":"stn-unknown-99"}`))
	h.mustCycle()
	h.sensor.add(observation(1, `{"station_id":"stn-unknown-98"}`))
	h.mustCycle()
	if len(h.store.unprocessed) != 2 || len(h.store.saved) != 0 {
		t.Fatalf("parked %d, saved %d; want both unknown-station observations parked", len(h.store.unprocessed), len(h.store.saved))
	}
	if h.sensor.stationCalls != 2 {
		t.Errorf("stations fetched %d times, want one refresh for both (at most once per 30 s)", h.sensor.stationCalls)
	}
}

func TestSensorUnknownStationIsHeldWhenTheListCannotBeRefreshed(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	before := h.store.position
	h.advance(time.Minute)

	h.sensor.stationsDown = true
	h.sensor.add(observation(0, `{"station_id":"stn-maybe-new"}`), observation(1, `{"critical":true}`))
	if err := h.cycle(); err == nil {
		t.Error("want the cycle to fail while an observation is held")
	}
	if got := h.store.refs(); len(got) != 1 || got[0] != "obs-000001" {
		t.Errorf("saved %v, want the known station's critical delivered anyway", got)
	}
	if len(h.store.unprocessed) != 0 {
		t.Errorf("parked %d, want the unknown station held, not parked", len(h.store.unprocessed))
	}
	if h.store.position != before {
		t.Errorf("position moved to %+v, want it kept at %+v so the held observation is read again", h.store.position, before)
	}
}

func TestSensorFailedWriteKeepsThePosition(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	before := h.store.position

	h.store.saveErr = fmt.Errorf("save hazard: %w: epoch 3", fence.ErrStaleEpoch)
	h.sensor.add(observation(0, ""))
	if err := h.cycle(); err == nil {
		t.Fatal("want a lost-leadership write to fail the cycle")
	}
	if h.store.position != before || h.store.positions != 1 {
		t.Errorf("position %+v after %d saves, want it kept at %+v", h.store.position, h.store.positions, before)
	}
}

func TestSensorUnavailableStoreParksForRetryAndMovesOn(t *testing.T) {
	h := newSensorHarness(t)
	h.store.saveErr = fmt.Errorf("save hazard: %w: range unavailable", repository.ErrUnavailable)
	h.sensor.add(observation(0, `{"critical":true}`))

	h.mustCycle()
	if len(h.store.unprocessed) != 1 || h.store.unprocessed[0].Status != model.StatusRetrying {
		t.Fatalf("unprocessed %+v, want the critical queued for retry", h.store.unprocessed)
	}
	if h.store.position.Cursor != "at:1" {
		t.Errorf("position %+v, want it moved past the retried observation", h.store.position)
	}
}

func TestSensorCycleReadsAtMostMaxPages(t *testing.T) {
	h := newSensorHarness(t)
	h.poller.pageLimit, h.poller.maxPages = 2, 3
	for i := range 9 {
		h.sensor.add(observation(i, ""))
	}
	h.mustCycle()
	if len(h.store.saved) != 6 || h.store.position.Cursor != "at:6" {
		t.Fatalf("saved %d, position %+v; want 3 pages of 2 and the cursor after them", len(h.store.saved), h.store.position)
	}
	h.mustCycle()
	if len(h.store.saved) != 9 {
		t.Errorf("saved %d after the next cycle, want the rest", len(h.store.saved))
	}
}

func TestSensorResponseWithoutGeneratedAtFailsTheCycle(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	before := h.store.position
	h.sensor.generatedAt = time.Time{}
	h.sensor.add(observation(0, `{"critical":true}`))
	h.sensor.mu.Lock()
	h.sensor.omitGeneratedAt = true
	h.sensor.mu.Unlock()

	if err := h.cycle(); err == nil {
		t.Fatal("want a response without generated_at to fail the cycle")
	}
	if len(h.store.saved) != 0 || h.store.position != before {
		t.Errorf("saved %d, position %+v; want no observation stored and the position kept", len(h.store.saved), h.store.position)
	}
	if len(h.store.unprocessed) != 1 || h.store.unprocessed[0].Code != model.ReasonContractViolation {
		t.Errorf("parked %+v, want the whole response parked as contract_violation", h.store.unprocessed)
	}
}
