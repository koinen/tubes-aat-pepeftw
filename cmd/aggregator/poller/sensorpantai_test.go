package poller

import (
	"context"
	"encoding/json"
	"errors"
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
	mu               sync.Mutex
	items            []string
	generatedAt      time.Time
	stations         []string
	stationsDown     bool
	observationsDown bool
	// omitGeneratedAt leaves generated_at out of observation responses.
	omitGeneratedAt bool
	requests        []string // query strings of observation requests, in order
	stationCalls    int
	healthCalls     int
	healthDown      bool
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
	case "/healthz":
		s.healthCalls++
		if s.healthDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
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
		if s.observationsDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
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
	positionErr error
	status      model.SourceStatus
	rejectRef   string // SaveHazard and any batch holding this source_ref_id fail with ErrRejected
	singles     int    // SaveHazard calls
	batches     int    // SaveHazards calls
}

func (s *sensorStore) SaveSourceStatus(_ context.Context, status model.SourceStatus) error {
	s.status = status
	return nil
}

func (s *sensorStore) ListSourceStatus(context.Context) ([]model.SourceStatus, error) {
	return []model.SourceStatus{s.status}, nil
}

func (s *sensorStore) SaveHazard(_ context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	s.singles++
	return s.save(h)
}

func (s *sensorStore) SaveHazards(_ context.Context, hs []model.HazardEvent) ([]repository.SaveOutcome, error) {
	s.batches++
	for _, h := range hs {
		if s.saveErr == nil && h.SourceRefID == s.rejectRef {
			return nil, fmt.Errorf("save hazards: %w: check constraint", repository.ErrRejected)
		}
	}
	var out []repository.SaveOutcome
	for _, h := range hs {
		o, err := s.save(h)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func (s *sensorStore) GetHazards(ctx context.Context, ids []string) (map[string]model.HazardEvent, error) {
	out := map[string]model.HazardEvent{}
	for _, id := range ids {
		if h, err := s.GetHazard(ctx, id); err == nil {
			out[id] = h
		}
	}
	return out, nil
}

func (s *sensorStore) save(h model.HazardEvent) (repository.SaveOutcome, error) {
	if s.saveErr != nil {
		return "", s.saveErr
	}
	if h.SourceRefID == s.rejectRef {
		return "", fmt.Errorf("save hazard: %w: check constraint", repository.ErrRejected)
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
	if s.positionErr != nil {
		return s.positionErr
	}
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
	deps := Deps{Hazards: h.store, Batch: h.store, Unprocessed: h.store, Positions: h.store, Log: log, Now: now}
	h.poller = newSensorPantaiPoller(f, deps)
	return h
}

func (h *sensorHarness) cycle() error {
	h.t.Helper()
	result := h.poller.cycle(correlation.With(context.Background(), "cid-sensor"))
	return errors.Join(result.sourceErr, result.localErr)
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

func sensorRunner(t *testing.T) (*fakeSensor, *sensorStore, func(time.Duration)) {
	t.Helper()
	sensor, srv := newFakeSensor(t)
	store := &sensorStore{}
	clock := sensorStart
	p := New(Config{SensorPantaiURL: srv.URL, SensorPantaiTimeout: time.Second,
		BreakerFailures: 3, BreakerOpenFor: 15 * time.Second}, Deps{
		Hazards: store, Batch: store, Unprocessed: store, Positions: store, Statuses: store,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return clock },
	})
	for _, r := range p.runners {
		if r.source == model.SourceSensorPantai {
			return sensor, store, func(elapsed time.Duration) {
				clock = clock.Add(elapsed)
				r.tick(context.Background())
			}
		}
	}
	t.Fatal("no Sensor Pantai runner")
	return nil, nil, nil
}

func TestSensorBreakerLimitsRequestsAndRecovers(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	sensor.add(observation(0, ""))
	tick(0)
	firstSuccess := *store.status.LastSuccessAt
	sensor.observationsDown = true
	count := len(sensor.requests)
	for i := 1; i <= 5; i++ {
		tick(time.Second)
		if len(sensor.requests) != count+i {
			t.Fatalf("failure %d: got %d requests since outage, want %d", i, len(sensor.requests)-count, i)
		}
	}
	if store.status.BreakerState != model.BreakerOpen || store.status.Status != model.StatusStale {
		t.Fatalf("outage status: %+v", store.status)
	}
	count += 5
	for _, wait := range []time.Duration{5, 10, 20, 40, 60, 60} {
		tick((wait - 1) * time.Second)
		if len(sensor.requests) != count {
			t.Fatalf("request sent before %ds cooldown elapsed", wait)
		}
		tick(time.Second)
		count++
		if len(sensor.requests) != count || sensor.requests[count-1] != "limit=1" {
			t.Fatalf("expected one cursor-free probe after %ds, got %v", wait, sensor.requests)
		}
	}

	sensor.observationsDown = false
	sensor.generatedAt = sensor.generatedAt.Add(time.Minute)
	sensor.add(observation(1, ""))
	tick(time.Minute)
	if len(sensor.requests) != count+1 || sensor.requests[count] != "limit=1" ||
		store.status.BreakerState != model.BreakerClosed || store.status.Status != model.StatusStale ||
		!store.status.LastSuccessAt.Equal(firstSuccess) || len(store.saved) != 1 {
		t.Fatalf("probe should close breaker without ingesting: requests %v, status %+v, saved %v",
			sensor.requests, store.status, store.refs())
	}
	tick(time.Second)
	if store.status.Status != model.StatusOK || len(store.saved) != 2 {
		t.Fatalf("normal polling did not resume: status %+v, saved %v", store.status, store.refs())
	}
}

func TestSensorStationSilenceDoesNotTripBreaker(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	sensor.stations = append(sensor.stations, "stn-cilacap-01")
	sensor.generatedAt = sensorStart.Add(30 * time.Second)
	sensor.add(observation(0, ""), observation(1, `{"wave_height_m":-1}`),
		observation(0, `{"observation_id":"cilacap-0","station_id":"stn-cilacap-01"}`))
	readSilence := func() map[string]any {
		t.Helper()
		raw, err := json.Marshal(store.status)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			StationSilence map[string]any `json:"station_silence"`
		}
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		return snapshot.StationSilence
	}
	tick(0)
	silence := readSilence()
	if unknown, ok := silence["stn-cilacap-01"]; !ok || unknown != nil || silence["stn-padang-01"] != false {
		t.Fatalf("first status should distinguish unknown and not silent: %v", silence)
	}
	sensor.generatedAt = sensorStart.Add(166 * time.Second)
	sensor.add(observation(1, ""), observation(0, ""),
		observation(10, `{"observation_id":"cilacap-1","station_id":"stn-cilacap-01"}`))
	tick(time.Second)
	silence = readSilence()
	if silence["stn-padang-01"] != true || silence["stn-cilacap-01"] != false ||
		store.status.BreakerState != model.BreakerClosed || store.status.Status != model.StatusOK {
		t.Fatalf("one silent station should not make the source unhealthy: %v %+v", silence, store.status)
	}
	sensor.observationsDown = true
	for range 5 {
		tick(time.Second)
	}
	if silence := readSilence(); silence["stn-padang-01"] != nil || len(silence) != 2 {
		t.Fatalf("open breaker should make station silence unknown: %v", silence)
	}
}

func TestSensorStalledClockOpensBreakerAndEqualProbeDoesNotCloseIt(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	tick(0)
	for repeat := 1; repeat <= 7; repeat++ {
		tick(time.Second)
		if repeat <= 2 && store.status.Status != model.StatusOK {
			t.Fatalf("repeat %d should be tolerated: %+v", repeat, store.status)
		}
		if repeat >= 3 && store.status.Status != model.StatusStale {
			t.Fatalf("repeat %d should mark source stale: %+v", repeat, store.status)
		}
		if repeat < 7 && store.status.BreakerState != model.BreakerClosed {
			t.Fatalf("repeat %d opened breaker early: %+v", repeat, store.status)
		}
	}
	if store.status.BreakerState != model.BreakerOpen {
		t.Fatalf("seventh repeat should open breaker: %+v", store.status)
	}
	tick(5 * time.Second)
	if store.status.BreakerState != model.BreakerOpen {
		t.Fatalf("frozen clock should fail recovery probe: %+v", store.status)
	}
	sensor.generatedAt = sensorStart
	tick(10 * time.Second)
	if store.status.BreakerState != model.BreakerClosed {
		t.Fatalf("sensor restart should recover the breaker: %+v", store.status)
	}
}

func TestSensorStallCountsCompletePollsRatherThanPages(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	tick(0)
	for i := range 2 {
		sensor.add(observation(i, ""))
		tick(time.Second) // one observation page and one empty page at the same sensor time
		if store.status.Status != model.StatusOK {
			t.Fatalf("repeat %d counted individual pages as clock samples: %+v", i+1, store.status)
		}
	}
	sensor.observationsDown = true
	tick(time.Second)
	sensor.observationsDown = false
	tick(time.Second)
	if !strings.Contains(store.status.LastError, "generated_at did not advance") {
		t.Fatalf("failed fetch unexpectedly cleared the stall streak: %+v", store.status)
	}
}

func TestSensorLocalCursorWriteFailureDoesNotOpenBreaker(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	tick(0)
	store.positionErr = fmt.Errorf("database write failed")
	for range 6 {
		sensor.generatedAt = sensor.generatedAt.Add(time.Minute)
		tick(time.Second)
	}
	if store.status.Status != model.StatusStale || store.status.BreakerState != model.BreakerClosed {
		t.Fatalf("local write failure should only make ingestion stale: %+v", store.status)
	}
}

func TestSensorReadinessIsAdvisoryAndStopsWhileBreakerIsOpen(t *testing.T) {
	sensor, store, tick := sensorRunner(t)
	sensor.healthDown = true
	tick(0)
	if sensor.healthCalls != 1 || store.status.Status != model.StatusOK {
		t.Fatalf("healthz should not make ingestion unhealthy: calls=%d status=%+v", sensor.healthCalls, store.status)
	}
	sensor.healthDown = false
	sensor.observationsDown = true
	for range 5 {
		tick(time.Second)
	}
	tick(29 * time.Second)
	if store.status.BreakerState != model.BreakerOpen || sensor.healthCalls != 1 {
		t.Fatalf("healthz should not run when open or probing: calls=%d status=%+v", sensor.healthCalls, store.status)
	}
	sensor.observationsDown = false
	sensor.generatedAt = sensor.generatedAt.Add(time.Minute)
	tick(10 * time.Second)
	if sensor.healthCalls != 1 || store.status.BreakerState != model.BreakerClosed {
		t.Fatalf("recovery should send only its probe: calls=%d status=%+v", sensor.healthCalls, store.status)
	}
	tick(time.Second)
	if sensor.healthCalls != 2 || store.status.Status != model.StatusOK {
		t.Fatalf("normal polling should resume readiness sampling: calls=%d status=%+v", sensor.healthCalls, store.status)
	}
}

type isolationStore struct{ sensorStore }

func (*isolationStore) GetCursor(context.Context, model.Source, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (*isolationStore) SetCursor(context.Context, model.Source, string, time.Time) error {
	return nil
}

func (*isolationStore) ListSourceStatus(context.Context) ([]model.SourceStatus, error) {
	return nil, nil
}

func (*isolationStore) SaveSourceStatus(context.Context, model.SourceStatus) error { return nil }

func (*isolationStore) ListVolcanoRefs(context.Context) ([]model.VolcanoRef, error) { return nil, nil }

func TestSensorBlockedRequestDoesNotBlockBMKG(t *testing.T) {
	blocked := make(chan struct{})
	sensor := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(blocked)
		<-r.Context().Done()
	}))
	defer sensor.Close()
	progress := make(chan struct{}, 1)
	bmkg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-blocked:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `[]`)
		if r.URL.Path == "/seismic-events" {
			progress <- struct{}{}
		}
	}))
	defer bmkg.Close()
	pvmbg := httptest.NewServer(http.NotFoundHandler())
	defer pvmbg.Close()
	store := &isolationStore{}
	p := New(Config{BMKGURL: bmkg.URL, PVMBGURL: pvmbg.URL, SensorPantaiURL: sensor.URL,
		BMKGTimeout: 5 * time.Second, PVMBGTimeout: time.Second, SensorPantaiTimeout: 5 * time.Second,
		PollInterval: time.Hour, SensorPantaiPollInterval: time.Hour, BreakerFailures: 3}, Deps{
		Hazards: store, Batch: store, Unprocessed: store, Positions: store, Statuses: store, Cursors: store, Volcanoes: store,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-progress:
	case <-time.After(2 * time.Second):
		t.Fatal("BMKG did not reach its seismic endpoint while Sensor Pantai was blocked")
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

func (h *sensorHarness) advance(d time.Duration) {
	h.clock = h.clock.Add(d)
	h.sensor.generatedAt = h.sensor.generatedAt.Add(d)
}

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

// An unavailable store fails the cycle before the cursor moves, so the next poll writes the same
// observations again. Nothing is parked: the retry queue lives in the same database.
func TestSensorUnavailableStoreKeepsThePositionAndRetriesNextPoll(t *testing.T) {
	h := newSensorHarness(t)
	h.mustCycle()
	before := h.store.position
	h.store.saveErr = fmt.Errorf("save hazards: %w: range unavailable", repository.ErrUnavailable)
	h.sensor.add(observation(0, `{"critical":true}`))

	if err := h.cycle(); err == nil {
		t.Fatal("want an unavailable store to fail the cycle")
	}
	if h.store.position != before || len(h.store.unprocessed) != 0 {
		t.Fatalf("position %+v, parked %d; want the position kept at %+v and nothing parked",
			h.store.position, len(h.store.unprocessed), before)
	}
	h.store.saveErr = nil
	h.mustCycle()
	if got := strings.Join(h.store.refs(), ","); got != "obs-000000" || h.store.position.Cursor != "at:1" {
		t.Errorf("after recovery saved %s at %+v, want obs-000000 and the cursor past it", got, h.store.position)
	}
}

// A backlog of thousands of observations (the sensor at 60x after a slow cycle) must cost a few
// transactions, not one per observation: at 3-5 s per transaction on a slow cluster, per-row writes
// made one cycle last over an hour (P5 practice game day, 2026-11-22).
func TestSensorBacklogIsWrittenInBatchesWithCriticalsFirst(t *testing.T) {
	h := newSensorHarness(t)
	h.poller.batchSize = 200
	h.sensor.generatedAt = sensorStart.Add(2 * time.Hour) // after the last observed_at
	for i := range 450 {
		extra := ""
		if i == 333 {
			extra = `{"critical":true}`
		}
		h.sensor.add(observation(i, extra))
	}
	h.mustCycle()

	if len(h.store.saved) != 450 || h.saved(0).SourceRefID != "obs-000333" {
		t.Fatalf("saved %d, first %s; want all 450 with the critical first", len(h.store.saved), h.saved(0).SourceRefID)
	}
	// One batch for the critical, then ceil(449/200) for the rest.
	if h.store.batches != 4 || h.store.singles != 0 {
		t.Errorf("%d batches and %d single writes, want 4 batches and none", h.store.batches, h.store.singles)
	}
}

func TestSensorManyCriticalsAreSplitIntoBatchesToo(t *testing.T) {
	h := newSensorHarness(t)
	h.poller.batchSize = 2
	for i := range 5 {
		h.sensor.add(observation(i, `{"critical":true}`))
	}
	h.sensor.add(observation(5, ""))
	h.mustCycle()
	if len(h.store.saved) != 6 || h.store.batches != 4 {
		t.Errorf("saved %d in %d batches, want 6 in 4 (criticals 2+2+1, then the rest)", len(h.store.saved), h.store.batches)
	}
}

// A batch the database rejects is written again row by row, so only the bad row is parked.
func TestSensorRejectedBatchFallsBackToRowsAndParksOnlyTheBadOne(t *testing.T) {
	h := newSensorHarness(t)
	h.store.rejectRef = "obs-000001"
	h.sensor.add(observation(0, ""), observation(1, ""), observation(2, ""))
	h.mustCycle()

	if got := strings.Join(h.store.refs(), ","); got != "obs-000000,obs-000002" {
		t.Fatalf("saved %s, want obs-000000,obs-000002", got)
	}
	if len(h.store.unprocessed) != 1 || h.store.unprocessed[0].SourceRefID != "obs-000001" ||
		h.store.unprocessed[0].Status != model.StatusParked {
		t.Fatalf("unprocessed %+v, want obs-000001 parked", h.store.unprocessed)
	}
	if h.store.position.Cursor != "at:3" {
		t.Errorf("position %+v, want it past the batch", h.store.position)
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
