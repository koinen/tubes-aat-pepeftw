package poller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/repository/memory"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type rejectedSpy struct {
	mu   sync.Mutex
	list []model.RejectedRecord
}

func (r *rejectedSpy) SaveRejected(_ context.Context, rec model.RejectedRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, rec)
	return nil
}

type volcanoRefs struct{}

func (volcanoRefs) ListVolcanoRefs(context.Context) ([]model.VolcanoRef, error) {
	return []model.VolcanoRef{{VolcanoID: "MERAPI", Name: "Merapi", Latitude: -7.540, Longitude: 110.446}}, nil
}

type fakeSource struct {
	mu        sync.Mutex
	responses map[string]string
	status    int
	delay     time.Duration
	hits      atomic.Int64
	lastReq   *http.Request
}

func (s *fakeSource) set(path, body string) { s.mu.Lock(); s.responses[path] = body; s.mu.Unlock() }

func (s *fakeSource) setStatus(code int)         { s.mu.Lock(); s.status = code; s.mu.Unlock() }
func (s *fakeSource) setDelay(d time.Duration)   { s.mu.Lock(); s.delay = d; s.mu.Unlock() }
func (s *fakeSource) lastRequest() *http.Request { s.mu.Lock(); defer s.mu.Unlock(); return s.lastReq }

func (s *fakeSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.hits.Add(1)
	s.mu.Lock()
	body, status, delay := s.responses[r.URL.Path], s.status, s.delay
	s.lastReq = r.Clone(context.Background())
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, body)
}

type env struct {
	repo     *memory.MemoryRepository
	rejected *rejectedSpy
	clock    *clock
	bmkg     *fakeSource
	pvmbg    *fakeSource
	cfg      Config
	deps     Deps
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		repo:     memory.New(),
		rejected: &rejectedSpy{},
		clock:    &clock{t: time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC)},
		bmkg:     &fakeSource{responses: map[string]string{"/seismic-events": "[]", "/tsunami-warnings": "[]"}},
		pvmbg:    &fakeSource{responses: map[string]string{"/volcanic-reports": `{"reports":[]}`}},
	}
	bs, ps := httptest.NewServer(e.bmkg), httptest.NewServer(e.pvmbg)
	t.Cleanup(func() { bs.Close(); ps.Close() })
	e.cfg = Config{
		Enabled: true, BMKGURL: bs.URL, BMKGAPIKey: "bmkg-key", PVMBGURL: ps.URL, PVMBGToken: "pvmbg.x.y",
		PollInterval: 20 * time.Millisecond, BMKGTimeout: time.Second, PVMBGTimeout: 200 * time.Millisecond,
		CursorOverlap: time.Minute, BreakerFailures: 3, BreakerOpenFor: 15 * time.Second, TsunamiGrace: 30 * time.Second,
	}
	e.deps = Deps{
		Hazards: e.repo, Rejected: e.rejected, Cursors: e.repo, Statuses: e.repo, Volcanoes: volcanoRefs{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: e.clock.Now,
	}
	return e
}

func (e *env) bmkgPoller() (*bmkgPoller, *runner) {
	f := newFetcher(model.SourceBMKG, e.cfg.BMKGURL, e.cfg.BMKGTimeout,
		func(r *http.Request) { r.Header.Set("X-BMKG-Key", e.cfg.BMKGAPIKey) }, e.deps.Log, e.clock.Now)
	p := newBMKGPoller(f, e.deps, e.cfg)
	return p, &runner{source: model.SourceBMKG, interval: e.cfg.PollInterval, breaker: NewBreaker(3, 15*time.Second),
		statuses: e.repo, cycle: p.cycle, log: e.deps.Log, now: e.clock.Now}
}

func (e *env) pvmbgRunner() *runner {
	f := newFetcher(model.SourcePVMBG, e.cfg.PVMBGURL, e.cfg.PVMBGTimeout,
		func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+e.cfg.PVMBGToken) }, e.deps.Log, e.clock.Now)
	p := newPVMBGPoller(f, e.deps, e.cfg)
	return &runner{source: model.SourcePVMBG, interval: e.cfg.PollInterval, breaker: NewBreaker(3, 15*time.Second),
		statuses: e.repo, cycle: p.cycle, log: e.deps.Log, now: e.clock.Now}
}

func (e *env) hazard(t *testing.T, source model.Source, ref string) (model.HazardEvent, bool) {
	t.Helper()
	h, err := e.repo.GetHazard(context.Background(), mapper.HazardID(source, ref))
	return h, err == nil
}

func (e *env) status(t *testing.T, source model.Source) model.SourceStatus {
	t.Helper()
	list, _ := e.repo.ListSourceStatus(context.Background())
	for _, s := range list {
		if s.Source == source {
			return s
		}
	}
	t.Fatalf("status %s belum disimpan", source)
	return model.SourceStatus{}
}

func event(id string, mag float64, tsunami bool, at string) map[string]any {
	return map[string]any{"event_id": id, "magnitude": mag, "depth_km": 10.0, "epicenter_lat": -6.4,
		"epicenter_lon": 105.0, "region_name": "Selat Sunda", "occurred_at": at, "potential_tsunami": tsunami}
}

func warning(id, eventID, level string) map[string]any {
	return map[string]any{"warning_id": id, "related_event_id": eventID, "threat_level": level,
		"affected_zones": []string{"Pandeglang"}, "estimated_arrival": "2026-10-05T01:30:00Z"}
}

func js(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestBMKGSavesEventsWithTsunamiCorrelation(t *testing.T) {
	e := newEnv(t)
	e.bmkg.set("/seismic-events", js([]any{
		event("E1", 4.2, false, "2026-10-05T00:50:00Z"),
		event("E2", 7.1, true, "2026-10-05T00:55:00Z"),
	}))
	e.bmkg.set("/tsunami-warnings", js([]any{warning("W2", "E2", "Awas")}))
	_, r := e.bmkgPoller()
	r.tick(context.Background())

	h1, ok1 := e.hazard(t, model.SourceBMKG, "E1")
	h2, ok2 := e.hazard(t, model.SourceBMKG, "E2")
	if !ok1 || !ok2 {
		t.Fatal("kedua gempa harus tersimpan")
	}
	if h1.Severity != model.SeverityNormal || h2.Severity != model.SeverityAwas {
		t.Errorf("severity E1=%s (ingin NORMAL), E2=%s (ingin AWAS dari warning)", h1.Severity, h2.Severity)
	}
	if _, ok := h2.Attributes["tsunami_warnings"]; !ok {
		t.Error("warning harus tersalin ke attributes gempa")
	}
	if st := e.status(t, model.SourceBMKG); st.Status != model.StatusOK || st.BreakerState != model.BreakerClosed || st.StaleSince != nil {
		t.Errorf("status BMKG harus OK/CLOSED, dapat %+v", st)
	}
	cur, found, _ := e.repo.GetCursor(context.Background(), model.SourceBMKG, mapper.EndpointSeismicEvents)
	if !found || !cur.Equal(time.Date(2026, 10, 5, 0, 55, 0, 0, time.UTC)) {
		t.Errorf("cursor harus = occurred_at terbaru, dapat %v", cur)
	}
	if got := e.bmkg.lastRequest().Header.Get("X-BMKG-Key"); got != "bmkg-key" {
		t.Errorf("header X-BMKG-Key tidak dikirim, dapat %q", got)
	}
	if e.bmkg.lastRequest().Header.Get("X-Correlation-ID") == "" {
		t.Error("X-Correlation-ID harus diteruskan ke sumber")
	}

	r.tick(context.Background())
	if since := e.bmkg.lastRequest().URL.Query().Get("since"); since != "2026-10-05T00:54:00Z" {
		t.Errorf("since siklus kedua %q, ingin cursor - 1 menit", since)
	}
	all, _ := e.repo.ListHazards(context.Background(), repository.HazardFilter{})
	if len(all) != 2 {
		t.Errorf("data ulang tidak boleh tersimpan ganda, dapat %d hazard", len(all))
	}
}

func TestBMKGHoldsTsunamiEventUntilWarningArrives(t *testing.T) {
	e := newEnv(t)
	e.bmkg.set("/seismic-events", js([]any{event("E9", 7.4, true, "2026-10-05T00:59:00Z")}))
	p, r := e.bmkgPoller()

	r.tick(context.Background())
	if _, ok := e.hazard(t, model.SourceBMKG, "E9"); ok {
		t.Fatal("gempa berpotensi tsunami tanpa warning harus ditahan dulu")
	}
	if _, ok := p.pending["E9"]; !ok {
		t.Fatal("gempa harus tercatat sebagai pending")
	}

	e.clock.Add(5 * time.Second)
	e.bmkg.set("/tsunami-warnings", js([]any{warning("W9", "E9", "Waspada")}))
	r.tick(context.Background())
	h, ok := e.hazard(t, model.SourceBMKG, "E9")
	if !ok || h.Severity != model.SeverityWaspada {
		t.Fatalf("setelah warning datang, gempa harus tersimpan dengan severity WASPADA, dapat %v %s", ok, h.Severity)
	}
	if len(p.pending) != 0 {
		t.Error("pending harus kosong setelah tersimpan")
	}
}

func TestBMKGReleasesHeldEventAfterGrace(t *testing.T) {
	e := newEnv(t)
	e.bmkg.set("/seismic-events", js([]any{event("E7", 6.8, true, "2026-10-05T00:59:00Z")}))
	_, r := e.bmkgPoller()

	r.tick(context.Background())
	cur, _, _ := e.repo.GetCursor(context.Background(), model.SourceBMKG, mapper.EndpointSeismicEvents)
	if !cur.Equal(time.Date(2026, 10, 5, 0, 59, 0, 0, time.UTC)) {
		t.Errorf("cursor tidak boleh melewati gempa pending, dapat %v", cur)
	}

	e.clock.Add(31 * time.Second)
	r.tick(context.Background())
	h, ok := e.hazard(t, model.SourceBMKG, "E7")
	if !ok || h.Severity != model.SeveritySiaga {
		t.Fatalf("setelah grace, gempa disimpan dengan severity dari magnitudo (SIAGA), dapat %v %s", ok, h.Severity)
	}
}

func TestBMKGRejectsBrokenRecords(t *testing.T) {
	e := newEnv(t)
	bad := event("E5", 5.5, false, "2026-10-05T00:58:00Z")
	delete(bad, "magnitude")
	e.bmkg.set("/seismic-events", js([]any{bad, event("E6", 5.5, false, "2026-10-05T00:58:30Z")}))
	e.bmkg.set("/tsunami-warnings", js([]any{warning("W5", "E5", "Bahaya")}))
	_, r := e.bmkgPoller()
	r.tick(context.Background())

	if len(e.rejected.list) != 2 {
		t.Fatalf("1 gempa + 1 warning rusak harus masuk rejected_records, dapat %d", len(e.rejected.list))
	}
	if _, ok := e.hazard(t, model.SourceBMKG, "E6"); !ok {
		t.Error("record rusak tidak boleh menghentikan record lain")
	}
	if st := e.status(t, model.SourceBMKG); st.Status != model.StatusOK {
		t.Errorf("record rusak bukan kegagalan sumber; status harus OK, dapat %s", st.Status)
	}
}

func TestPVMBGSavesReportsAndSchemaEvolution(t *testing.T) {
	e := newEnv(t)
	e.pvmbg.set("/volcanic-reports", js(map[string]any{"generated_at": "2026-10-05T01:00:00Z", "reports": []any{
		map[string]any{"report_id": "R1", "volcano_id": "MERAPI", "alert_level": "Siaga", "eruption_count_24h": 4,
			"ash_column_height_m": 900, "reported_at": "2026-10-05T00:58:00Z"},
		map[string]any{"report_id": "R2", "volcano_id": "MERAPI", "alert_level": "Awas", "eruption_count_24h": 20,
			"ash_column_height_m": 3000, "reported_at": "2026-10-05T00:59:00Z", "confidence_level": 0.81},
		map[string]any{"report_id": "R3", "volcano_id": "TIDAK_ADA", "alert_level": "Normal", "eruption_count_24h": 0,
			"ash_column_height_m": 0, "reported_at": "2026-10-05T00:59:30Z"},
	}}))
	r := e.pvmbgRunner()
	r.tick(context.Background())

	h1, ok1 := e.hazard(t, model.SourcePVMBG, "R1")
	h2, ok2 := e.hazard(t, model.SourcePVMBG, "R2")
	if !ok1 || !ok2 {
		t.Fatal("laporan valid harus tersimpan")
	}
	if _, has := h1.Attributes["confidence_level"]; has {
		t.Error("laporan schema v1 tidak punya confidence_level")
	}
	if h2.Attributes["confidence_level"] != 0.81 || h2.AreaName != "Merapi" {
		t.Errorf("laporan schema v2: confidence_level harus di attributes, dapat %v", h2.Attributes)
	}
	if len(e.rejected.list) != 1 || e.rejected.list[0].SourceRefID != "R3" {
		t.Errorf("volcano_id tak dikenal harus masuk rejected_records, dapat %+v", e.rejected.list)
	}
	if got := e.pvmbg.lastRequest().Header.Get("Authorization"); got != "Bearer pvmbg.x.y" {
		t.Errorf("header Authorization salah: %q", got)
	}
}

func TestPVMBGOutageStatusAndBreaker(t *testing.T) {
	e := newEnv(t)
	r := e.pvmbgRunner()

	e.pvmbg.setStatus(http.StatusServiceUnavailable)
	r.tick(context.Background())
	if st := e.status(t, model.SourcePVMBG); st.Status != model.StatusUnavailable {
		t.Fatalf("gagal sebelum pernah berhasil harus UNAVAILABLE, dapat %s", st.Status)
	}

	e.pvmbg.setStatus(0)
	e.clock.Add(time.Second)
	r.tick(context.Background())
	okAt := e.clock.Now().Truncate(time.Second)
	if st := e.status(t, model.SourcePVMBG); st.Status != model.StatusOK || st.LastSuccessAt == nil {
		t.Fatalf("setelah berhasil harus OK, dapat %+v", st)
	}

	e.pvmbg.setDelay(time.Second)
	for i := 0; i < 3; i++ {
		e.clock.Add(3 * time.Second)
		start := time.Now()
		r.tick(context.Background())
		if time.Since(start) > 900*time.Millisecond {
			t.Fatal("request ke PVMBG yang menggantung harus dihentikan oleh timeout")
		}
	}
	st := e.status(t, model.SourcePVMBG)
	if st.Status != model.StatusStale || st.StaleSince == nil || !st.StaleSince.Equal(okAt) || st.BreakerState != model.BreakerOpen {
		t.Fatalf("setelah 3 kegagalan: ingin STALE, stale_since=%v, OPEN; dapat %+v", okAt, st)
	}

	hits := e.pvmbg.hits.Load()
	e.clock.Add(5 * time.Second)
	r.tick(context.Background())
	if e.pvmbg.hits.Load() != hits {
		t.Error("breaker OPEN: PVMBG tidak boleh dipanggil")
	}

	e.pvmbg.setDelay(0)
	e.clock.Add(15 * time.Second)
	r.tick(context.Background())
	if st := e.status(t, model.SourcePVMBG); st.Status != model.StatusOK || st.BreakerState != model.BreakerClosed || st.StaleSince != nil {
		t.Fatalf("setelah pulih harus OK/CLOSED tanpa stale_since, dapat %+v", st)
	}
}

func TestSlowPVMBGDoesNotBlockBMKG(t *testing.T) {
	e := newEnv(t)
	e.deps.Now = time.Now
	e.pvmbg.setDelay(5 * time.Second)
	e.bmkg.set("/seismic-events", js([]any{event("E1", 4.0, false, "2026-10-05T00:50:00Z")}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { New(e.cfg, e.deps).Run(ctx); close(done) }()

	time.Sleep(700 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Poller harus berhenti saat context dibatalkan")
	}
	if n := e.bmkg.hits.Load(); n < 10 {
		t.Errorf("BMKG harus terus dipoll selama PVMBG menggantung; hanya %d request", n)
	}
	if _, ok := e.hazard(t, model.SourceBMKG, "E1"); !ok {
		t.Error("data BMKG harus tersimpan walaupun PVMBG menggantung")
	}
}

func TestBreakerTransitions(t *testing.T) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	b := NewBreaker(2, 10*time.Second)
	if !b.Allow(now) || b.State() != model.BreakerClosed {
		t.Fatal("awal harus CLOSED")
	}
	b.Failure(now)
	if b.State() != model.BreakerClosed {
		t.Fatal("satu kegagalan belum membuka breaker")
	}
	b.Failure(now)
	if b.State() != model.BreakerOpen || b.Allow(now.Add(5*time.Second)) {
		t.Fatal("dua kegagalan -> OPEN dan menolak panggilan")
	}
	if !b.Allow(now.Add(10*time.Second)) || b.State() != model.BreakerHalfOpen {
		t.Fatal("setelah OpenFor -> HALF_OPEN dan mengizinkan satu percobaan")
	}
	b.Failure(now.Add(10 * time.Second))
	if b.State() != model.BreakerOpen {
		t.Fatal("gagal saat HALF_OPEN -> OPEN lagi")
	}
	b.Allow(now.Add(20 * time.Second))
	b.Success()
	if b.State() != model.BreakerClosed {
		t.Fatal("berhasil saat HALF_OPEN -> CLOSED")
	}
}

type hazardSpy struct {
	*memory.MemoryRepository
	mu   sync.Mutex
	cids []string
}

func (s *hazardSpy) SaveHazard(ctx context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	s.mu.Lock()
	s.cids = append(s.cids, correlation.From(ctx))
	s.mu.Unlock()
	return s.MemoryRepository.SaveHazard(ctx, h)
}

func TestCorrelationIDReachesOutbox(t *testing.T) {
	e := newEnv(t)
	spy := &hazardSpy{MemoryRepository: e.repo}
	e.deps.Hazards = spy
	e.bmkg.set("/seismic-events", js([]any{event("E1", 4.0, false, "2026-10-05T00:50:00Z")}))
	_, r := e.bmkgPoller()

	r.tick(context.Background())
	sent := e.bmkg.lastRequest().Header.Get("X-Correlation-ID")
	if sent == "" || len(spy.cids) != 1 || spy.cids[0] != sent {
		t.Fatalf("correlation ID di SaveHazard (%v) harus sama dengan header ke BMKG (%q)", spy.cids, sent)
	}

	r.tick(context.Background())
	if len(spy.cids) != 2 || spy.cids[1] == spy.cids[0] {
		t.Errorf("setiap siklus harus memakai correlation ID baru, dapat %v", spy.cids)
	}
}
