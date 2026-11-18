//go:build crdb

package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/httpapi"
	"tubes-aat-pepeftw/cmd/aggregator/migrate"
	"tubes-aat-pepeftw/cmd/aggregator/migrations"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
)

// CRDB_TEST_URL must point at a disposable aggregator database with the baseline schema.
func TestSensorStoredReadsWhileProbeIsBlocked(t *testing.T) {
	url := os.Getenv("CRDB_TEST_URL")
	if url == "" {
		t.Skip("CRDB_TEST_URL not set")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrate.Run(ctx, pool, migrations.Files, log); err != nil {
		t.Fatal(err)
	}
	repo := postgres.New(pool)
	statuses, err := repo.ListSourceStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prior := slices.IndexFunc(statuses, func(s model.SourceStatus) bool { return s.Source == model.SourceSensorPantai })
	t.Cleanup(func() {
		if prior >= 0 {
			err = repo.SaveSourceStatus(ctx, statuses[prior])
		} else {
			_, err = pool.Exec(ctx, `DELETE FROM source_status WHERE source = $1`, model.SourceSensorPantai)
		}
		if err != nil {
			t.Error(err)
		}
	})

	sensor := &fakeSensor{stations: []string{"stn-padang-01"}, generatedAt: sensorStart.Add(30 * time.Second)}
	ref := "crdb-status-" + uuid.NewString()
	sensor.add(observation(0, fmt.Sprintf(`{"observation_id":%q}`, ref)),
		observation(1, fmt.Sprintf(`{"observation_id":%q}`, ref+"-2")))
	probeStarted, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/observations" && r.URL.Query().Get("limit") == "1" {
			close(probeStarted)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		sensor.ServeHTTP(w, r)
	}))
	defer upstream.Close()

	clock := time.Now().UTC()
	p := New(Config{SensorPantaiURL: upstream.URL, SensorPantaiTimeout: 8 * time.Second}, Deps{
		Hazards: repo, Existing: repo, Unprocessed: repo, Positions: &sensorStore{}, Statuses: repo,
		Log: log, Now: func() time.Time { return clock },
	})
	r := p.runners[slices.IndexFunc(p.runners, func(r *runner) bool { return r.source == model.SourceSensorPantai })]
	tick := func(elapsed time.Duration) {
		clock = clock.Add(elapsed)
		r.tick(ctx)
	}
	app := httpapi.New(httpapi.Deps{Hazards: repo, Statuses: repo, Log: log, InternalKey: "test", SensorPantaiEnabled: true})
	get := func(path string, dst any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Internal-Key", "test")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: got %s", path, resp.Status)
		}
		if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
			t.Fatal(err)
		}
	}
	readStatus := func() model.SourceStatus {
		t.Helper()
		var response struct {
			Data map[model.Source]model.SourceStatus
		}
		get("/internal/sources/status", &response)
		return response.Data[model.SourceSensorPantai]
	}
	tick(0)
	first := readStatus()
	if silence := first.StationSilence["stn-padang-01"]; first.Status != model.StatusOK || silence == nil || *silence {
		t.Fatalf("expected stored healthy status and station silence=false: %+v", first)
	}
	sensor.observationsDown = true
	for range 5 {
		tick(time.Second)
	}
	sensor.observationsDown = false
	sensor.generatedAt = sensor.generatedAt.Add(time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick(5 * time.Second)
	}()
	defer func() {
		unblock()
		<-done
	}()
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not reach Sensor Pantai")
	}
	open := readStatus()
	if silence, ok := open.StationSilence["stn-padang-01"]; open.Status != model.StatusStale ||
		open.BreakerState != model.BreakerOpen || !ok || silence != nil || !open.LastSuccessAt.Equal(*first.LastSuccessAt) {
		t.Fatalf("expected stored open snapshot while probe was blocked: %+v", open)
	}
	var hazards struct {
		Data    []model.HazardEvent
		Sources map[model.Source]model.SourceStatus
	}
	get("/internal/hazards?type=COASTAL", &hazards)
	if !slices.ContainsFunc(hazards.Data, func(h model.HazardEvent) bool { return h.SourceRefID == ref }) ||
		hazards.Sources[model.SourceSensorPantai].Status != model.StatusStale {
		t.Fatalf("expected last known COASTAL data and stale status while probe was blocked")
	}
	unblock()
	<-done
	if st := readStatus(); st.BreakerState != model.BreakerClosed || st.Status != model.StatusStale {
		t.Fatalf("probe should close the breaker without refreshing stored data: %+v", st)
	}
	tick(time.Second)
	if st := readStatus(); st.Status != model.StatusOK || !st.LastSuccessAt.After(*first.LastSuccessAt) {
		t.Fatalf("normal ingestion did not update freshness: %+v", st)
	}
}
