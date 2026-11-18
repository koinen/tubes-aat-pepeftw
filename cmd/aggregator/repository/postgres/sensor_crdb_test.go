//go:build crdb

// Run against a CockroachDB that holds the aggregator schema (migrations through 012; 009 still needs
// the manual workaround on a fresh database):
//
//	CRDB_TEST_URL='postgres://root@127.0.0.1:26299/aggregator?sslmode=disable' \
//	  go test -tags crdb -run Sensor ./cmd/aggregator/repository/postgres/
//
// The test applies any newer migrations itself, uses fresh ids, and leaves its rows behind.
package postgres_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/migrate"
	"tubes-aat-pepeftw/cmd/aggregator/migrations"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/repository/postgres"
)

func cockroach(t *testing.T) (*postgres.PostgresRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("CRDB_TEST_URL")
	if url == "" {
		t.Skip("CRDB_TEST_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrate.Run(ctx, pool, migrations.Files, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return postgres.New(pool), pool
}

func coastalHazard(t *testing.T, ref, extra string) model.HazardEvent {
	t.Helper()
	raw := `{"observation_id":"` + ref + `","station_id":"stn-padang-01","observed_at":"2026-11-12T00:30:00Z",
		"sea_level_anomaly_m":1,"wave_height_m":5.3,"quality":"ok"` + extra + `}`
	page := mapper.CoastalPage{GeneratedAt: time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC), FetchedAt: time.Now().UTC()}
	r := mapper.Coastal([]byte(raw), page, func(string) (mapper.Station, bool) {
		return mapper.Station{Name: "Padang", Province: "Sumatera Barat", Latitude: -0.9492, Longitude: 100.3543}, true
	})
	if r.Hazard == nil {
		t.Fatalf("mapping %s: %v", ref, r.Violation)
	}
	return *r.Hazard
}

// relayBatch is the aggregator's relay batch size (main.go relayBatchSize).
const relayBatch = 100

// A new AWAS is in the relay's next batch ahead of every NORMAL row, however large the backlog of
// older NORMAL rows is. It holds while fewer than relayBatch older AWAS rows are pending.
func TestSensorAwasIsPublishedBeforeOlderNormalEvents(t *testing.T) {
	repo, _ := cockroach(t)
	ctx := context.Background()
	run := uuid.NewString()[:8]

	for i := range 3 {
		h := coastalHazard(t, "crdb-"+run+"-n"+strconv.Itoa(i), `,"critical":false`)
		if _, err := repo.SaveHazard(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	awas := coastalHazard(t, "crdb-"+run+"-critical", `,"critical":true`)
	if _, err := repo.SaveHazard(ctx, awas); err != nil {
		t.Fatal(err)
	}

	batch, err := repo.PendingOutbox(ctx, relayBatch)
	if err != nil {
		t.Fatal(err)
	}
	sawNormal := false
	for i, r := range batch {
		var msg model.HazardMessage
		if err := json.Unmarshal(r.Payload, &msg); err != nil {
			t.Fatalf("row %d payload: %v", i, err)
		}
		if r.HazardID == awas.HazardID {
			if sawNormal {
				t.Fatalf("the new AWAS is at %d, behind a NORMAL row", i)
			}
			return
		}
		sawNormal = sawNormal || msg.Payload.Severity != model.SeverityAwas
	}
	t.Fatalf("the new AWAS is not in the next relay batch of %d (older NORMAL rows pending ahead of it?)", len(batch))
}

func TestSensorObservationSurvivesAStoreRoundTrip(t *testing.T) {
	repo, _ := cockroach(t)
	ctx := context.Background()
	h := coastalHazard(t, "crdb-"+uuid.NewString()[:8], `,"critical":false`)
	if _, err := repo.SaveHazard(ctx, h); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.GetHazard(ctx, h.HazardID)
	if err != nil {
		t.Fatal(err)
	}
	if !mapper.SameObservation(stored, h) {
		t.Errorf("stored attributes %v no longer match %v; every re-fetch would be parked as id_conflict", stored.Attributes, h.Attributes)
	}
}

func TestSensorStreamPositionRoundTripsBesideTimeCursors(t *testing.T) {
	repo, _ := cockroach(t)
	ctx := context.Background()
	endpoint := "crdb-" + uuid.NewString()[:8]

	if _, found, err := repo.GetStreamPosition(ctx, model.SourceSensorPantai, endpoint); err != nil || found {
		t.Fatalf("new endpoint: found=%v err=%v, want not found", found, err)
	}
	want := repository.StreamPosition{Cursor: "eyJvIjo4MTN9", GeneratedAt: time.Date(2026, 11, 12, 2, 43, 5, 0, time.UTC)}
	if err := repo.SetStreamPosition(ctx, model.SourceSensorPantai, endpoint, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := repo.GetStreamPosition(ctx, model.SourceSensorPantai, endpoint)
	if err != nil || !found || got != want {
		t.Fatalf("got %+v found=%v err=%v, want %+v", got, found, err, want)
	}

	// From the start again after a sensor restart: no cursor, an earlier generated_at.
	restart := repository.StreamPosition{GeneratedAt: time.Date(2026, 11, 12, 0, 1, 0, 0, time.UTC)}
	if err := repo.SetStreamPosition(ctx, model.SourceSensorPantai, endpoint, restart); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := repo.GetStreamPosition(ctx, model.SourceSensorPantai, endpoint); got != restart {
		t.Errorf("after restart got %+v, want %+v", got, restart)
	}

	// The time cursors of the other pollers read the same table and must not see a cursor column.
	since := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	if err := repo.SetCursor(ctx, model.SourcePVMBG, endpoint, since); err != nil {
		t.Fatal(err)
	}
	if got, found, err := repo.GetCursor(ctx, model.SourcePVMBG, endpoint); err != nil || !found || !got.Equal(since) {
		t.Errorf("PVMBG cursor %v found=%v err=%v, want %v", got, found, err, since)
	}
}
