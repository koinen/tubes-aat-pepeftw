package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const coastalEvent = `{"event_id":"ev-1","event_type":"hazard.created","hazard_id":"hz-1",` +
	`"emitted_at":"2026-11-18T03:00:01Z","payload":{"hazard_id":"hz-1","source":"SENSOR_PANTAI",` +
	`"severity":"AWAS","latitude":-8.5,"attributes":{"fetched_at":"2026-11-18T03:00:00.123Z","critical":true}}}`

func TestWebhookRetriesBeforeMarkingAnAlertSeen(t *testing.T) {
	seen, log := newNotifierTestDependencies(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen.Has("hz-1") {
			t.Error("alert marked seen before 2xx")
		}
		var body struct {
			HazardID   string         `json:"hazard_id"`
			EventID    string         `json:"event_id"`
			EmittedAt  string         `json:"emitted_at"`
			Latitude   float64        `json:"latitude"`
			Attributes map[string]any `json:"attributes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPost || body.HazardID != "hz-1" || body.EventID != "ev-1" ||
			body.EmittedAt != "2026-11-18T03:00:01Z" || body.Latitude != -8.5 ||
			body.Attributes["fetched_at"] != "2026-11-18T03:00:00.123Z" {
			t.Errorf("unexpected webhook: %s %+v", r.Method, body)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	n := newNotifier(seen, log, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	record := &kgo.Record{Value: []byte(coastalEvent)}
	if err := handleRecord(ctx, record, n, nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || !seen.Has("hz-1") {
		t.Fatalf("got %d attempts, seen=%v; want 2 attempts and seen", calls.Load(), seen.Has("hz-1"))
	}
	if err := handleRecord(ctx, record, n, nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("already seen alert was sent again")
	}
}

func newNotifierTestDependencies(t *testing.T) (*seenStore, *slog.Logger) {
	t.Helper()
	seen, err := openSeenStore(filepath.Join(t.TempDir(), "seen.txt"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seen.Close() })
	return seen, slog.New(slog.NewTextHandler(io.Discard, nil))
}
