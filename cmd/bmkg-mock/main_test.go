package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testKey = "bmkg_test_key"

func testApp(t *testing.T) (*Store, func(*http.Request) *http.Response) {
	t.Helper()
	store := &Store{}
	seed(store, 25)
	cfg := Config{APIKey: testKey, DelayMin: 0, DelayMax: 0}
	app := newApp(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return store, func(r *http.Request) *http.Response {
		resp, err := app.Test(r)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
}

func decodeErrCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	var body struct {
		Error struct {
			Code          string `json:"code"`
			CorrelationID string `json:"correlation_id"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.CorrelationID == "" {
		t.Error("error response tanpa correlation_id")
	}
	return body.Error.Code
}

func TestSeedIsDeterministicAndLargeEnough(t *testing.T) {
	a, b := &Store{}, &Store{}
	seed(a, 25)
	seed(b, 25)
	ea, eb := a.EventsSince(time.Time{}), b.EventsSince(time.Time{})
	if len(ea) < 20 {
		t.Fatalf("seed hanya %d event, minimal 20", len(ea))
	}
	for i := range ea {
		if ea[i] != eb[i] {
			t.Fatalf("seed tidak deterministik pada indeks %d", i)
		}
	}
	if _, nw := a.Counts(); nw == 0 {
		t.Fatal("seed harus memuat minimal satu TsunamiWarning")
	}
}

func TestAuth(t *testing.T) {
	_, do := testApp(t)
	cases := []struct {
		name   string
		header map[string]string
		status int
		code   string
	}{
		{"tanpa kredensial", nil, 401, "UNAUTHORIZED"},
		{"key salah", map[string]string{"X-BMKG-Key": "salah"}, 401, "UNAUTHORIZED"},
		{"token PVMBG di Authorization", map[string]string{"Authorization": "Bearer pvmbg.bnpb-aggregator.abc"}, 403, "FORBIDDEN"},
		{"token PVMBG di X-BMKG-Key", map[string]string{"X-BMKG-Key": "pvmbg.bnpb-aggregator.abc"}, 403, "FORBIDDEN"},
		{"key benar", map[string]string{"X-BMKG-Key": testKey}, 200, ""},
	}
	for _, tc := range cases {
		for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
			req := httptest.NewRequest("GET", path, nil)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			resp := do(req)
			if resp.StatusCode != tc.status {
				t.Errorf("%s %s: status %d, ingin %d", tc.name, path, resp.StatusCode, tc.status)
				continue
			}
			if tc.code != "" {
				if got := decodeErrCode(t, resp); got != tc.code {
					t.Errorf("%s %s: code %s, ingin %s", tc.name, path, got, tc.code)
				}
			}
		}
	}
}

func TestSinceFilterInclusive(t *testing.T) {
	store, do := testApp(t)
	all := store.EventsSince(time.Time{})
	pivot := all[10].OccurredAt

	req := httptest.NewRequest("GET", "/seismic-events?since="+pivot.Format(time.RFC3339), nil)
	req.Header.Set("X-BMKG-Key", testKey)
	resp := do(req)
	var got []SeismicEvent
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(all)-10 {
		t.Fatalf("dapat %d event, ingin %d", len(got), len(all)-10)
	}
	if got[0].EventID != all[10].EventID {
		t.Fatalf("since harus inklusif: event pertama %s, ingin %s", got[0].EventID, all[10].EventID)
	}
}

func TestSinceInvalid(t *testing.T) {
	_, do := testApp(t)
	req := httptest.NewRequest("GET", "/tsunami-warnings?since=kemarin", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	resp := do(req)
	if resp.StatusCode != 400 || decodeErrCode(t, resp) != "VALIDATION_ERROR" {
		t.Fatalf("since tidak valid harus 400 VALIDATION_ERROR, dapat %d", resp.StatusCode)
	}
}

func TestWarningsMatchEvents(t *testing.T) {
	store, _ := testApp(t)
	byID := map[string]SeismicEvent{}
	for _, e := range store.EventsSince(time.Time{}) {
		byID[e.EventID] = e
	}
	for _, w := range store.WarningsSince(time.Time{}) {
		ev, ok := byID[w.RelatedEventID]
		if !ok {
			t.Fatalf("warning %s menunjuk event yang tidak ada", w.WarningID)
		}
		if !ev.PotentialTsunami {
			t.Errorf("warning %s untuk event tanpa potential_tsunami", w.WarningID)
		}
		if !w.eventOccurredAt.Equal(ev.OccurredAt) {
			t.Errorf("waktu filter warning %s harus sama dengan occurred_at %s", w.eventOccurredAt, ev.OccurredAt)
		}
	}
}

func TestCorrelationIDPropagated(t *testing.T) {
	_, do := testApp(t)
	req := httptest.NewRequest("GET", "/health", nil)
	cid := "7f3c9a2e-4b1d-4e8a-9c2f-1a2b3c4d5e6f"
	req.Header.Set("X-Correlation-ID", cid)
	if got := do(req).Header.Get("X-Correlation-ID"); got != cid {
		t.Fatalf("correlation id %q, ingin %q", got, cid)
	}
	if got := do(httptest.NewRequest("GET", "/health", nil)).Header.Get("X-Correlation-ID"); len(got) != 36 {
		t.Fatalf("correlation id baru harus UUID, dapat %q", got)
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	_, do := testApp(t)
	resp := do(httptest.NewRequest("GET", "/tidak-ada", nil))
	if resp.StatusCode != 404 || decodeErrCode(t, resp) != "NOT_FOUND" {
		t.Fatalf("route tak dikenal harus 404 NOT_FOUND, dapat %d", resp.StatusCode)
	}
}

func TestTsunamiWarningJSONMatchesSpec(t *testing.T) {
	store, do := testApp(t)
	all := store.WarningsSince(time.Time{})
	req := httptest.NewRequest("GET", "/tsunami-warnings", nil)
	req.Header.Set("X-BMKG-Key", testKey)
	var got []map[string]any
	if err := json.NewDecoder(do(req).Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(all) || len(got) == 0 {
		t.Fatalf("dapat %d warning, ingin %d", len(got), len(all))
	}
	want := map[string]bool{"warning_id": true, "related_event_id": true, "threat_level": true,
		"affected_zones": true, "estimated_arrival": true}
	for k := range got[0] {
		if !want[k] {
			t.Errorf("field %q tidak ada di spesifikasi TsunamiWarning", k)
		}
	}
	if len(got[0]) != len(want) {
		t.Errorf("TsunamiWarning punya %d field, ingin %d", len(got[0]), len(want))
	}
}

func TestTsunamiSinceUsesRelatedEventTime(t *testing.T) {
	store, do := testApp(t)
	byID := map[string]SeismicEvent{}
	for _, e := range store.EventsSince(time.Time{}) {
		byID[e.EventID] = e
	}
	all := store.WarningsSince(time.Time{})
	if len(all) < 2 {
		t.Skip("seed punya kurang dari 2 warning")
	}
	pivot := byID[all[1].RelatedEventID].OccurredAt
	req := httptest.NewRequest("GET", "/tsunami-warnings?since="+pivot.Format(time.RFC3339), nil)
	req.Header.Set("X-BMKG-Key", testKey)
	var got []TsunamiWarning
	if err := json.NewDecoder(do(req).Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(all)-1 || got[0].WarningID != all[1].WarningID {
		t.Fatalf("dapat %d warning (pertama %v), ingin %d mulai %s", len(got), got, len(all)-1, all[1].WarningID)
	}
}