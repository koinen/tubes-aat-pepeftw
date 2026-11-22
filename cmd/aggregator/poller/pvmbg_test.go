package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	goodReport   = `{"report_id":"r1","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z"}`
	renamedField = `{"report_id":"r3","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:05:00Z"}`
)

var (
	pollTime    = time.Date(2026, 11, 18, 0, 10, 0, 0, time.UTC)
	priorCursor = time.Date(2026, 11, 17, 23, 0, 0, 0, time.UTC)
)

type fakeStore struct {
	saveErr     error
	dlqErr      error
	saved       []model.HazardEvent
	unprocessed []model.UnprocessedMessage
	cursor      time.Time
}

func (f *fakeStore) SaveHazard(_ context.Context, h model.HazardEvent) (repository.SaveOutcome, error) {
	if f.saveErr != nil {
		return "", f.saveErr
	}
	for _, s := range f.saved {
		if s.SourceRefID == h.SourceRefID {
			return repository.OutcomeUnchanged, nil
		}
	}
	f.saved = append(f.saved, h)
	return repository.OutcomeCreated, nil
}

func (f *fakeStore) SaveUnprocessed(_ context.Context, m model.UnprocessedMessage) (bool, error) {
	if f.dlqErr != nil {
		return false, f.dlqErr
	}
	f.unprocessed = append(f.unprocessed, m)
	return true, nil
}

func (f *fakeStore) GetCursor(context.Context, model.Source, string) (time.Time, bool, error) {
	return f.cursor, true, nil
}

func (f *fakeStore) SetCursor(_ context.Context, _ model.Source, _ string, t time.Time) error {
	f.cursor = t
	return nil
}

func (f *fakeStore) ListVolcanoRefs(context.Context) ([]model.VolcanoRef, error) {
	return []model.VolcanoRef{{VolcanoID: "MERAPI", Name: "Merapi", Latitude: -7.54, Longitude: 110.44}}, nil
}

func pvmbgCycle(t *testing.T, body string, store *fakeStore) error {
	t.Helper()
	return pvmbgCycleWith(t, body, store, mapper.VolcanicAdaptations{}, io.Discard)
}

func pvmbgCycleWith(t *testing.T, body string, store *fakeStore, rules mapper.VolcanicAdaptations, logs io.Writer) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	log := slog.New(slog.NewJSONHandler(logs, nil))
	now := func() time.Time { return pollTime }
	store.cursor = priorCursor
	deps := Deps{Hazards: store, Unprocessed: store, Cursors: store, Volcanoes: store, Log: log, Now: now}
	f := newFetcher(model.SourcePVMBG, srv.URL, time.Second, func(*http.Request) {}, log, now)
	ctx := correlation.With(context.Background(), "cid-1")
	return newPVMBGPoller(f, deps, Config{CursorOverlap: time.Minute, PVMBGAdaptations: rules}).cycle(ctx)
}

func reports(rs ...string) string { return `{"reports":[` + strings.Join(rs, ",") + `]}` }

func TestBreakingChangeIsParkedAndPollingMovesOn(t *testing.T) {
	store := &fakeStore{}
	if err := pvmbgCycle(t, reports(goodReport, renamedField), store); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if len(store.saved) != 1 || len(store.unprocessed) != 1 {
		t.Fatalf("saved %d, unprocessed %d, want 1 and 1", len(store.saved), len(store.unprocessed))
	}
	m := store.unprocessed[0]
	if m.Status != model.StatusParked || m.Code != model.ReasonSchemaIncompatible || m.SourceRefID != "r3" {
		t.Errorf("got %q %q ref %q", m.Status, m.Code, m.SourceRefID)
	}
	if string(m.Payload) != renamedField {
		t.Errorf("payload %q, want the record intact", m.Payload)
	}
	if want := time.Date(2026, 11, 18, 0, 5, 0, 0, time.UTC); !store.cursor.Equal(want) {
		t.Errorf("cursor %v, want %v", store.cursor, want)
	}
}

// Once the rename is declared, the same breaking report is stored live instead of parked,
// and the log says which rule it needed.
func TestDeclaredAdaptationStoresTheBreakingReport(t *testing.T) {
	rules, err := mapper.ParseVolcanicAdaptations("rename:alert_status=alert_level")
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	var logs bytes.Buffer
	store := &fakeStore{}
	if err := pvmbgCycleWith(t, reports(renamedField), store, rules, &logs); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if len(store.saved) != 1 || len(store.unprocessed) != 0 {
		t.Fatalf("saved %d, unprocessed %d, want 1 and 0", len(store.saved), len(store.unprocessed))
	}
	if h := store.saved[0]; h.SourceRefID != "r3" || h.Severity != model.SeveritySiaga {
		t.Errorf("got ref %q severity %q, want r3 SIAGA", h.SourceRefID, h.Severity)
	}
	want := map[string]any{"msg": "adaptation_applied", "source": "PVMBG", "source_ref_id": "r3",
		"rule": "rename:alert_status=alert_level", "correlation_id": "cid-1"}
	if !hasLogLine(t, logs.String(), want) {
		t.Errorf("no log line with %v in\n%s", want, logs.String())
	}
}

// The cursor overlap fetches the same report again on later polls. Only the poll that stored it
// counts the adaptation, so the counter says how many reports needed the rule.
func TestRefetchedAdaptedReportIsCountedOnce(t *testing.T) {
	rules, err := mapper.ParseVolcanicAdaptations("rename:alert_status=alert_level")
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	var logs bytes.Buffer
	store := &fakeStore{}
	for i := range 2 {
		if err := pvmbgCycleWith(t, reports(renamedField), store, rules, &logs); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}
	if n := countLogLines(t, logs.String(), map[string]any{"msg": "adaptation_applied"}); n != 1 {
		t.Errorf("%d adaptation_applied lines, want 1", n)
	}
}

func hasLogLine(t *testing.T, logs string, want map[string]any) bool {
	t.Helper()
	return countLogLines(t, logs, want) > 0
}

func countLogLines(t *testing.T, logs string, want map[string]any) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var got map[string]any
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
			}
		}
		if match {
			n++
		}
	}
	return n
}

func TestFailedDLQWriteKeepsTheCursor(t *testing.T) {
	store := &fakeStore{dlqErr: fmt.Errorf("insert: %w", repository.ErrUnavailable)}
	if err := pvmbgCycle(t, reports(goodReport, renamedField), store); err == nil {
		t.Fatal("cycle succeeded although the parked record was not stored")
	}
	if !store.cursor.Equal(priorCursor) {
		t.Errorf("cursor moved to %v; the next poll would not see the lost record", store.cursor)
	}
}

func TestUnavailableStoreLeavesARetryingRowWithTheHazard(t *testing.T) {
	store := &fakeStore{saveErr: fmt.Errorf("save hazard: %w: range unavailable", repository.ErrUnavailable)}
	if err := pvmbgCycle(t, reports(goodReport), store); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if len(store.unprocessed) != 1 {
		t.Fatalf("unprocessed %d, want 1", len(store.unprocessed))
	}
	m := store.unprocessed[0]
	if m.Status != model.StatusRetrying || m.Code != model.ReasonStoreUnavailable || m.SourceRefID != "r1" {
		t.Errorf("got %q %q ref %q", m.Status, m.Code, m.SourceRefID)
	}
	if m.Hazard == nil || m.Hazard.SourceRefID != "r1" || m.CorrelationID != "cid-1" {
		t.Errorf("retrying row lacks what the worker needs: hazard %v correlation %q", m.Hazard, m.CorrelationID)
	}
	if store.cursor.Equal(priorCursor) {
		t.Errorf("cursor did not advance although the record is safe in the DLQ")
	}
}

func TestLostLeadershipWritesNothing(t *testing.T) {
	store := &fakeStore{saveErr: fmt.Errorf("save hazard: %w: epoch 3", fence.ErrStaleEpoch)}
	err := pvmbgCycle(t, reports(goodReport), store)
	if !errors.Is(err, fence.ErrStaleEpoch) {
		t.Fatalf("cycle error %v, want stale epoch", err)
	}
	if len(store.unprocessed) != 0 || !store.cursor.Equal(priorCursor) {
		t.Errorf("a fenced-out leader wrote %d DLQ rows / moved the cursor", len(store.unprocessed))
	}
}

func TestCorruptResponseIsParkedWholeAndRefetched(t *testing.T) {
	tests := []struct {
		name string
		body string
		want model.ReasonCode
	}{
		{"truncated", `{"reports":[` + goodReport, model.ReasonMalformedJSON},
		{"invalid UTF-8", "{\"reports\":[{\"report_id\":\"\xff\"}]}", model.ReasonInvalidUTF8},
		{"too deep", `{"reports":` + strings.Repeat("[", 100) + strings.Repeat("]", 100) + `}`, model.ReasonTooDeep},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			if err := pvmbgCycle(t, tt.body, store); err == nil {
				t.Error("cycle succeeded on a corrupt response")
			}
			if len(store.unprocessed) != 1 || store.unprocessed[0].Code != tt.want {
				t.Fatalf("unprocessed %+v, want one %s", store.unprocessed, tt.want)
			}
			if string(store.unprocessed[0].Payload) != tt.body || store.unprocessed[0].Endpoint != "volcanic-reports" {
				t.Errorf("whole body not stored for the endpoint")
			}
			if !store.cursor.Equal(priorCursor) {
				t.Errorf("cursor moved; the real data would be skipped")
			}
		})
	}
}

func TestOversizedResponseIsParkedWithoutHoldingIt(t *testing.T) {
	body := reports(goodReport) + strings.Repeat(" ", 11<<20)
	store := &fakeStore{}
	if err := pvmbgCycle(t, body, store); err == nil {
		t.Error("cycle succeeded on an oversized response")
	}
	if len(store.unprocessed) != 1 {
		t.Fatalf("unprocessed %d, want 1", len(store.unprocessed))
	}
	m := store.unprocessed[0]
	if m.Code != model.ReasonTooLarge || m.PayloadSize != int64(len(body)) || len(m.Payload) != 64<<10 {
		t.Errorf("code %q size %d stored %d", m.Code, m.PayloadSize, len(m.Payload))
	}
	if string(m.Payload[:len(reports(goodReport))]) != reports(goodReport) {
		t.Errorf("stored prefix is not the start of the body")
	}
	if len(store.saved) != 0 || !store.cursor.Equal(priorCursor) {
		t.Errorf("oversized response was partly processed")
	}
}

// A malformed response between 1 and 10 MiB is parked whole with its real code, not too_large.
func TestLargeMalformedResponseKeepsItsRealCode(t *testing.T) {
	body := `{"reports":[` + goodReport + strings.Repeat(" ", 2<<20)
	store := &fakeStore{}
	if err := pvmbgCycle(t, body, store); err == nil {
		t.Error("cycle succeeded on a malformed response")
	}
	if len(store.unprocessed) != 1 {
		t.Fatalf("unprocessed %d, want 1", len(store.unprocessed))
	}
	if m := store.unprocessed[0]; m.Code != model.ReasonMalformedJSON || len(m.Payload) != 64<<10 || m.PayloadSize != int64(len(body)) {
		t.Errorf("code %q stored %d of %d bytes, want malformed_json 65536 of %d", m.Code, len(m.Payload), m.PayloadSize, len(body))
	}
}
