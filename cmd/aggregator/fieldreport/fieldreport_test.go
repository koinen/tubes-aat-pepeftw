package fieldreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"

	wire "tubes-aat-pepeftw/internal/fieldreport"
)

var now = time.Date(2026, 11, 18, 4, 0, 0, 0, time.UTC)

func message(key, description string) wire.Message {
	return wire.Message{
		ReportID:       wire.ReportID("field-team", key),
		Subject:        "field-team",
		IdempotencyKey: key,
		AcceptedAt:     time.Date(2026, 11, 18, 3, 0, 1, 0, time.UTC),
		CorrelationID:  "7f3c9a2e-4b1d-4e8a-9c2f-1a2b3c4d5e6f",
		Report: wire.Report{Latitude: -7.54, Longitude: 110.44, ReportedAt: time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC),
			Description: description},
	}
}

func record(t *testing.T, offset int64, m wire.Message) Record {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return Record{Topic: "field-reports", Partition: 2, Offset: offset, Value: b}
}

func TestDecideStoresAValidReport(t *testing.T) {
	m := message("laporan-001", "Abu vulkanik turun")
	m.Report.HazardID = "7d0e6f1c-0d7b-5c55-9a0e-5f0a1d2b3c4d"
	d := Decide(record(t, 41, m), now)
	if d.Parked != nil || d.Report == nil {
		t.Fatalf("decision %+v, want a report to store", d)
	}
	want := model.FieldReport{
		ReportID: m.ReportID, Subject: "field-team", IdempotencyKey: "laporan-001", Latitude: -7.54, Longitude: 110.44,
		ReportedAt: m.Report.ReportedAt, Description: "Abu vulkanik turun", HazardID: m.Report.HazardID,
		BodySHA256: m.Report.SHA256(), AcceptedAt: m.AcceptedAt, CorrelationID: m.CorrelationID, Partition: 2, Offset: 41,
	}
	if *d.Report != want {
		t.Errorf("report %+v\nwant   %+v", *d.Report, want)
	}
}

// The body hash covers the report only: a resend accepted later hashes the same, another
// description does not, and a reported_at in another zone is the same instant.
func TestBodyHashIsTheReportOnly(t *testing.T) {
	a := message("laporan-001", "Abu vulkanik turun")
	b := a
	b.AcceptedAt = b.AcceptedAt.Add(time.Minute)
	b.CorrelationID = "other"
	b.Report.ReportedAt = b.Report.ReportedAt.In(time.FixedZone("WIB", 7*3600))
	c := message("laporan-001", "Lahar dingin")
	if a.Report.SHA256() != b.Report.SHA256() {
		t.Error("a resend of the same report hashes differently")
	}
	if a.Report.SHA256() == c.Report.SHA256() {
		t.Error("another report hashes the same")
	}
}

// Bytes that are not a field report are parked as TIM_LAPANGAN under their topic position, with
// the payload intact.
func TestDecideParksBadBytes(t *testing.T) {
	forged := message("laporan-001", "x")
	forged.ReportID = wire.ReportID("field-team", "laporan-999")
	noSubject := message("laporan-001", "x")
	noSubject.Subject = ""
	tooLong := message("laporan-001", strings.Repeat("a", wire.MaxDescription+1))

	for _, tt := range []struct {
		name  string
		value []byte
		code  model.ReasonCode
	}{
		{"invalid UTF-8", []byte("{\"report_id\":\"\xff\"}"), model.ReasonInvalidUTF8},
		{"too deep", []byte(strings.Repeat("[", 33) + strings.Repeat("]", 33)), model.ReasonTooDeep},
		{"truncated", []byte(`{"report_id":"6b1f`), model.ReasonMalformedJSON},
		{"empty", []byte{}, model.ReasonMalformedJSON},
		{"wrong type", []byte(`{"report":{"latitude":"-7.5"}}`), model.ReasonSchemaIncompatible},
		{"null", []byte(`null`), model.ReasonSchemaIncompatible},
		{"no subject", mustJSON(t, noSubject), model.ReasonSchemaIncompatible},
		{"report_id not from subject and key", mustJSON(t, forged), model.ReasonSchemaIncompatible},
		{"description over 4 KiB", mustJSON(t, tooLong), model.ReasonSchemaIncompatible},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := Decide(Record{Topic: "field-reports", Partition: 1, Offset: 7, Value: tt.value}, now)
			if d.Report != nil || d.Parked == nil {
				t.Fatalf("decision %+v, want parked", d)
			}
			p := d.Parked
			if p.Code != tt.code || p.Source != model.SourceTimLapangan || p.Endpoint != "field-reports/1/7" ||
				p.Status != model.StatusParked || !bytes.Equal(p.Payload, tt.value) {
				t.Errorf("parked %s %s %s %s (payload intact %v), want %s TIM_LAPANGAN field-reports/1/7 parked",
					p.Code, p.Source, p.Endpoint, p.Status, bytes.Equal(p.Payload, tt.value), tt.code)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeStore is field_reports and unprocessed_messages in memory, with ON CONFLICT DO NOTHING on
// both. fail makes the next calls fail with the given errors, in order.
type fakeStore struct {
	reports     map[string]model.FieldReport
	unprocessed map[string]model.UnprocessedMessage
	fail        []error
	calls       int
}

func newFakeStore() *fakeStore {
	return &fakeStore{reports: map[string]model.FieldReport{}, unprocessed: map[string]model.UnprocessedMessage{}}
}

func (f *fakeStore) nextErr() error {
	f.calls++
	if len(f.fail) == 0 {
		return nil
	}
	err := f.fail[0]
	f.fail = f.fail[1:]
	return err
}

func (f *fakeStore) SaveFieldReport(_ context.Context, r model.FieldReport) (repository.FieldReportResult, error) {
	if err := f.nextErr(); err != nil {
		return "", err
	}
	old, ok := f.reports[r.ReportID]
	switch {
	case !ok:
		f.reports[r.ReportID] = r
		return repository.FieldReportInserted, nil
	case old.BodySHA256 == r.BodySHA256:
		return repository.FieldReportDuplicate, nil
	default:
		return repository.FieldReportConflict, nil
	}
}

func (f *fakeStore) SaveUnprocessed(_ context.Context, m model.UnprocessedMessage) (bool, error) {
	if err := f.nextErr(); err != nil {
		return false, err
	}
	if _, ok := f.unprocessed[m.MessageID]; ok {
		return false, nil
	}
	f.unprocessed[m.MessageID] = m
	return true, nil
}

func newTestHandler(store *fakeStore) (*Handler, *[]int) {
	h := NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.now = func() time.Time { return now }
	var waits []int
	h.wait = func(ctx context.Context, attempt int) error {
		waits = append(waits, attempt)
		return ctx.Err()
	}
	return h, &waits
}

// Resends under one key are stored once; the DLQ stays empty.
func TestResendsAreStoredOnce(t *testing.T) {
	store := newFakeStore()
	h, _ := newTestHandler(store)
	m := message("laporan-001", "Abu vulkanik turun")
	for offset := int64(0); offset < 3; offset++ {
		if err := h.Handle(context.Background(), record(t, offset, m)); err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
	}
	other := message("laporan-002", "Abu vulkanik turun")
	if err := h.Handle(context.Background(), record(t, 3, other)); err != nil {
		t.Fatal(err)
	}
	if len(store.reports) != 2 || len(store.unprocessed) != 0 {
		t.Errorf("stored %d reports and %d unprocessed, want 2 and 0", len(store.reports), len(store.unprocessed))
	}
	if got := store.reports[m.ReportID].Offset; got != 0 {
		t.Errorf("stored copy from offset %d, want the first (0)", got)
	}
}

// Another body under a stored key keeps the first and parks the second as idempotency_conflict,
// so nothing that got 202 disappears silently.
func TestConflictingBodyIsParked(t *testing.T) {
	store := newFakeStore()
	h, _ := newTestHandler(store)
	first := message("laporan-001", "Abu vulkanik turun")
	second := message("laporan-001", "Lahar dingin di Kali Gendol")
	for i, m := range []wire.Message{first, second, second} {
		if err := h.Handle(context.Background(), record(t, int64(10+i), m)); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.reports[first.ReportID].Description; got != first.Report.Description {
		t.Errorf("stored description %q, want the first %q", got, first.Report.Description)
	}
	if len(store.unprocessed) != 2 {
		t.Fatalf("unprocessed rows %d, want 2 (one per conflicting record)", len(store.unprocessed))
	}
	for _, p := range store.unprocessed {
		if p.Code != model.ReasonIdempotencyConflict || p.Source != model.SourceTimLapangan || p.Status != model.StatusParked ||
			p.SourceRefID != first.ReportID || p.CorrelationID != second.CorrelationID || !strings.HasPrefix(p.Endpoint, "field-reports/2/1") {
			t.Errorf("parked %+v, want TIM_LAPANGAN idempotency_conflict for %s", p, first.ReportID)
		}
		if !bytes.Equal(p.Payload, mustJSON(t, second)) {
			t.Errorf("parked payload is not the second record")
		}
	}
}

// The same record read again (after a rebalance) is not parked twice.
func TestConflictReadTwiceIsParkedOnce(t *testing.T) {
	store := newFakeStore()
	h, _ := newTestHandler(store)
	_ = h.Handle(context.Background(), record(t, 1, message("laporan-001", "a")))
	conflict := record(t, 2, message("laporan-001", "b"))
	_ = h.Handle(context.Background(), conflict)
	_ = h.Handle(context.Background(), conflict)
	if len(store.unprocessed) != 1 {
		t.Errorf("unprocessed rows %d, want 1", len(store.unprocessed))
	}
}

// While the store is down the same record is retried, with backoff, until it is stored.
func TestStoreDownRetriesTheSameRecord(t *testing.T) {
	store := newFakeStore()
	down := fmt.Errorf("save field report: %w", repository.ErrUnavailable)
	store.fail = []error{down, down, errors.New("relation \"field_reports\" does not exist")}
	h, waits := newTestHandler(store)
	m := message("laporan-001", "Abu vulkanik turun")
	if err := h.Handle(context.Background(), record(t, 5, m)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 4 || len(store.reports) != 1 || len(store.unprocessed) != 0 {
		t.Errorf("calls %d, reports %d, unprocessed %d; want 4, 1, 0", store.calls, len(store.reports), len(store.unprocessed))
	}
	if fmt.Sprint(*waits) != "[1 2 3]" {
		t.Errorf("waited before attempts %v, want [1 2 3]", *waits)
	}
}

// Parking retries the same way: a record is never committed unstored and unparked.
func TestParkRetriesWhileTheStoreIsDown(t *testing.T) {
	store := newFakeStore()
	store.fail = []error{fmt.Errorf("save unprocessed: %w", repository.ErrUnavailable)}
	h, _ := newTestHandler(store)
	if err := h.Handle(context.Background(), Record{Topic: "field-reports", Value: []byte(`{`)}); err != nil {
		t.Fatal(err)
	}
	if store.calls != 2 || len(store.unprocessed) != 1 {
		t.Errorf("calls %d, unprocessed %d; want 2, 1", store.calls, len(store.unprocessed))
	}
}

// Shutting down while the store is down leaves the record uncommitted.
func TestShutdownWhileRetryingReturnsTheError(t *testing.T) {
	store := newFakeStore()
	store.fail = []error{repository.ErrUnavailable, repository.ErrUnavailable}
	h, _ := newTestHandler(store)
	ctx, cancel := context.WithCancel(context.Background())
	h.wait = func(context.Context, int) error { cancel(); return context.Canceled }
	if err := h.Handle(ctx, record(t, 1, message("laporan-001", "a"))); !errors.Is(err, context.Canceled) {
		t.Errorf("Handle: %v, want context.Canceled", err)
	}
	if len(store.reports) != 0 {
		t.Error("report stored after shutdown")
	}
}

// A value the store refuses can never be stored; it is parked as store_rejected and committed.
func TestRejectedValueIsParked(t *testing.T) {
	store := newFakeStore()
	store.fail = []error{fmt.Errorf("save field report: %w", repository.ErrRejected)}
	h, waits := newTestHandler(store)
	m := message("laporan-001", "a")
	if err := h.Handle(context.Background(), record(t, 9, m)); err != nil {
		t.Fatal(err)
	}
	if len(store.unprocessed) != 1 || len(*waits) != 0 {
		t.Fatalf("unprocessed %d, waits %v; want 1 and none", len(store.unprocessed), *waits)
	}
	for _, p := range store.unprocessed {
		if p.Code != model.ReasonStoreRejected || p.Status != model.StatusParked || p.SourceRefID != m.ReportID {
			t.Errorf("parked %s %s ref %q, want store_rejected parked %s", p.Code, p.Status, p.SourceRefID, m.ReportID)
		}
	}
}
