package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"tubes-aat-pepeftw/cmd/client-api/internal/fieldreport"

	wire "tubes-aat-pepeftw/internal/fieldreport"
)

const fieldTeamScopes = "hazard:read:raw field-report:write"

// fakePublisher stands in for Kafka. It records what was acknowledged and the deadline it was given.
type fakePublisher struct {
	mu        sync.Mutex
	err       error
	published []wire.Message
	deadline  time.Duration
}

func (f *fakePublisher) Publish(ctx context.Context, m wire.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, m)
	return nil
}

const validReport = `{"latitude":-7.54,"longitude":110.44,"reported_at":"2026-11-18T03:00:00+07:00",` +
	`"description":"Abu vulkanik turun di Dusun Kaliurang","hazard_id":"7D0E6F1C-0D7B-5C55-9A0E-5F0A1D2B3C4D"}`

func (a *testAPI) report(key, body, sub, scope string) (int, map[string]any, http.Header) {
	a.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	return a.send(req, sub, scope)
}

// The field team's report is accepted only once Kafka has it; the record carries everything the
// aggregator needs to store it.
func TestFieldTeamReportIsAcceptedAfterKafkaHasIt(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")

	status, body, _ := api.report("laporan-001", validReport, "field-team", fieldTeamScopes)
	if status != http.StatusAccepted || body["status"] != "accepted" {
		t.Fatalf("got %d %v, want 202 accepted", status, body)
	}
	if len(api.reports.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(api.reports.published))
	}
	m := api.reports.published[0]
	if body["report_id"] != m.ReportID || m.ReportID != wire.ReportID("field-team", "laporan-001") {
		t.Errorf("report_id %v, published %q, want %q", body["report_id"], m.ReportID, wire.ReportID("field-team", "laporan-001"))
	}
	if m.Subject != "field-team" || m.IdempotencyKey != "laporan-001" || m.AcceptedAt.IsZero() || m.CorrelationID == "" {
		t.Errorf("message %+v, want subject, key, accepted_at and correlation_id set", m)
	}
	want := wire.Report{Latitude: -7.54, Longitude: 110.44, ReportedAt: time.Date(2026, 11, 17, 20, 0, 0, 0, time.UTC),
		Description: "Abu vulkanik turun di Dusun Kaliurang", HazardID: "7d0e6f1c-0d7b-5c55-9a0e-5f0a1d2b3c4d"}
	if m.Report != want {
		t.Errorf("report %+v, want %+v", m.Report, want)
	}
	if api.reports.deadline <= 0 || api.reports.deadline > fieldreport.PublishTimeout {
		t.Errorf("publish deadline %s, want within %s", api.reports.deadline, fieldreport.PublishTimeout)
	}
}

// A resend under the same key gets the same report_id, so the aggregator stores it once. Another
// key, or the same key from another client, is another report.
func TestResendKeepsTheReportID(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
	_, first, _ := api.report("laporan-001", validReport, "field-team", fieldTeamScopes)
	_, again, _ := api.report("laporan-001", validReport, "field-team", fieldTeamScopes)
	_, other, _ := api.report("laporan-002", validReport, "field-team", fieldTeamScopes)
	if first["report_id"] == nil || first["report_id"] != again["report_id"] {
		t.Errorf("resend got report_id %v, first %v; want the same", again["report_id"], first["report_id"])
	}
	if other["report_id"] == first["report_id"] {
		t.Errorf("another key got the same report_id %v", other["report_id"])
	}
	if wire.ReportID("field-team", "ab") == wire.ReportID("field-tea", "mab") {
		t.Error("subject and key are not separated in the report_id")
	}
}

// Kafka not answering in time is 503 with Retry-After, never a 202 for an unwritten report.
func TestKafkaDownIsUnavailable(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
	api.reports.err = context.DeadlineExceeded
	status, body, header := api.report("laporan-001", validReport, "field-team", fieldTeamScopes)
	if status != http.StatusServiceUnavailable || errorCode(body) != "UPSTREAM_UNAVAILABLE" || header.Get("Retry-After") != "5" {
		t.Errorf("got %d %v Retry-After %q, want 503 UPSTREAM_UNAVAILABLE with Retry-After 5", status, body, header.Get("Retry-After"))
	}
	api.reports.err = errors.New("NOT_ENOUGH_REPLICAS")
	if status, _, _ := api.report("laporan-001", validReport, "field-team", fieldTeamScopes); status != http.StatusServiceUnavailable {
		t.Errorf("not enough replicas: got %d, want 503", status)
	}
}

// Only field-team sends reports.
func TestOnlyFieldTeamSendsReports(t *testing.T) {
	for _, tt := range []struct {
		sub, scope string
		status     int
		code       string
	}{
		{"media", "hazard:read:summary", http.StatusForbidden, "FORBIDDEN"},
		{"bnpb-ops", opsScopes, http.StatusForbidden, "FORBIDDEN"},
		{"field-team", "hazard:read:raw", http.StatusForbidden, "FORBIDDEN"},
		{"", "", http.StatusUnauthorized, "UNAUTHORIZED"},
	} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
		status, body, _ := api.report("laporan-001", validReport, tt.sub, tt.scope)
		if status != tt.status || errorCode(body) != tt.code || len(api.reports.published) != 0 {
			t.Errorf("%q with %q: got %d %v (published %d), want %d %s and nothing published",
				tt.sub, tt.scope, status, body, len(api.reports.published), tt.status, tt.code)
		}
	}
}

func TestIdempotencyKeyIsRequired(t *testing.T) {
	for _, tt := range []struct {
		name, key string
		ok        bool
	}{
		{"missing", "", false},
		{"129 characters", strings.Repeat("k", 129), false},
		{"a space", "laporan 001", false},
		{"non-ASCII", "laporan-ü", false},
		{"128 characters", strings.Repeat("k", 128), true},
		{"one character", "~", true},
	} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
		status, body, _ := api.report(tt.key, validReport, "field-team", fieldTeamScopes)
		if tt.ok && status != http.StatusAccepted {
			t.Errorf("%s: got %d %v, want 202", tt.name, status, body)
		}
		if !tt.ok && (status != http.StatusBadRequest || errorCode(body) != "VALIDATION_ERROR" || len(api.reports.published) != 0) {
			t.Errorf("%s: got %d %v (published %d), want 400 VALIDATION_ERROR and nothing published",
				tt.name, status, body, len(api.reports.published))
		}
	}
}

// A body that is poisonous, malformed, incomplete or out of range is 400 and never reaches Kafka.
func TestBadReportBodiesAreRejected(t *testing.T) {
	for _, tt := range []struct{ name, body string }{
		{"invalid UTF-8", "{\"latitude\":-7.5,\"longitude\":110.4,\"reported_at\":\"2026-11-18T03:00:00Z\",\"description\":\"\xff\xfe\"}"},
		{"33 levels of nesting", `{"latitude":` + strings.Repeat("[", 33) + strings.Repeat("]", 33) + `}`},
		{"truncated JSON", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","descr`},
		{"empty body", ``},
		{"not an object", `[1,2]`},
		{"two objects", validReport + validReport},
		{"unknown field", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x","severity":"HIGH"}`},
		{"latitude missing", `{"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"latitude null", `{"latitude":null,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"longitude missing", `{"latitude":-7.5,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"reported_at missing", `{"latitude":-7.5,"longitude":110.4,"description":"x"}`},
		{"description missing", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z"}`},
		{"latitude out of range", `{"latitude":-91,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"longitude out of range", `{"latitude":-7.5,"longitude":180.5,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"latitude as text", `{"latitude":"-7.5","longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x"}`},
		{"reported_at not RFC 3339", `{"latitude":-7.5,"longitude":110.4,"reported_at":"18/11/2026 03:00","description":"x"}`},
		{"empty description", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":""}`},
		{"description over 4 KiB", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"` + strings.Repeat("a", 4097) + `"}`},
		{"hazard_id not a UUID", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x","hazard_id":"merapi"}`},
		{"hazard_id empty", `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00Z","description":"x","hazard_id":""}`},
	} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
		status, body, _ := api.report("laporan-001", tt.body, "field-team", fieldTeamScopes)
		if status != http.StatusBadRequest || errorCode(body) != "VALIDATION_ERROR" || len(api.reports.published) != 0 {
			t.Errorf("%s: got %d %v (published %d), want 400 VALIDATION_ERROR and nothing published",
				tt.name, status, body, len(api.reports.published))
		}
	}
}

// A description of exactly 4 KiB, without hazard_id, is a valid report.
func TestLongestDescriptionIsAccepted(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
	body := `{"latitude":-7.5,"longitude":110.4,"reported_at":"2026-11-18T03:00:00.123Z","description":"` + strings.Repeat("a", 4096) + `"}`
	if status, resp, _ := api.report("laporan-001", body, "field-team", fieldTeamScopes); status != http.StatusAccepted {
		t.Errorf("got %d %v, want 202", status, resp)
	}
}

// Any body over 64 KiB is 413 before anything reads it, on every route. fasthttp rejects it while
// reading the request, which app.Test reports as an error, so this runs over a real listener.
func TestBodyOver64KiBIsTooLarge(t *testing.T) {
	big := `{"description":"` + strings.Repeat("a", 64<<10) + `"}`
	for _, target := range []string{"/field-reports", "/unprocessed/replay?source=PVMBG"} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, "{}")
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = api.app.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
		t.Cleanup(func() { _ = api.app.Shutdown() })
		api.call = http.DefaultClient.Do

		req := httptest.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+target, strings.NewReader(big))
		req.RequestURI = ""
		req.Header.Set("Idempotency-Key", "laporan-001")
		status, body, _ := api.send(req, "field-team", fieldTeamScopes)
		if status != http.StatusRequestEntityTooLarge || errorCode(body) != "VALIDATION_ERROR" ||
			len(api.reports.published) != 0 || len(api.seen) != 0 {
			t.Errorf("%s: got %d %v (published %d, aggregator calls %d), want 413 VALIDATION_ERROR and nothing sent on",
				target, status, body, len(api.reports.published), len(api.seen))
		}
	}
}
