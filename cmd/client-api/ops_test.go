package main

import (
	"crypto/ed25519"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/client-api/internal/aggregator"
	"tubes-aat-pepeftw/cmd/client-api/internal/auth"
	"tubes-aat-pepeftw/cmd/client-api/internal/config"
)

const (
	testOpsToken = "ops-secret"
	testIssuer   = "auth-service"
	testAudience = "bnpb-ops"
	testKid      = "test-key"

	opsScopes = "hazard:read:raw hazard:read:unprocessed hazard:replay:unprocessed"
)

// seenRequest is what the fake aggregator received.
type seenRequest struct {
	method, uri, auth, internalKey, requestedBy string
}

type testAPI struct {
	t    *testing.T
	agg  *httptest.Server
	key  ed25519.PrivateKey
	call func(req *http.Request) (*http.Response, error)
	seen []seenRequest
	// reports is the fake Kafka behind POST /field-reports.
	reports *fakePublisher
	app     *fiber.App
}

// newTestAPI runs client-api's router against a fake aggregator that answers with status and body.
func newTestAPI(t *testing.T, opsToken string, status int, body string) *testAPI {
	t.Helper()
	api := &testAPI{t: t, reports: &fakePublisher{}}
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.seen = append(api.seen, seenRequest{
			method: r.Method, uri: r.URL.RequestURI(), auth: r.Header.Get("Authorization"),
			internalKey: r.Header.Get("X-Internal-Key"), requestedBy: r.Header.Get("X-Requested-By"),
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(agg.Close)
	api.agg = agg

	pub, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	api.key = key
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier := auth.NewVerifier(&auth.JWKS{PublicKey: pub, Kid: testKid}, testIssuer, testAudience, nil)
	app := newApp(appDeps{
		Agg:          aggregator.NewClient(config.AggregatorConfig{BaseURL: agg.URL, InternalKey: "internal", OpsToken: opsToken}, log),
		FieldReports: api.reports,
		Protect:      []any{verifier.Middleware},
		Log:          log,
	})
	api.app = app
	api.call = func(req *http.Request) (*http.Response, error) { return app.Test(req) }
	return api
}

// closeAggregator makes every later aggregator call fail to connect.
func (a *testAPI) closeAggregator() {
	a.agg.Close()
}

func (a *testAPI) token(sub, scope string) string {
	a.t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": sub, "scope": scope, "iss": testIssuer, "aud": testAudience,
		"jti": uuid.NewString(), "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = testKid
	signed, err := tok.SignedString(a.key)
	if err != nil {
		a.t.Fatal(err)
	}
	return signed
}

// do sends method target with a token for sub and scope ("" sends no token).
func (a *testAPI) do(method, target, sub, scope string) (int, map[string]any, http.Header) {
	a.t.Helper()
	return a.send(httptest.NewRequest(method, target, nil), sub, scope)
}

// send sends req with a token for sub and scope ("" sends no token).
func (a *testAPI) send(req *http.Request, sub, scope string) (int, map[string]any, http.Header) {
	a.t.Helper()
	if scope != "" {
		req.Header.Set("Authorization", "Bearer "+a.token(sub, scope))
	}
	resp, err := a.call(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body, resp.Header
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

const unprocessedPage = `{"items":[{"message_id":"m-1","source":"PVMBG","received_at":"2026-11-18T03:00:00Z",` +
	`"reason":"schema_incompatible: alert_level missing","retryable":false,"attempts":1,"status":"parked"}],"next_cursor":null}`

// BNPB Internal Ops lists failed messages; client-api asks the aggregator's ops route with OPS_TOKEN.
func TestOpsListsFailedMessages(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, unprocessedPage)

	status, body, _ := api.do(http.MethodGet, "/unprocessed?source=PVMBG&limit=5&cursor=abc&since=2026-11-18T00:00:00Z&x=1", "bnpb-ops", opsScopes)
	if status != http.StatusOK {
		t.Fatalf("got %d %v, want 200", status, body)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["message_id"] != "m-1" {
		t.Errorf("body %v, want the aggregator's page", body)
	}
	if len(api.seen) != 1 {
		t.Fatalf("aggregator saw %d requests, want 1", len(api.seen))
	}
	got := api.seen[0]
	want := seenRequest{method: http.MethodGet, uri: "/internal/unprocessed?cursor=abc&limit=5&since=2026-11-18T00%3A00%3A00Z&source=PVMBG",
		auth: "Bearer " + testOpsToken}
	if got != want {
		t.Errorf("aggregator saw %+v, want %+v", got, want)
	}
}

// Only BNPB Internal Ops reaches the failed messages: other clients get 403, no token gets 401,
// and neither reaches the aggregator.
func TestOnlyOpsReachesFailedMessages(t *testing.T) {
	for _, tt := range []struct {
		method, target, sub, scope string
		status                     int
		code                       string
	}{
		{http.MethodGet, "/unprocessed", "media", "hazard:read:summary", http.StatusForbidden, "FORBIDDEN"},
		{http.MethodGet, "/unprocessed", "field-team", "hazard:read:raw", http.StatusForbidden, "FORBIDDEN"},
		{http.MethodGet, "/unprocessed", "", "", http.StatusUnauthorized, "UNAUTHORIZED"},
	} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, unprocessedPage)
		status, body, _ := api.do(tt.method, tt.target, tt.sub, tt.scope)
		if status != tt.status || errorCode(body) != tt.code {
			t.Errorf("%s %s as %q: got %d %v, want %d %s", tt.method, tt.target, tt.sub, status, body, tt.status, tt.code)
		}
		if len(api.seen) != 0 {
			t.Errorf("%s %s as %q reached the aggregator", tt.method, tt.target, tt.sub)
		}
	}
}

const queuedJob = `{"job_id":"0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10","status":"queued"}`

// Ops starts a replay; the aggregator learns who asked from the JWT subject.
func TestOpsStartsAReplay(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusAccepted, queuedJob)

	status, body, _ := api.do(http.MethodPost, "/unprocessed/replay?source=PVMBG", "bnpb-ops", opsScopes)
	if status != http.StatusAccepted || body["job_id"] != "0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10" || body["status"] != "queued" {
		t.Fatalf("got %d %v, want 202 with the queued job", status, body)
	}
	want := seenRequest{method: http.MethodPost, uri: "/internal/unprocessed/replay?source=PVMBG",
		auth: "Bearer " + testOpsToken, requestedBy: "bnpb-ops"}
	if len(api.seen) != 1 || api.seen[0] != want {
		t.Errorf("aggregator saw %+v, want %+v", api.seen, want)
	}
}

// Ops follows a replay job.
func TestOpsFollowsAReplayJob(t *testing.T) {
	job := `{"job_id":"0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10","status":"done","selected":3,"recovered":3}`
	api := newTestAPI(t, testOpsToken, http.StatusOK, job)

	status, body, _ := api.do(http.MethodGet, "/unprocessed/replay/0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10", "bnpb-ops", opsScopes)
	if status != http.StatusOK || body["status"] != "done" || body["recovered"] != 3.0 {
		t.Fatalf("got %d %v, want 200 with the job", status, body)
	}
	want := seenRequest{method: http.MethodGet, uri: "/internal/unprocessed/replay/0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10",
		auth: "Bearer " + testOpsToken}
	if len(api.seen) != 1 || api.seen[0] != want {
		t.Errorf("aggregator saw %+v, want %+v", api.seen, want)
	}
}

// Reading failed messages does not allow replaying them: replay has its own scope.
func TestReplayNeedsTheReplayScope(t *testing.T) {
	for _, tt := range []struct{ method, target string }{
		{http.MethodPost, "/unprocessed/replay?source=PVMBG"},
		{http.MethodGet, "/unprocessed/replay/0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10"},
	} {
		for _, tok := range []struct{ sub, scope string }{
			{"bnpb-ops", "hazard:read:raw hazard:read:unprocessed"},
			{"field-team", "hazard:read:raw"},
			{"media", "hazard:read:summary"},
		} {
			api := newTestAPI(t, testOpsToken, http.StatusAccepted, queuedJob)
			status, body, _ := api.do(tt.method, tt.target, tok.sub, tok.scope)
			if status != http.StatusForbidden || errorCode(body) != "FORBIDDEN" || len(api.seen) != 0 {
				t.Errorf("%s %s with %q: got %d %v (aggregator calls %d), want 403 FORBIDDEN and no call",
					tt.method, tt.target, tok.scope, status, body, len(api.seen))
			}
		}
	}
}

func auditError(code, message string) string {
	return `{"error":{"code":"` + code + `","message":"` + message + `"}}`
}

// The aggregator's answers come back in client-api's error body. A rejected OPS_TOKEN is our
// configuration, not the caller's fault, so it is reported as an unavailable upstream.
func TestOpsMapsAggregatorErrors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		aggStatus  int
		aggBody    string
		status     int
		code       string
		message    string
		retryAfter string
	}{
		{"bad request", http.StatusBadRequest, auditError("bad_request", "source tidak mendukung replay"),
			http.StatusBadRequest, "VALIDATION_ERROR", "source tidak mendukung replay", ""},
		{"not found", http.StatusNotFound, auditError("not_found", "job_id tidak ditemukan"),
			http.StatusNotFound, "NOT_FOUND", "job_id tidak ditemukan", ""},
		{"token rejected", http.StatusUnauthorized, auditError("unauthorized", "token tidak valid"),
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "upstream aggregator unavailable", ""},
		{"store down", http.StatusServiceUnavailable, auditError("unavailable", "penyimpanan sedang tidak terjangkau"),
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "upstream aggregator unavailable", "5"},
		{"aggregator bug", http.StatusInternalServerError, auditError("internal", "internal error"),
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "upstream aggregator unavailable", "5"},
		{"not json", http.StatusBadGateway, "<html>bad gateway</html>",
			http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE", "upstream aggregator unavailable", "5"},
	} {
		for _, req := range []struct{ method, target string }{
			{http.MethodGet, "/unprocessed"},
			{http.MethodPost, "/unprocessed/replay?source=BMKG"},
			{http.MethodGet, "/unprocessed/replay/0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10"},
		} {
			api := newTestAPI(t, testOpsToken, tt.aggStatus, tt.aggBody)
			status, body, header := api.do(req.method, req.target, "bnpb-ops", opsScopes)
			e, _ := body["error"].(map[string]any)
			if status != tt.status || e["code"] != tt.code || e["message"] != tt.message || e["correlation_id"] == "" {
				t.Errorf("%s, %s %s: got %d %v, want %d %s %q", tt.name, req.method, req.target, status, body, tt.status, tt.code, tt.message)
			}
			if got := header.Get("Retry-After"); got != tt.retryAfter {
				t.Errorf("%s, %s %s: Retry-After %q, want %q", tt.name, req.method, req.target, got, tt.retryAfter)
			}
		}
	}
}

func TestOpsWhenTheAggregatorIsUnreachable(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, unprocessedPage)
	api.closeAggregator()
	status, body, header := api.do(http.MethodGet, "/unprocessed", "bnpb-ops", opsScopes)
	if status != http.StatusServiceUnavailable || errorCode(body) != "UPSTREAM_UNAVAILABLE" || header.Get("Retry-After") != "5" {
		t.Errorf("got %d %v Retry-After %q, want 503 UPSTREAM_UNAVAILABLE with Retry-After 5", status, body, header.Get("Retry-After"))
	}
}

// A malformed job_id is the caller's mistake and never reaches the aggregator.
func TestReplayJobIDMustBeAUUID(t *testing.T) {
	api := newTestAPI(t, testOpsToken, http.StatusOK, queuedJob)
	status, body, _ := api.do(http.MethodGet, "/unprocessed/replay/not-a-uuid", "bnpb-ops", opsScopes)
	if status != http.StatusBadRequest || errorCode(body) != "VALIDATION_ERROR" || len(api.seen) != 0 {
		t.Errorf("got %d %v (aggregator calls %d), want 400 VALIDATION_ERROR and no call", status, body, len(api.seen))
	}
}

// Without OPS_TOKEN the ops routes say so and do not call the aggregator; the scope check still comes first.
func TestOpsRoutesWithoutOpsToken(t *testing.T) {
	for _, tt := range []struct{ method, target string }{
		{http.MethodGet, "/unprocessed"},
		{http.MethodPost, "/unprocessed/replay?source=PVMBG"},
		{http.MethodGet, "/unprocessed/replay/0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10"},
	} {
		api := newTestAPI(t, "", http.StatusOK, unprocessedPage)
		status, body, header := api.do(tt.method, tt.target, "bnpb-ops", opsScopes)
		e, _ := body["error"].(map[string]any)
		if status != http.StatusServiceUnavailable || e["code"] != "UPSTREAM_UNAVAILABLE" || e["message"] != "ops access not configured" {
			t.Errorf("%s %s: got %d %v, want 503 ops access not configured", tt.method, tt.target, status, body)
		}
		if header.Get("Retry-After") != "" || len(api.seen) != 0 {
			t.Errorf("%s %s: Retry-After %q, aggregator calls %d; want neither", tt.method, tt.target, header.Get("Retry-After"), len(api.seen))
		}
		if status, _, _ := api.do(tt.method, tt.target, "media", "hazard:read:summary"); status != http.StatusForbidden {
			t.Errorf("%s %s as media: got %d, want 403", tt.method, tt.target, status)
		}
	}
}

const hazardPage = `{"data":[{"hazard_id":"7d0e6f1c-0d7b-5c55-9a0e-5f0a1d2b3c4d","source":"PVMBG","hazard_type":"VOLCANIC",` +
	`"severity":"HIGH","area_name":"Merapi","occurred_at":"2026-11-18T03:00:00Z","ingested_at":"2026-11-18T03:00:05Z",` +
	`"source_ref_id":"r-1","latitude":-7.54,"longitude":110.44,"attributes":{}}],"next_cursor":null}`

// The extra ops scopes do not take raw fields away from bnpb-ops; media still sees only the summary.
func TestOpsTokenStillReadsRawHazardFields(t *testing.T) {
	for _, tt := range []struct {
		sub, scope, target string
		status             int
		hasLatitude        bool
	}{
		{"bnpb-ops", opsScopes, "/hazards", http.StatusOK, true},
		{"bnpb-ops", opsScopes, "/hazards?fields=hazard_id,latitude", http.StatusOK, true},
		{"field-team", "hazard:read:raw", "/hazards", http.StatusOK, true},
		{"media", "hazard:read:summary", "/hazards", http.StatusOK, false},
		{"media", "hazard:read:summary", "/hazards?fields=latitude", http.StatusForbidden, false},
	} {
		api := newTestAPI(t, testOpsToken, http.StatusOK, hazardPage)
		status, body, _ := api.do(http.MethodGet, tt.target, tt.sub, tt.scope)
		if status != tt.status {
			t.Errorf("%s %s: got %d %v, want %d", tt.sub, tt.target, status, body, tt.status)
			continue
		}
		data, _ := body["data"].([]any)
		var hasLatitude bool
		if len(data) == 1 {
			_, hasLatitude = data[0].(map[string]any)["latitude"]
		}
		if hasLatitude != tt.hasLatitude {
			t.Errorf("%s %s: latitude present = %v, want %v (%v)", tt.sub, tt.target, hasLatitude, tt.hasLatitude, body)
		}
	}
}
