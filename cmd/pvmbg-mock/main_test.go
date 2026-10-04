package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

const (
	testSecret   = "secret-uji"
	testAdminKey = "admin-uji"
)

type harness struct {
	t     *testing.T
	app   *fiber.App
	store *Store
	state *State
	gen   *Generator
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store := &Store{}
	gen := seed(store, 25)
	state := newState(1)
	cfg := Config{TokenSecret: testSecret, AdminKey: testAdminKey, HangMax: 2 * time.Second}
	app := newApp(context.Background(), cfg, store, state, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &harness{t: t, app: app, store: store, state: state, gen: gen}
}

func (h *harness) do(req *http.Request, timeout time.Duration) *http.Response {
	h.t.Helper()
	resp, err := h.app.Test(req, fiber.TestConfig{Timeout: timeout, FailOnTimeout: true})
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

func dataReq(header map[string]string) *http.Request {
	req := httptest.NewRequest("GET", "/volcanic-reports", nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	return req
}

func adminReq(path, body string) *http.Request {
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Key", testAdminKey)
	return req
}

func validAuth() map[string]string {
	return map[string]string{"Authorization": "Bearer " + makeToken(testSecret, "bnpb-aggregator")}
}

type listResp struct {
	Reports     []map[string]any `json:"reports"`
	GeneratedAt string           `json:"generated_at"`
}

func errCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	var b struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		t.Fatal(err)
	}
	return b.Error.Code
}

func TestTokenRoundTrip(t *testing.T) {
	tok := makeToken(testSecret, "bnpb-aggregator")
	if !strings.HasPrefix(tok, "pvmbg.bnpb-aggregator.") {
		t.Fatalf("format token salah: %s", tok)
	}
	if id, ok := verifyToken(testSecret, tok); !ok || id != "bnpb-aggregator" {
		t.Fatal("token sah ditolak")
	}
	if _, ok := verifyToken("secret-lain", tok); ok {
		t.Fatal("token dengan secret lain diterima")
	}
	if _, ok := verifyToken(testSecret, "pvmbg.penyusup."+signToken(testSecret, "bnpb-aggregator")); ok {
		t.Fatal("tanda tangan milik client lain diterima")
	}
}

func TestAuth(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name   string
		header map[string]string
		status int
		code   string
	}{
		{"tanpa kredensial", nil, 401, "UNAUTHORIZED"},
		{"key BMKG", map[string]string{"X-BMKG-Key": "bmkg_key_apa_pun"}, 403, "FORBIDDEN"},
		{"bearer bukan format pvmbg", map[string]string{"Authorization": "Bearer abc"}, 401, "UNAUTHORIZED"},
		{"tanda tangan salah", map[string]string{"Authorization": "Bearer pvmbg.bnpb-aggregator.palsu"}, 401, "UNAUTHORIZED"},
		{"token sah", validAuth(), 200, ""},
	}
	for _, tc := range cases {
		resp := h.do(dataReq(tc.header), time.Second)
		if resp.StatusCode != tc.status {
			t.Errorf("%s: status %d, ingin %d", tc.name, resp.StatusCode, tc.status)
			continue
		}
		if tc.code != "" && errCode(t, resp) != tc.code {
			t.Errorf("%s: code salah", tc.name)
		}
	}
}

func TestSeedAndSince(t *testing.T) {
	h := newHarness(t)
	resp := h.do(dataReq(validAuth()), time.Second)
	var all listResp
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		t.Fatal(err)
	}
	if len(all.Reports) < 20 {
		t.Fatalf("seed hanya %d laporan, minimal 20", len(all.Reports))
	}
	if all.GeneratedAt == "" {
		t.Fatal("generated_at kosong")
	}
	pivot := all.Reports[5]["reported_at"].(string)
	req := httptest.NewRequest("GET", "/volcanic-reports?since="+pivot, nil)
	req.Header.Set("Authorization", validAuth()["Authorization"])
	var part listResp
	if err := json.NewDecoder(h.do(req, time.Second).Body).Decode(&part); err != nil {
		t.Fatal(err)
	}
	if len(part.Reports) != len(all.Reports)-5 || part.Reports[0]["report_id"] != all.Reports[5]["report_id"] {
		t.Fatalf("since harus inklusif: dapat %d laporan", len(part.Reports))
	}
}

func TestVolcanoIDsMatchReference(t *testing.T) {
	seen := map[string]bool{}
	for _, v := range volcanoIDs {
		if seen[v.id] {
			t.Fatalf("volcano_id ganda: %s", v.id)
		}
		seen[v.id] = true
	}
	h := newHarness(t)
	for _, r := range h.store.Since(time.Time{}) {
		if !seen[r.VolcanoID] {
			t.Fatalf("laporan memakai volcano_id tak dikenal %s", r.VolcanoID)
		}
	}
}

func TestSchemaVersionAddsConfidenceOnlyToNewReports(t *testing.T) {
	h := newHarness(t)
	before := h.store.Count()

	req := httptest.NewRequest("POST", "/admin/schema-version", strings.NewReader(`{"version":2}`))
	if resp := h.do(req, time.Second); resp.StatusCode != 401 {
		t.Fatalf("schema-version tanpa X-Admin-Key harus 401, dapat %d", resp.StatusCode)
	}
	if resp := h.do(adminReq("/admin/schema-version", `{"version":3}`), time.Second); resp.StatusCode != 400 {
		t.Fatalf("version 3 harus 400, dapat %d", resp.StatusCode)
	}

	resp := h.do(adminReq("/admin/schema-version", `{"version":2}`), time.Second)
	var st adminState
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil || st.SchemaVersion != 2 {
		t.Fatalf("schema version tidak berubah: %+v %v", st, err)
	}

	h.store.add(h.gen.make(time.Now(), "PVMBG-VR-UJI", h.state.SchemaVersion() == 2))

	var list listResp
	if err := json.NewDecoder(h.do(dataReq(validAuth()), time.Second).Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	for i, r := range list.Reports {
		_, has := r["confidence_level"]
		if i < before && has {
			t.Fatalf("laporan lama %v tidak boleh berubah", r["report_id"])
		}
		if i >= before && !has {
			t.Fatalf("laporan baru %v harus punya confidence_level", r["report_id"])
		}
	}
}

func TestOutageErrorMode(t *testing.T) {
	h := newHarness(t)
	h.do(adminReq("/admin/outage", `{"enabled":true,"mode":"error"}`), time.Second)

	resp := h.do(dataReq(validAuth()), time.Second)
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || errCode(t, resp) != "UPSTREAM_UNAVAILABLE" {
		t.Fatalf("outage error harus 503 UPSTREAM_UNAVAILABLE + Retry-After, dapat %d", resp.StatusCode)
	}
	if resp := h.do(httptest.NewRequest("GET", "/health", nil), time.Second); resp.StatusCode != 503 {
		t.Fatalf("health saat outage harus 503, dapat %d", resp.StatusCode)
	}

	h.do(adminReq("/admin/outage", `{"enabled":false}`), time.Second)
	if resp := h.do(dataReq(validAuth()), time.Second); resp.StatusCode != 200 {
		t.Fatalf("setelah pulih harus 200, dapat %d", resp.StatusCode)
	}
}

func TestOutageHangModeBlocksThenRecovers(t *testing.T) {
	h := newHarness(t)
	h.do(adminReq("/admin/outage", `{"enabled":true}`), time.Second) // mode default: hang

	if _, err := h.app.Test(dataReq(validAuth()), fiber.TestConfig{Timeout: 300 * time.Millisecond, FailOnTimeout: true}); err == nil {
		t.Fatal("outage hang seharusnya tidak memberi respons")
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		h.state.SetOutage(false, "")
	}()
	start := time.Now()
	resp := h.do(dataReq(validAuth()), 2*time.Second)
	if resp.StatusCode != 200 {
		t.Fatalf("setelah pulih harus 200, dapat %d", resp.StatusCode)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("request seharusnya tertahan sampai outage dimatikan")
	}
}

func TestAdminWorksDuringOutage(t *testing.T) {
	h := newHarness(t)
	h.do(adminReq("/admin/outage", `{"enabled":true,"mode":"hang"}`), time.Second)
	if resp := h.do(adminReq("/admin/schema-version", `{"version":2}`), time.Second); resp.StatusCode != 200 {
		t.Fatalf("admin harus tetap jalan saat outage, dapat %d", resp.StatusCode)
	}
	if resp := h.do(adminReq("/admin/outage", `{"enabled":false}`), time.Second); resp.StatusCode != 200 {
		t.Fatalf("outage harus bisa dimatikan, dapat %d", resp.StatusCode)
	}
}

func TestVolcanoIDsMatchAggregatorMigration(t *testing.T) {
	sql, err := os.ReadFile("../aggregator/migrations/002_volcano_ref_seed.sql")
	if err != nil {
		t.Skipf("migration volcano_ref tidak ditemukan: %v", err)
	}
	var ref []string
	for _, m := range regexp.MustCompile(`\('([A-Z0-9_]+)'`).FindAllStringSubmatch(string(sql), -1) {
		ref = append(ref, m[1])
	}
	var mock []string
	for _, v := range volcanoIDs {
		mock = append(mock, v.id)
	}
	slices.Sort(ref)
	slices.Sort(mock)
	if !slices.Equal(ref, mock) {
		t.Fatalf("volcano_id berbeda.\nmock:      %v\nmigration: %v", mock, ref)
	}
}