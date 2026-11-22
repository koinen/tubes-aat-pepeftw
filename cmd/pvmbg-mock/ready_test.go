package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// During a simulated outage /health reports it, but /ready stays 200: the pod must stay behind its
// Service, so the aggregator meets the hang itself and the admin call that ends the outage can reach
// the pod (P5 practice game day, 2026-11-22: "outage off" found no endpoint).
func TestReadyIgnoresTheSimulatedOutage(t *testing.T) {
	h := newMockHarness(t)
	for _, mode := range []string{"hang", "error"} {
		req := httptest.NewRequest("POST", "/admin/outage", strings.NewReader(`{"enabled":true,"mode":"`+mode+`"}`))
		req.Header.Set("X-Admin-Key", "k")
		if resp, err := h.app.Test(req); err != nil || resp.StatusCode != 200 {
			t.Fatalf("enable %s outage: %v %v", mode, resp, err)
		}
		if code := h.get("/health"); code != 503 {
			t.Errorf("%s outage: /health %d, want 503", mode, code)
		}
		if code := h.get("/ready"); code != 200 {
			t.Errorf("%s outage: /ready %d, want 200", mode, code)
		}
	}
}

func (h *mockHarness) get(path string) int {
	h.t.Helper()
	resp, err := h.app.Test(httptest.NewRequest("GET", path, nil))
	if err != nil {
		h.t.Fatal(err)
	}
	return resp.StatusCode
}
