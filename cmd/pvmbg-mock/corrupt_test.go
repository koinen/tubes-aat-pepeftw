package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gofiber/fiber/v3"
)

type mockHarness struct {
	t     *testing.T
	app   *fiber.App
	state *State
	token string
}

func newMockHarness(t *testing.T) *mockHarness {
	t.Helper()
	cfg := Config{TokenSecret: "s", AdminKey: "k", HangMax: time.Second}
	store := &Store{}
	g := newGenerator(rand.New(rand.NewPCG(1, 2)))
	for i := range 3 {
		store.add(g.make(time.Now(), "PVMBG-VR-T"+string(rune('0'+i)), schemaBase))
	}
	state := newState(schemaBase)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &mockHarness{t: t, app: newApp(context.Background(), cfg, store, state, log), state: state, token: makeToken("s", "aggregator")}
}

func (h *mockHarness) admin(body string) (int, string) {
	h.t.Helper()
	req := httptest.NewRequest("POST", "/admin/corrupt", strings.NewReader(body))
	req.Header.Set("X-Admin-Key", "k")
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.app.Test(req)
	if err != nil {
		h.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (h *mockHarness) poll() (int, []byte) {
	h.t.Helper()
	req := httptest.NewRequest("GET", "/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := h.app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		h.t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp.StatusCode, b
}

func mustBeValidResponse(t *testing.T, body []byte) {
	t.Helper()
	var r struct {
		Reports []json.RawMessage `json:"reports"`
	}
	if err := json.Unmarshal(body, &r); err != nil || len(r.Reports) != 3 {
		t.Fatalf("want a normal response with 3 reports, got err=%v body=%.200s", err, body)
	}
}

func TestCorruptKinds(t *testing.T) {
	tests := []struct {
		kind  string
		check func(t *testing.T, body []byte)
	}{
		{corruptTruncated, func(t *testing.T, body []byte) {
			var v any
			if err := json.Unmarshal(body, &v); err == nil {
				t.Errorf("truncated body decodes: %.200s", body)
			}
			if !bytes.HasPrefix(body, []byte(`{"generated_at":`)) {
				t.Errorf("truncated body should be a prefix of a real response: %.200s", body)
			}
		}},
		{corruptDeep, func(t *testing.T, body []byte) {
			if got := maxDepth(body); got < corruptDepth {
				t.Errorf("depth %d, want >= %d", got, corruptDepth)
			}
		}},
		{corruptInvalidUTF8, func(t *testing.T, body []byte) {
			if utf8.Valid(body) {
				t.Errorf("body is valid UTF-8")
			}
			fixed := bytes.ToValidUTF8(body, nil)
			mustBeValidResponse(t, fixed)
		}},
		{corruptOversized, func(t *testing.T, body []byte) {
			if len(body) < oversizedBytes {
				t.Errorf("size %d, want >= %d", len(body), oversizedBytes)
			}
			if !json.Valid(body) {
				t.Errorf("oversized body should still be syntactically valid JSON")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			h := newMockHarness(t)
			if code, body := h.admin(`{"kind":"` + tt.kind + `","count":1}`); code != 200 ||
				!strings.Contains(body, `"corrupt":{"kind":"`+tt.kind+`","remaining":1}`) {
				t.Fatalf("admin: %d %s", code, body)
			}
			code, body := h.poll()
			if code != 200 {
				t.Fatalf("corrupt poll status %d", code)
			}
			tt.check(t, body)

			_, next := h.poll()
			mustBeValidResponse(t, next)
		})
	}
}

func TestCorruptCountsDownThenTurnsOff(t *testing.T) {
	h := newMockHarness(t)
	if code, body := h.admin(`{"kind":"truncated","count":2}`); code != 200 {
		t.Fatalf("admin: %d %s", code, body)
	}
	for i := range 2 {
		if _, body := h.poll(); json.Valid(body) {
			t.Fatalf("poll %d should be corrupted", i+1)
		}
	}
	_, body := h.poll()
	mustBeValidResponse(t, body)
	if s := h.state.Snapshot(); s.Corrupt.Kind != "" || s.Corrupt.Remaining != 0 {
		t.Errorf("state after count ran out: %+v", s.Corrupt)
	}
}

func TestCorruptCountDefaultsToOneAndZeroClears(t *testing.T) {
	h := newMockHarness(t)
	if code, body := h.admin(`{"kind":"deep"}`); code != 200 || !strings.Contains(body, `"remaining":1`) {
		t.Fatalf("default count: %d %s", code, body)
	}
	if code, body := h.admin(`{"kind":"deep","count":0}`); code != 200 || !strings.Contains(body, `"corrupt":{"remaining":0}`) {
		t.Fatalf("count 0: %d %s", code, body)
	}
	_, body := h.poll()
	mustBeValidResponse(t, body)
}

func TestCorruptRejectsBadRequests(t *testing.T) {
	h := newMockHarness(t)
	for _, bad := range []string{`{}`, `{"kind":"gzip"}`, `{"kind":"deep","count":-1}`, `{"kind":"deep","count":101}`, `{"kind":"deep","count":"2"}`, `not json`} {
		if code, body := h.admin(bad); code != 400 {
			t.Errorf("%s: got %d %s, want 400", bad, code, body)
		}
	}
	req := httptest.NewRequest("POST", "/admin/corrupt", strings.NewReader(`{"kind":"deep"}`))
	resp, err := h.app.Test(req)
	if err != nil || resp.StatusCode != 401 {
		t.Errorf("no admin key: %v %v, want 401", resp.StatusCode, err)
	}
}

func TestCorruptNotConsumedByUnauthorizedPoll(t *testing.T) {
	h := newMockHarness(t)
	h.admin(`{"kind":"truncated","count":1}`)
	req := httptest.NewRequest("GET", "/volcanic-reports", nil)
	if resp, _ := h.app.Test(req); resp.StatusCode != 401 {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
	if _, body := h.poll(); json.Valid(body) {
		t.Errorf("the authorized poll should still get the corrupted body")
	}
}

func maxDepth(b []byte) int {
	depth, max, inString, escaped := 0, 0, false, false
	for _, c := range b {
		switch {
		case escaped:
			escaped = false
		case inString:
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{' || c == '[':
			depth++
			if depth > max {
				max = depth
			}
		case c == '}' || c == ']':
			depth--
		}
	}
	return max
}
