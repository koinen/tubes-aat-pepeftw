package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func reportJSON(t *testing.T, version int) map[string]any {
	t.Helper()
	g := newGenerator(rand.New(rand.NewPCG(1, 2)))
	r := g.make(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC), "PVMBG-VR-TEST", version)
	r.EruptionCount24h = 12
	r.AshColumnHeightM = 1500
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal v%d: %v", version, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal v%d: %v", version, err)
	}
	return m
}

func TestSchemaVersionWireShape(t *testing.T) {
	tests := []struct {
		version int
		check   func(t *testing.T, m map[string]any)
	}{
		{schemaBase, func(t *testing.T, m map[string]any) {
			mustHave(t, m, "alert_level", "eruption_count_24h", "ash_column_height_m")
			mustLack(t, m, "confidence_level", "alert_status")
		}},
		{schemaAdditive, func(t *testing.T, m map[string]any) {
			mustHave(t, m, "alert_level", "confidence_level")
		}},
		{schemaRenameField, func(t *testing.T, m map[string]any) {
			mustHave(t, m, "alert_status", "confidence_level")
			mustLack(t, m, "alert_level")
		}},
		{schemaTypeChange, func(t *testing.T, m map[string]any) {
			if got, ok := m["eruption_count_24h"].(string); !ok || got != "12" {
				t.Errorf(`eruption_count_24h = %#v, want "12"`, m["eruption_count_24h"])
			}
			mustHave(t, m, "alert_level", "ash_column_height_m")
		}},
		{schemaMissingRequired, func(t *testing.T, m map[string]any) {
			mustLack(t, m, "ash_column_height_m")
			mustHave(t, m, "report_id", "volcano_id", "alert_level", "reported_at", "eruption_count_24h")
		}},
	}
	for _, tt := range tests {
		t.Run(schemaModes[tt.version], func(t *testing.T) {
			tt.check(t, reportJSON(t, tt.version))
		})
	}
}

func TestReportKeepsVersionItWasGeneratedWith(t *testing.T) {
	store := &Store{}
	state := newState(schemaRenameField)
	g := newGenerator(rand.New(rand.NewPCG(1, 2)))
	store.add(g.make(time.Now(), "during-v3", state.SchemaVersion()))
	state.SetSchemaVersion(schemaBase)
	store.add(g.make(time.Now(), "after-switch-back", state.SchemaVersion()))

	b, _ := json.Marshal(store.Since(time.Time{}))
	var got []map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	byID := map[string]map[string]any{}
	for _, r := range got {
		byID[r["report_id"].(string)] = r
	}
	mustHave(t, byID["during-v3"], "alert_status")
	mustLack(t, byID["during-v3"], "alert_level")
	mustHave(t, byID["after-switch-back"], "alert_level")
}

func TestAdminSchemaVersion(t *testing.T) {
	cfg := Config{TokenSecret: "s", AdminKey: "k", HangMax: time.Second}
	state := newState(schemaBase)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := newApp(context.Background(), cfg, &Store{}, state, log)

	post := func(body string) (int, string) {
		req := httptest.NewRequest("POST", "/admin/schema-version", strings.NewReader(body))
		req.Header.Set("X-Admin-Key", "k")
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	for v := schemaBase; v <= schemaMissingRequired; v++ {
		code, body := post(`{"version":` + string(rune('0'+v)) + `}`)
		if code != 200 || !strings.Contains(body, `"schema_mode":"`+schemaModes[v]+`"`) {
			t.Errorf("version %d: got %d %s", v, code, body)
		}
		if state.SchemaVersion() != v {
			t.Errorf("version %d: state is %d", v, state.SchemaVersion())
		}
	}
	for _, bad := range []string{`{"version":0}`, `{"version":6}`, `{"version":"3"}`, `{}`} {
		if code, body := post(bad); code != 400 {
			t.Errorf("%s: got %d %s, want 400", bad, code, body)
		}
	}
}

func mustHave(t *testing.T, m map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing field %q in %v", k, m)
		}
	}
}

func mustLack(t *testing.T, m map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, ok := m[k]; ok {
			t.Errorf("unexpected field %q in %v", k, m)
		}
	}
}
