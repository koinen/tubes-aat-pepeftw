package mapper

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// An unset PVMBG_ADAPTATIONS is the normal state: the declaration from Q3.1 applies unchanged.
func TestNoAdaptationsByDefault(t *testing.T) {
	a, err := ParseVolcanicAdaptations("")
	if err != nil {
		t.Fatalf("empty rules: %v", err)
	}
	if got := a.String(); got != "" {
		t.Errorf("String() = %q, want empty", got)
	}
}

// Two pods with the same rules written with different spacing must agree that their configuration is the same.
func TestDeclaredRulesHaveACanonicalForm(t *testing.T) {
	a, err := ParseVolcanicAdaptations(" rename:alert_status=alert_level, numeric_string:eruption_count_24h ,optional:ash_column_height_m ")
	if err != nil {
		t.Fatalf("valid rules: %v", err)
	}
	want := "rename:alert_status=alert_level,numeric_string:eruption_count_24h,optional:ash_column_height_m"
	if got := a.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// A declaration the mapper cannot apply exactly is refused, so the pod does not start with it and
// a rolling update keeps the old pods. A typo must fail here, not silently adapt nothing.
func TestUnusableDeclarationIsRefused(t *testing.T) {
	for _, s := range []string{
		"alert_level",                    // no rule kind
		"frobnicate:alert_level",         // unknown rule kind
		"numeric_string:",                // no field
		"rename:alert_status",            // rename without a canonical name
		"rename:=alert_level",            // rename without an incoming name
		"rename:alert_level=alert_level", // rename onto itself
		"rename:alert_status=alert_levl", // canonical name the mapper does not read
		"numeric_string:eruption_count",  // field the mapper does not read
		"numeric_string:alert_level",     // not a numeric field
		"optional:report_id",             // identity fields are never optional
		"optional:alert_level",           // severity cannot be absent
		"rename:alert_status=alert_level,rename:alert_status=reported_at", // two rules for one incoming field
		"optional:ash_column_height_m,optional:ash_column_height_m",       // declared twice
	} {
		if _, err := ParseVolcanicAdaptations(s); err == nil {
			t.Errorf("ParseVolcanicAdaptations(%q) accepted, want an error", s)
		}
	}
}

func mustRules(t *testing.T, s string) VolcanicAdaptations {
	t.Helper()
	a, err := ParseVolcanicAdaptations(s)
	if err != nil {
		t.Fatalf("rules %q: %v", s, err)
	}
	return a
}

// With its declared rule, each breaking PVMBG version maps to the HazardEvent the v1 report would give.
func TestDeclaredRuleMakesABreakingVersionReadable(t *testing.T) {
	lookup := func(string) (Volcano, bool) { return Volcano{Name: "Merapi", Latitude: -7.54, Longitude: 110.44}, true }
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name, rules, payload string
		wantAttributes       map[string]any
	}{
		{
			"v3 renamed field", "rename:alert_status=alert_level",
			`{"report_id":"r","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`,
			map[string]any{"volcano_id": "MERAPI", "eruption_count_24h": 12.0, "ash_column_height_m": 1500.0, "confidence_level": 0.8},
		},
		{
			"v4 number sent as string", "numeric_string:eruption_count_24h",
			`{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":"12","ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`,
			map[string]any{"volcano_id": "MERAPI", "eruption_count_24h": 12.0, "ash_column_height_m": 1500.0, "confidence_level": 0.8},
		},
		{
			"v5 missing required field", "optional:ash_column_height_m",
			`{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`,
			map[string]any{"volcano_id": "MERAPI", "eruption_count_24h": 12.0, "confidence_level": 0.8},
		},
		{
			"optional field sent as null", "optional:ash_column_height_m",
			`{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":null,"reported_at":"2026-11-18T00:00:00Z"}`,
			map[string]any{"volcano_id": "MERAPI", "eruption_count_24h": 12.0, "ash_column_height_m": nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := Volcanic(json.RawMessage(tt.payload), lookup, mustRules(t, tt.rules), now)
			if err != nil {
				t.Fatalf("want accepted, got %v", err)
			}
			if h.Severity != model.SeveritySiaga || h.SourceRefID != "r" || h.AreaName != "Merapi" {
				t.Errorf("got severity %q, ref %q, area %q; want SIAGA, r, Merapi", h.Severity, h.SourceRefID, h.AreaName)
			}
			if !reflect.DeepEqual(h.Attributes, tt.wantAttributes) {
				t.Errorf("attributes = %v, want %v", h.Attributes, tt.wantAttributes)
			}
		})
	}
}

// A rule makes exactly its declared change readable and nothing else. Whatever it can't read exactly
// is still a breaking change (schema_incompatible), so it is parked instead of guessed at.
func TestRulesDoNotGuess(t *testing.T) {
	lookup := func(string) (Volcano, bool) { return Volcano{Name: "Merapi"}, true }
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	all := "rename:alert_status=alert_level,numeric_string:eruption_count_24h,optional:ash_column_height_m"

	tests := []struct{ name, rules, payload string }{
		{"both old and new name", all, `{"report_id":"r","volcano_id":"MERAPI","alert_status":"Awas","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
		{"text that is not a number", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":"abc","ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
		{"empty text", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":"","ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
		{"number with spaces", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":" 12","ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
		{"number out of range", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":"1e999","ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
		{"optional field present with wrong type", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":"x","reported_at":"2026-11-18T00:00:00Z"}`},
		{"string on a field without numeric_string", all, `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":"1500","reported_at":"2026-11-18T00:00:00Z"}`},
		{"v3 under a rule for another change", "numeric_string:eruption_count_24h", `{"report_id":"r","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":12,"ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Volcanic(json.RawMessage(tt.payload), lookup, mustRules(t, tt.rules), now)
			var re *RejectError
			if !errors.As(err, &re) || re.Code != model.ReasonSchemaIncompatible {
				t.Fatalf("want a schema_incompatible rejection, got %v", err)
			}
		})
	}
}

// Rules stay in place after the source returns to a compatible version, so they must not change a v1 or v2 report.
func TestRulesLeaveCompatibleVersionsUnchanged(t *testing.T) {
	lookup := func(string) (Volcano, bool) { return Volcano{Name: "Merapi"}, true }
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	rules := mustRules(t, "rename:alert_status=alert_level,numeric_string:eruption_count_24h,optional:ash_column_height_m")

	v2 := `{"report_id":"r","volcano_id":"MERAPI","alert_level":"Awas","eruption_count_24h":3,"ash_column_height_m":800,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.5}`
	h, err := Volcanic(json.RawMessage(v2), lookup, rules, now)
	if err != nil {
		t.Fatalf("v2 under rules: %v", err)
	}
	want := map[string]any{"volcano_id": "MERAPI", "eruption_count_24h": 3.0, "ash_column_height_m": 800.0, "confidence_level": 0.5}
	if h.Severity != model.SeverityAwas || !reflect.DeepEqual(h.Attributes, want) {
		t.Errorf("got severity %q, attributes %v; want AWAS, %v", h.Severity, h.Attributes, want)
	}
}

// The caller learns which rules a report needed, for the adaptation counter and log. Only rules that
// acted are reported, and only for a report that became a HazardEvent.
func TestObserverHearsTheRulesAReportNeeded(t *testing.T) {
	lookup := func(string) (Volcano, bool) { return Volcano{Name: "Merapi"}, true }
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	var heard []string
	rules := mustRules(t, "rename:alert_status=alert_level,numeric_string:eruption_count_24h,optional:ash_column_height_m").
		WithObserver(func(rule, sourceRefID string) { heard = append(heard, rule+" "+sourceRefID) })

	v5 := `{"report_id":"r5","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"reported_at":"2026-11-18T00:00:00Z"}`
	if _, err := Volcanic(json.RawMessage(v5), lookup, rules, now); err != nil {
		t.Fatal(err)
	}
	v3v4 := `{"report_id":"r34","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":"12","ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`
	if _, err := Volcanic(json.RawMessage(v3v4), lookup, rules, now); err != nil {
		t.Fatal(err)
	}
	rejected := `{"report_id":"bad","volcano_id":"MERAPI","alert_status":"Merah","eruption_count_24h":12,"ash_column_height_m":1,"reported_at":"2026-11-18T00:00:00Z"}`
	if _, err := Volcanic(json.RawMessage(rejected), lookup, rules, now); err == nil {
		t.Fatal("unknown alert level accepted")
	}

	want := []string{
		"optional:ash_column_height_m r5",
		"rename:alert_status=alert_level r34",
		"numeric_string:eruption_count_24h r34",
	}
	if !reflect.DeepEqual(heard, want) {
		t.Errorf("heard %q, want %q", heard, want)
	}
}
