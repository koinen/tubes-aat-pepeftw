package mapper

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// These payloads are what the PVMBG mock sends for each schema version. The
// test pins the C2 declaration: compatible changes are accepted, breaking
// changes are rejected (and therefore parked) instead of guessed at.
func TestVolcanicSchemaPolicy(t *testing.T) {
	lookup := func(string) (Volcano, bool) { return Volcano{Name: "Merapi", Latitude: -7.54, Longitude: 110.44}, true }
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		payload  string
		accepted bool
	}{
		{"v1 base", `{"report_id":"r1","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z"}`, true},
		{"v2 new optional field", `{"report_id":"r2","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`, true},
		{"v3 renamed field", `{"report_id":"r3","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`, false},
		{"v4 number sent as string", `{"report_id":"r4","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":"12","ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`, false},
		{"v5 missing required field", `{"report_id":"r5","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":12,"reported_at":"2026-11-18T00:00:00Z","confidence_level":0.8}`, false},
		{"unknown enum value", `{"report_id":"r6","volcano_id":"MERAPI","alert_level":"Merah","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:00:00Z"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := Volcanic(json.RawMessage(tt.payload), lookup, VolcanicAdaptations{}, now)
			if tt.accepted {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				if tt.name == "v2 new optional field" && h.Attributes["confidence_level"] != 0.8 {
					t.Errorf("confidence_level not kept in attributes: %v", h.Attributes)
				}
				return
			}
			var re *RejectError
			if !errors.As(err, &re) {
				t.Fatalf("want *RejectError, got %v", err)
			}
		})
	}
}
