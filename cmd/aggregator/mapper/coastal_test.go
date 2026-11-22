package mapper

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

var (
	coastalPage = CoastalPage{
		GeneratedAt: time.Date(2026, 11, 12, 2, 43, 5, 0, time.UTC),
		FetchedAt:   time.Date(2026, 11, 18, 9, 30, 1, 250_000_000, time.UTC),
	}
	padang = Station{Name: "Padang", Province: "Sumatera Barat", Latitude: -0.9492, Longitude: 100.3543}
)

func knownStations(id string) (Station, bool) {
	if id == "stn-padang-01" {
		return padang, true
	}
	return Station{}, false
}

func coastal(t *testing.T, payload string) CoastalResult {
	t.Helper()
	return Coastal(json.RawMessage(payload), coastalPage, knownStations)
}

func TestCoastalMapsValidObservation(t *testing.T) {
	r := coastal(t, `{"observation_id":"obs-000812","station_id":"stn-padang-01","observed_at":"2026-11-12T02:43:00Z",
		"sea_level_anomaly_m":0.42,"wave_height_m":1.8,"quality":"ok","critical":false}`)

	if r.Violation != nil {
		t.Fatalf("unexpected violation: %v", r.Violation)
	}
	if r.Hazard == nil {
		t.Fatal("want a HazardEvent")
	}
	h := *r.Hazard
	want := model.HazardEvent{
		HazardID:    HazardID(model.SourceSensorPantai, "obs-000812"),
		Source:      model.SourceSensorPantai,
		SourceRefID: "obs-000812",
		HazardType:  model.HazardCoastal,
		Severity:    model.SeverityNormal,
		AreaName:    "Padang",
		Latitude:    -0.9492,
		Longitude:   100.3543,
		OccurredAt:  coastalPage.FetchedAt,
		IngestedAt:  coastalPage.FetchedAt,
	}
	got := h
	got.Attributes = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("HazardEvent mismatch\n got %+v\nwant %+v", got, want)
	}
	wantAttrs := map[string]any{
		"station_id":          "stn-padang-01",
		"province":            "Sumatera Barat",
		"sea_level_anomaly_m": 0.42,
		"wave_height_m":       1.8,
		"quality":             "ok",
		"critical":            false,
		"verified":            true,
		"observed_at":         "2026-11-12T02:43:00Z",
		"fetched_at":          "2026-11-18T09:30:01.250Z",
	}
	if len(h.Attributes) != len(wantAttrs) {
		t.Errorf("attributes = %v, want %v", h.Attributes, wantAttrs)
	}
	for k, v := range wantAttrs {
		if h.Attributes[k] != v {
			t.Errorf("attributes[%q] = %#v, want %#v", k, h.Attributes[k], v)
		}
	}
}

// obs builds an observation from the valid one, with fields overridden or (value nil) removed.
func obs(t *testing.T, overrides map[string]any) string {
	t.Helper()
	m := map[string]any{
		"observation_id": "obs-000812", "station_id": "stn-padang-01", "observed_at": "2026-11-12T02:43:00Z",
		"sea_level_anomaly_m": 0.42, "wave_height_m": 1.8, "quality": "ok", "critical": false,
	}
	for k, v := range overrides {
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCoastalContract(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]any
		valid     bool
	}{
		{"integers where floats are expected", map[string]any{"sea_level_anomaly_m": 1, "wave_height_m": 2}, true},
		{"quality suspect", map[string]any{"quality": "suspect"}, true},
		{"wave height at 0", map[string]any{"wave_height_m": 0}, true},
		{"wave height at 30", map[string]any{"wave_height_m": 30}, true},
		{"sea level anomaly at -10", map[string]any{"sea_level_anomaly_m": -10}, true},
		{"observed_at equal to generated_at", map[string]any{"observed_at": "2026-11-12T02:43:05Z"}, true},

		{"observation_id missing", map[string]any{"observation_id": nil}, false},
		{"station_id missing", map[string]any{"station_id": nil}, false},
		{"observed_at missing", map[string]any{"observed_at": nil}, false},
		{"sea_level_anomaly_m missing", map[string]any{"sea_level_anomaly_m": nil}, false},
		{"wave_height_m missing", map[string]any{"wave_height_m": nil}, false},
		{"quality missing", map[string]any{"quality": nil}, false},
		{"critical missing", map[string]any{"critical": nil}, false},
		{"number sent as string", map[string]any{"wave_height_m": "1.8"}, false},
		{"critical sent as string", map[string]any{"critical": "false"}, false},
		{"observed_at not RFC 3339", map[string]any{"observed_at": "12/11/2026"}, false},
		{"unknown station", map[string]any{"station_id": "stn-unknown-99"}, false},
		{"unknown quality", map[string]any{"quality": "bad"}, false},
		{"negative wave height", map[string]any{"wave_height_m": -0.1}, false},
		{"wave height above 30", map[string]any{"wave_height_m": 30.5}, false},
		{"sea level anomaly above 10", map[string]any{"sea_level_anomaly_m": 10.2}, false},
		{"sea level anomaly below -10", map[string]any{"sea_level_anomaly_m": -11}, false},
		{"observed_at after generated_at", map[string]any{"observed_at": "2026-11-12T02:43:06Z"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := coastal(t, obs(t, tt.overrides))
			if tt.valid {
				if r.Violation != nil || r.Hazard == nil {
					t.Fatalf("want valid, got hazard=%v violation=%v", r.Hazard != nil, r.Violation)
				}
				return
			}
			if r.Hazard != nil {
				t.Errorf("want no HazardEvent, got %+v", *r.Hazard)
			}
			if r.Violation == nil {
				t.Fatal("want a violation")
			}
			if r.Violation.Code != model.ReasonContractViolation {
				t.Errorf("code = %q, want %q", r.Violation.Code, model.ReasonContractViolation)
			}
		})
	}
}

func TestCoastalRejectsNonObjectItem(t *testing.T) {
	for _, payload := range []string{`null`, `5`, `"obs"`, `[]`} {
		r := coastal(t, payload)
		if r.Hazard != nil || r.Violation == nil || r.Violation.Code != model.ReasonContractViolation {
			t.Errorf("%s: want only a contract_violation, got hazard=%v violation=%v", payload, r.Hazard != nil, r.Violation)
		}
	}
}

func TestCoastalCriticalWithBadValueIsUnverifiedAlert(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]any
		missing   string // attribute left out because its value could not be read
	}{
		{"negative wave height", map[string]any{"wave_height_m": -1}, ""},
		{"unknown quality", map[string]any{"quality": "bad"}, ""},
		{"observed_at after generated_at", map[string]any{"observed_at": "2026-11-12T02:50:00Z"}, ""},
		{"wave height missing", map[string]any{"wave_height_m": nil}, "wave_height_m"},
		{"sea level sent as string", map[string]any{"sea_level_anomaly_m": "0.4"}, "sea_level_anomaly_m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.overrides["critical"] = true
			r := coastal(t, obs(t, tt.overrides))
			if r.Violation == nil || r.Violation.Code != model.ReasonContractViolation {
				t.Fatalf("want a contract_violation to park, got %v", r.Violation)
			}
			if r.Hazard == nil {
				t.Fatal("want an unverified alert")
			}
			h := *r.Hazard
			if h.Severity != model.SeverityAwas || h.HazardID != HazardID(model.SourceSensorPantai, "obs-000812") {
				t.Errorf("severity=%s hazard_id=%s, want AWAS for obs-000812", h.Severity, h.HazardID)
			}
			if h.Attributes["verified"] != false {
				t.Errorf("verified = %#v, want false", h.Attributes["verified"])
			}
			if h.Attributes["violation"] != r.Violation.Reason {
				t.Errorf("violation attribute = %#v, want the parked reason %q", h.Attributes["violation"], r.Violation.Reason)
			}
			if tt.missing != "" {
				if v, ok := h.Attributes[tt.missing]; ok {
					t.Errorf("attributes[%q] = %#v, want it left out", tt.missing, v)
				}
			}
		})
	}
}

func TestCoastalCriticalWithBadIdentityIsOnlyParked(t *testing.T) {
	tests := map[string]map[string]any{
		"observation_id missing":   {"observation_id": nil},
		"unknown station":          {"station_id": "stn-unknown-99"},
		"observed_at missing":      {"observed_at": nil},
		"observed_at not RFC 3339": {"observed_at": "kemarin"},
	}
	for name, overrides := range tests {
		t.Run(name, func(t *testing.T) {
			overrides["critical"] = true
			r := coastal(t, obs(t, overrides))
			if r.Hazard != nil {
				t.Errorf("want no alert, got %+v", *r.Hazard)
			}
			if r.Violation == nil {
				t.Fatal("want a violation")
			}
		})
	}
}

func TestCoastalNonCriticalViolationListsEveryProblem(t *testing.T) {
	r := coastal(t, obs(t, map[string]any{"wave_height_m": -1, "quality": "bad"}))
	if r.Violation == nil {
		t.Fatal("want a violation")
	}
	for _, field := range []string{"wave_height_m", "quality"} {
		if !strings.Contains(r.Violation.Reason, field) {
			t.Errorf("reason %q does not mention %s", r.Violation.Reason, field)
		}
	}
}
