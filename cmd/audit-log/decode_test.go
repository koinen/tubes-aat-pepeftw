package main

import (
	"strings"
	"testing"

	"tubes-aat-pepeftw/internal/poison"
)

const validEvent = `{"event_id":"ev-1","event_type":"hazard.created","hazard_id":"hz-1",` +
	`"emitted_at":"2026-11-18T03:00:00Z","payload":{"hazard_id":"hz-1","source":"PVMBG","severity":"AWAS"}}`

func TestDecodeAcceptsAHazardEvent(t *testing.T) {
	msg, p, err := decode([]byte(validEvent))
	if err != nil {
		t.Fatal(err)
	}
	if msg.HazardID != "hz-1" || p.Severity != "AWAS" {
		t.Errorf("decoded %+v", msg)
	}
}

// A value that fails is forwarded to topic unprocessed under the matching reason code.
func TestDecodeNamesWhyAValueIsUnprocessed(t *testing.T) {
	cases := []struct{ name, value, code string }{
		{"invalid utf-8", strings.Replace(validEvent, "PVMBG", "PV\xffMBG", 1), "invalid_utf8"},
		{"too deep", `{"hazard_id":"hz-1","x":` + strings.Repeat("[", 40) + strings.Repeat("]", 40) + `}`, "too_deep"},
		{"truncated", validEvent[:40], "malformed_json"},
		{"wrong type", `{"hazard_id":7}`, "schema_incompatible"},
		{"no hazard_id", `{"event_id":"ev-1"}`, "schema_incompatible"},
		{"payload wrong type", `{"hazard_id":"hz-1","payload":{"severity":3}}`, "schema_incompatible"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := decode([]byte(c.value))
			if err == nil {
				t.Fatal("decoded a value that should be unprocessed")
			}
			if got := poison.Reason(err); !strings.HasPrefix(got, c.code+": ") {
				t.Errorf("reason %q, want code %s", got, c.code)
			}
		})
	}
}
