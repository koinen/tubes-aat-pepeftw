package main

import (
	"encoding/json"
	"time"
)

type hazardMessage struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	HazardID  string          `json:"hazard_id"`
	EmittedAt time.Time       `json:"emitted_at"`
	Payload   json.RawMessage `json:"payload"`
}

type hazardPayload struct {
	Source      string    `json:"source"`
	SourceRefID string    `json:"source_ref_id"`
	HazardType  string    `json:"hazard_type"`
	Severity    string    `json:"severity"`
	AreaName    string    `json:"area_name"`
	OccurredAt  time.Time `json:"occurred_at"`
}
