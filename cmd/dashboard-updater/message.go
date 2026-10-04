package main

import "time"

type hazardMessage struct {
	EventID   string        `json:"event_id"`
	EventType string        `json:"event_type"`
	HazardID  string        `json:"hazard_id"`
	EmittedAt time.Time     `json:"emitted_at"`
	Payload   hazardPayload `json:"payload"`
}

type hazardPayload struct {
	HazardID   string    `json:"hazard_id"`
	Source     string    `json:"source"`
	HazardType string    `json:"hazard_type"`
	Severity   string    `json:"severity"`
	AreaName   string    `json:"area_name"`
	OccurredAt time.Time `json:"occurred_at"`
}
