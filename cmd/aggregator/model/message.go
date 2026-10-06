package model

import "time"

type EventType string

const EventHazardCreated EventType = "HAZARD_CREATED"

type HazardMessage struct {
	EventID   string      `json:"event_id"`
	EventType EventType   `json:"event_type"`
	HazardID  string      `json:"hazard_id"`
	EmittedAt time.Time   `json:"emitted_at"`
	Payload   HazardEvent `json:"payload"`
}
