package model

import "encoding/json"

type OutboxRecord struct {
	ID            int64
	EventID       string
	HazardID      string
	Payload       json.RawMessage
	CorrelationID string
}
