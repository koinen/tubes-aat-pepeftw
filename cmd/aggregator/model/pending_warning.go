package model

import (
	"encoding/json"
	"time"
)

// PendingWarning = tsunami warning yang related seismic event nya belom di ingest.
type PendingWarning struct {
	WarningID      string          `json:"warning_id"`
	RelatedEventID string          `json:"related_event_id"`
	Payload        json.RawMessage `json:"payload"`
	ReceivedAt     time.Time       `json:"received_at"`
}
