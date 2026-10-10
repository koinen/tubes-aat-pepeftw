package model

import (
	"encoding/json"
	"time"
)

type RejectedRecord struct {
	Source      Source          `json:"source"`
	Endpoint    string          `json:"endpoint"`
	SourceRefID string          `json:"source_ref_id,omitempty"`
	Reason      string          `json:"reason"`
	Payload     json.RawMessage `json:"payload"`
	RejectedAt  time.Time       `json:"rejected_at"`
}
