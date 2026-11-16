package repository

import (
	"context"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const (
	AuditDefaultLimit = 100
	AuditMaxLimit     = 1000
)

type AuditFilter struct {
	Source *model.Source
	Since  *time.Time
	Limit  int
	Cursor *Keyset
}

func (f AuditFilter) EffectiveLimit() int {
	switch {
	case f.Limit <= 0:
		return AuditDefaultLimit
	case f.Limit > AuditMaxLimit:
		return AuditMaxLimit
	default:
		return f.Limit
	}
}

type Checksum struct {
	Source           model.Source
	Count            int
	Checksum         string
	AsOf             time.Time
	Partial          bool
	UnavailableUnits []string
}

type Location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type HazardLocation struct {
	HazardID  string
	Locations []Location
	Partial   bool
}

type UnprocessedMessage struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

type AuditReader interface {
	AuditEvents(ctx context.Context, f AuditFilter) (Page[model.HazardEvent], error)
	AuditChecksum(ctx context.Context, source model.Source) (Checksum, error)
	HazardLocation(ctx context.Context, hazardID string) (HazardLocation, error)
	Unprocessed(ctx context.Context, f AuditFilter) (Page[UnprocessedMessage], error)
}
