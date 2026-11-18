package model

import "time"

// ReasonCode is the "<code>" part of an unprocessed message's reason (Lampiran A).
type ReasonCode string

const (
	ReasonSchemaIncompatible ReasonCode = "schema_incompatible"
	ReasonContractViolation  ReasonCode = "contract_violation"
	ReasonMalformedJSON      ReasonCode = "malformed_json"
	ReasonInvalidUTF8        ReasonCode = "invalid_utf8"
	ReasonTooDeep            ReasonCode = "too_deep"
	ReasonTooLarge           ReasonCode = "too_large"
	ReasonStoreRejected      ReasonCode = "store_rejected"
	ReasonStoreUnavailable   ReasonCode = "store_unavailable"
)

type UnprocessedStatus string

const (
	StatusRetrying  UnprocessedStatus = "retrying"
	StatusParked    UnprocessedStatus = "parked"
	StatusRecovered UnprocessedStatus = "recovered"
)

// UnprocessedMessage is an inbound message the system could not turn into a HazardEvent.
type UnprocessedMessage struct {
	MessageID     string
	Source        Source
	Endpoint      string
	SourceRefID   string
	ReceivedAt    time.Time
	Code          ReasonCode
	Reason        string // "<code>: <detail>"
	Retryable     bool
	Status        UnprocessedStatus
	Attempts      int
	Payload       []byte // intact, or a prefix when PayloadSize > len(Payload)
	PayloadSize   int64
	PayloadSHA256 string
	Hazard        *HazardEvent // retrying rows only: what to save on retry
	CorrelationID string
}
