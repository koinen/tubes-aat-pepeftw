package model

import "time"

// FieldReport is a field team's report as the aggregator stores it, once per
// (Subject, IdempotencyKey).
type FieldReport struct {
	ReportID       string
	Subject        string
	IdempotencyKey string
	Latitude       float64
	Longitude      float64
	ReportedAt     time.Time
	Description    string
	HazardID       string // empty when the report names no hazard
	BodySHA256     string
	AcceptedAt     time.Time
	CorrelationID  string
	Partition      int32
	Offset         int64
}
