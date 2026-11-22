// Package fieldreport is the record client-api writes to topic field-reports and the aggregator
// reads back. The value is self-describing JSON: it carries who sent it, under which
// Idempotency-Key, and when client-api accepted it, so the consumer needs no headers to store it.
package fieldreport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	// MaxDescription is the longest description, in bytes.
	MaxDescription = 4 << 10
	// MaxIdempotencyKey is the longest Idempotency-Key.
	MaxIdempotencyKey = 128
)

// namespace makes report ids: UUIDv5(namespace, sub + "\x00" + key). Never change it, or a resend
// after a deploy gets a new id.
var namespace = uuid.MustParse("6b1f2c1e-3f4a-5d6e-9a7b-8c9d0e1f2a3b")

// ReportID is the id of the report sub sent under key. The same pair always gets the same id.
func ReportID(sub, key string) string {
	return uuid.NewSHA1(namespace, []byte(sub+"\x00"+key)).String()
}

// ValidKey reports whether key is a usable Idempotency-Key: 1–128 visible ASCII characters.
func ValidKey(key string) bool {
	if len(key) == 0 || len(key) > MaxIdempotencyKey {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7E {
			return false
		}
	}
	return true
}

// Report is what the field team observed.
type Report struct {
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	ReportedAt  time.Time `json:"reported_at"`
	Description string    `json:"description"`
	HazardID    string    `json:"hazard_id,omitempty"`
}

// Validate checks the ranges both sides agree on.
func (r Report) Validate() error {
	switch {
	case r.Latitude < -90 || r.Latitude > 90:
		return errors.New("latitude harus di antara -90 dan 90")
	case r.Longitude < -180 || r.Longitude > 180:
		return errors.New("longitude harus di antara -180 dan 180")
	case r.ReportedAt.IsZero():
		return errors.New("reported_at wajib diisi")
	case r.Description == "":
		return errors.New("description wajib diisi")
	case len(r.Description) > MaxDescription:
		return fmt.Errorf("description lebih dari %d byte", MaxDescription)
	case !utf8.ValidString(r.Description):
		return errors.New("description bukan UTF-8 yang valid")
	}
	if r.HazardID != "" {
		if _, err := uuid.Parse(r.HazardID); err != nil {
			return errors.New("hazard_id harus UUID")
		}
	}
	return nil
}

// SHA256 is the hex sha256 of the report's canonical JSON. Two sends of the same report hash the
// same however the client spaced or ordered its body; a different report under the same key does not.
func (r Report) SHA256() string {
	r.ReportedAt = r.ReportedAt.UTC()
	b, _ := json.Marshal(r) // a struct of plain fields cannot fail to marshal
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Message is one record on topic field-reports. Its key is ReportID.
type Message struct {
	ReportID       string    `json:"report_id"`
	Subject        string    `json:"subject"`
	IdempotencyKey string    `json:"idempotency_key"`
	AcceptedAt     time.Time `json:"accepted_at"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	Report         Report    `json:"report"`
}

// Validate checks a message read from the topic. A message whose report_id does not follow from
// its subject and key was not written by client-api.
func (m Message) Validate() error {
	switch {
	case m.Subject == "":
		return errors.New("subject wajib diisi")
	case !ValidKey(m.IdempotencyKey):
		return errors.New("idempotency_key tidak valid")
	case m.ReportID != ReportID(m.Subject, m.IdempotencyKey):
		return errors.New("report_id tidak cocok dengan subject dan idempotency_key")
	case m.AcceptedAt.IsZero():
		return errors.New("accepted_at wajib diisi")
	}
	return m.Report.Validate()
}
