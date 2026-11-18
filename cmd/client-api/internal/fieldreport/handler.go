// Package fieldreport serves POST /field-reports: a field team's observation, accepted with 202
// only after Kafka has it on every in-sync replica. The aggregator stores it later, once per
// (subject, Idempotency-Key).
package fieldreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/client-api/internal/common"

	"tubes-aat-pepeftw/internal/fieldreport"
	"tubes-aat-pepeftw/internal/poison"
)

const (
	// ScopeWrite lets a client send field reports. Only field-team holds it.
	ScopeWrite = "field-report:write"
	// HeaderIdempotencyKey names the client's key for one report; a resend reuses it.
	HeaderIdempotencyKey = "Idempotency-Key"
	// PublishTimeout bounds the wait for Kafka's acknowledgement.
	PublishTimeout = 5 * time.Second
	retryAfter     = "5"
)

// Publisher writes a message to topic field-reports and returns once it is acknowledged by
// every in-sync replica, or with the reason it is not.
type Publisher interface {
	Publish(ctx context.Context, m fieldreport.Message) error
}

type Handler struct {
	pub Publisher
	log *slog.Logger
	now func() time.Time
}

func NewHandler(pub Publisher, log *slog.Logger) *Handler {
	return &Handler{pub: pub, log: log, now: time.Now}
}

// request has pointers so a missing field is told apart from a zero one.
type request struct {
	Latitude    *float64 `json:"latitude"`
	Longitude   *float64 `json:"longitude"`
	ReportedAt  *string  `json:"reported_at"`
	Description *string  `json:"description"`
	HazardID    *string  `json:"hazard_id"`
}

// Submit handles POST /field-reports. A body that is rejected is never produced.
func (h *Handler) Submit(c fiber.Ctx) error {
	key := c.Get(HeaderIdempotencyKey)
	if !fieldreport.ValidKey(key) {
		return invalid(c, "header Idempotency-Key wajib: 1–128 karakter ASCII yang terlihat (0x21–0x7E)")
	}
	report, err := decode(c.Body())
	if err != nil {
		return invalid(c, err.Error())
	}

	sub := common.GetSub(c)
	cid := common.CorrelationIDFrom(c)
	m := fieldreport.Message{
		ReportID:       fieldreport.ReportID(sub, key),
		Subject:        sub,
		IdempotencyKey: key,
		AcceptedAt:     h.now().UTC(),
		CorrelationID:  cid,
		Report:         report,
	}

	ctx, cancel := context.WithTimeout(c.Context(), PublishTimeout)
	defer cancel()
	start := time.Now()
	if err := h.pub.Publish(ctx, m); err != nil {
		h.log.Warn("field_report_publish_failed", "correlation_id", cid, "report_id", m.ReportID, "sub", sub,
			"latency_ms", time.Since(start).Milliseconds(), "error", err.Error())
		c.Set(fiber.HeaderRetryAfter, retryAfter)
		return common.WriteError(c, fiber.StatusServiceUnavailable, common.CodeUpstreamUnavailable, "field reports unavailable")
	}
	h.log.Info("field_report_accepted", "correlation_id", cid, "report_id", m.ReportID, "sub", sub,
		"latency_ms", time.Since(start).Milliseconds())
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"report_id": m.ReportID, "status": "accepted"})
}

// decode turns a body into a report: the poison check first, then strict JSON (no unknown
// fields, nothing after the object), then the required fields and their ranges.
func decode(body []byte) (fieldreport.Report, error) {
	if err := poison.Check(body); err != nil {
		return fieldreport.Report{}, err
	}
	var req request
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return fieldreport.Report{}, errors.New("body bukan JSON laporan yang valid: " + err.Error())
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fieldreport.Report{}, errors.New("body hanya boleh berisi satu objek JSON")
	}

	switch {
	case req.Latitude == nil:
		return fieldreport.Report{}, errors.New("latitude wajib diisi")
	case req.Longitude == nil:
		return fieldreport.Report{}, errors.New("longitude wajib diisi")
	case req.ReportedAt == nil:
		return fieldreport.Report{}, errors.New("reported_at wajib diisi")
	case req.Description == nil:
		return fieldreport.Report{}, errors.New("description wajib diisi")
	}
	reportedAt, err := time.Parse(time.RFC3339, *req.ReportedAt)
	if err != nil {
		return fieldreport.Report{}, errors.New("reported_at harus RFC 3339, misalnya 2026-11-18T03:00:00Z")
	}
	r := fieldreport.Report{
		Latitude:    *req.Latitude,
		Longitude:   *req.Longitude,
		ReportedAt:  reportedAt.UTC(),
		Description: *req.Description,
	}
	if req.HazardID != nil {
		id, err := uuid.Parse(*req.HazardID)
		if err != nil {
			return fieldreport.Report{}, errors.New("hazard_id harus UUID")
		}
		r.HazardID = id.String()
	}
	return r, r.Validate()
}

func invalid(c fiber.Ctx, message string) error {
	return common.WriteError(c, fiber.StatusBadRequest, common.CodeValidation, message)
}
