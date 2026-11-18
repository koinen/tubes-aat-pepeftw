// Package fieldreport consumes topic field-reports into the field_reports table. Every aggregator
// pod is in consumer group aggregator-field-reports, outside leader election, and writes unfenced:
// the insert is idempotent, so a record read twice is stored once.
//
// A record's offset is committed only once the record is stored or parked. While the store is
// unreachable the same record is retried with backoff and the partition does not move on.
package fieldreport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"

	wire "tubes-aat-pepeftw/internal/fieldreport"
)

// GroupID is the consumer group every aggregator pod joins for topic field-reports.
const GroupID = "aggregator-field-reports"

var stored = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bnpb_field_reports_stored_total",
	Help: "Laporan lapangan yang dibaca dari Kafka: inserted (baru disimpan) atau duplicate (sudah ada, body sama).",
}, []string{"result"})

// Record is one Kafka record from topic field-reports.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
}

// Endpoint names where a parked record came from: <topic>/<partition>/<offset>.
func (r Record) Endpoint() string {
	return fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
}

// Decision is what to do with one record: store Report, or park Parked. Exactly one is set.
type Decision struct {
	Report *model.FieldReport
	Parked *model.UnprocessedMessage
}

// Decide reads a record. Bytes that are poisonous, not JSON, or not a valid field report are
// parked as TIM_LAPANGAN with the matching reason code; everything else is a report to store.
func Decide(r Record, now time.Time) Decision {
	park := func(cause error) Decision {
		m := unprocessed.Build(model.SourceTimLapangan, r.Endpoint(), r.Value, cause, now)
		return Decision{Parked: &m}
	}
	if err := unprocessed.Check(r.Value); err != nil {
		return park(err)
	}
	var m wire.Message
	if err := json.Unmarshal(r.Value, &m); err != nil {
		return park(err) // a syntax error is malformed_json; a wrong type is schema_incompatible
	}
	if err := m.Validate(); err != nil {
		return park(&mapper.RejectError{Code: model.ReasonSchemaIncompatible, SourceRefID: m.ReportID, Reason: err.Error()})
	}
	return Decision{Report: &model.FieldReport{
		ReportID:       m.ReportID,
		Subject:        m.Subject,
		IdempotencyKey: m.IdempotencyKey,
		Latitude:       m.Report.Latitude,
		Longitude:      m.Report.Longitude,
		ReportedAt:     m.Report.ReportedAt.UTC(),
		Description:    m.Report.Description,
		HazardID:       m.Report.HazardID,
		BodySHA256:     m.Report.SHA256(),
		AcceptedAt:     m.AcceptedAt.UTC(),
		CorrelationID:  m.CorrelationID,
		Partition:      r.Partition,
		Offset:         r.Offset,
	}}
}

// Store is what the handler writes to.
type Store interface {
	repository.FieldReportWriter
	repository.UnprocessedWriter
}

// Handler stores or parks one record at a time.
type Handler struct {
	store Store
	log   *slog.Logger
	now   func() time.Time
	// wait sleeps before the next attempt (attempt counts from 1); it returns ctx's error if ctx ends first.
	wait func(ctx context.Context, attempt int) error
}

func NewHandler(store Store, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log, now: time.Now, wait: backoff}
}

const (
	retryBase = 500 * time.Millisecond
	retryMax  = 30 * time.Second
)

func backoff(ctx context.Context, attempt int) error {
	d := retryMax
	if attempt < 7 {
		d = min(retryBase<<(attempt-1), retryMax)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// Handle stores or parks r and returns nil once its offset may be committed. It retries the same
// record until the store answers; it returns an error only when ctx ends first.
func (h *Handler) Handle(ctx context.Context, r Record) error {
	d := Decide(r, h.now())
	for attempt := 1; ; attempt++ {
		err := h.apply(ctx, r, d)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		h.log.Warn("field_report_store_failed", "endpoint", r.Endpoint(), "attempt", attempt, "error", err.Error())
		if err := h.wait(ctx, attempt); err != nil {
			return err
		}
	}
}

// HandleRecord is Handle for a record read by kafka.Consumer.
func (h *Handler) HandleRecord(ctx context.Context, r *kgo.Record) error {
	return h.Handle(ctx, Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Value: r.Value})
}

// apply runs one attempt. A value the store refuses can never be stored, so it is parked as
// store_rejected; any other store error is retried.
func (h *Handler) apply(ctx context.Context, r Record, d Decision) error {
	if d.Parked != nil {
		return h.park(ctx, r, *d.Parked)
	}
	rep := d.Report
	result, err := h.store.SaveFieldReport(ctx, *rep)
	switch {
	case errors.Is(err, repository.ErrRejected):
		m := unprocessed.Build(model.SourceTimLapangan, r.Endpoint(), r.Value, err, h.now())
		m.SourceRefID = rep.ReportID
		return h.park(ctx, r, m)
	case err != nil:
		return err
	}

	attrs := []any{"correlation_id", rep.CorrelationID, "report_id", rep.ReportID, "endpoint", r.Endpoint()}
	switch result {
	case repository.FieldReportInserted:
		stored.WithLabelValues(string(result)).Inc()
		h.log.Info("field_report_stored", attrs...)
	case repository.FieldReportDuplicate:
		stored.WithLabelValues(string(result)).Inc()
		h.log.Info("field_report_duplicate", attrs...)
	case repository.FieldReportConflict:
		m := unprocessed.Build(model.SourceTimLapangan, r.Endpoint(), r.Value, &mapper.RejectError{
			Code: model.ReasonIdempotencyConflict, SourceRefID: rep.ReportID,
			Reason: "Idempotency-Key " + rep.IdempotencyKey + " sudah dipakai untuk laporan lain; laporan pertama dipertahankan",
		}, h.now())
		m.CorrelationID = rep.CorrelationID
		return h.park(ctx, r, m)
	}
	return nil
}

func (h *Handler) park(ctx context.Context, r Record, m model.UnprocessedMessage) error {
	inserted, err := h.store.SaveUnprocessed(ctx, m)
	if err != nil {
		return err
	}
	if inserted {
		unprocessed.Written(m)
		h.log.Warn("message_unprocessed", "correlation_id", m.CorrelationID, "source", string(m.Source),
			"endpoint", m.Endpoint, "message_id", m.MessageID, "status", string(m.Status), "reason", m.Reason)
	}
	return nil
}
