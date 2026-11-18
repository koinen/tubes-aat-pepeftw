// Package forwarded consumes topic unprocessed: values that notifier, audit-log or
// dashboard-updater could not read from hazard-events. Each is parked as an INTERNAL unprocessed
// message under its original topic position, so the same record forwarded by several consumer
// groups is one row.
//
// Every aggregator pod is in consumer group aggregator-unprocessed, outside leader election, and
// writes unfenced: the insert is idempotent by message_id.
package forwarded

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"

	"tubes-aat-pepeftw/internal/poison"
)

// GroupID is the consumer group every aggregator pod joins for topic unprocessed.
const GroupID = "aggregator-unprocessed"

// reported are the codes a consumer may put in the reason header; any other is kept as detail.
var reported = map[model.ReasonCode]bool{
	model.ReasonInvalidUTF8:        true,
	model.ReasonTooDeep:            true,
	model.ReasonMalformedJSON:      true,
	model.ReasonSchemaIncompatible: true,
}

// Decide describes a record from topic unprocessed as an INTERNAL unprocessed message. Its endpoint
// is the original <topic>/<partition>/<offset> from the headers; a record whose headers do not say
// where it came from is parked under its own position as schema_incompatible.
func Decide(r *kgo.Record, now time.Time) model.UnprocessedMessage {
	h := map[string]string{}
	for _, hd := range r.Headers {
		h[hd.Key] = string(hd.Value)
	}

	var cause error
	endpoint, missing := origin(h)
	if missing != "" {
		endpoint = fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
		cause = &mapper.RejectError{Code: model.ReasonSchemaIncompatible,
			Reason: "header " + missing + " hilang atau tidak valid; record dikirim langsung ke topic " + r.Topic}
	} else {
		code, detail := reason(h[poison.HeaderReason])
		if consumer := h[poison.HeaderConsumer]; consumer != "" {
			detail += " (dilaporkan oleh " + consumer + ")"
		}
		cause = &mapper.RejectError{Code: code, Reason: detail}
	}

	m := unprocessed.Build(model.SourceInternal, endpoint, r.Value, cause, now)
	m.CorrelationID = h["correlation_id"]
	return m
}

// origin is "<topic>/<partition>/<offset>" from the headers, or the name of the first header that
// is missing or invalid.
func origin(h map[string]string) (endpoint, missing string) {
	topic := h[poison.HeaderTopic]
	if topic == "" {
		return "", poison.HeaderTopic
	}
	partition, err := strconv.ParseInt(h[poison.HeaderPartition], 10, 32)
	if err != nil || partition < 0 {
		return "", poison.HeaderPartition
	}
	offset, err := strconv.ParseInt(h[poison.HeaderOffset], 10, 64)
	if err != nil || offset < 0 {
		return "", poison.HeaderOffset
	}
	return fmt.Sprintf("%s/%d/%d", topic, partition, offset), ""
}

// reason splits "<code>: <detail>". An unknown or missing code becomes schema_incompatible, with
// the header's whole text as the detail.
func reason(header string) (model.ReasonCode, string) {
	if code, detail, ok := strings.Cut(header, ": "); ok && reported[model.ReasonCode(code)] {
		return model.ReasonCode(code), detail
	}
	if header == "" {
		return model.ReasonSchemaIncompatible, "header " + poison.HeaderReason + " hilang"
	}
	return model.ReasonSchemaIncompatible, header
}

// Handler parks one record at a time.
type Handler struct {
	store repository.UnprocessedWriter
	log   *slog.Logger
	now   func() time.Time
	// wait sleeps before the next attempt (attempt counts from 1); it returns ctx's error if ctx ends first.
	wait func(ctx context.Context, attempt int) error
}

func NewHandler(store repository.UnprocessedWriter, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log, now: time.Now, wait: poison.Backoff}
}

// Handle parks r and returns nil once its offset may be committed. It retries the same record until
// the store answers; it returns an error only when ctx ends first.
func (h *Handler) Handle(ctx context.Context, r *kgo.Record) error {
	m := Decide(r, h.now())
	for attempt := 1; ; attempt++ {
		inserted, err := h.store.SaveUnprocessed(ctx, m)
		if err == nil {
			if inserted {
				unprocessed.Written(m)
				h.log.Warn("message_unprocessed", "correlation_id", m.CorrelationID, "source", string(m.Source),
					"endpoint", m.Endpoint, "message_id", m.MessageID, "status", string(m.Status), "reason", m.Reason)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		h.log.Warn("unprocessed_store_failed", "endpoint", m.Endpoint, "attempt", attempt, "error", err.Error())
		if err := h.wait(ctx, attempt); err != nil {
			return err
		}
	}
}
