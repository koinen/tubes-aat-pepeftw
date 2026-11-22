package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
	"tubes-aat-pepeftw/cmd/aggregator/unprocessed"
)

// recorder writes one poll cycle's unprocessed messages. err holds the first failed write:
// a cycle with a failed write must not advance its cursor, so the next poll fetches the record again.
type recorder struct {
	repo   repository.UnprocessedWriter
	log    *slog.Logger
	source model.Source
	now    time.Time
	err    error
}

func newRecorder(repo repository.UnprocessedWriter, log *slog.Logger, source model.Source, now time.Time) *recorder {
	return &recorder{repo: repo, log: log, source: source, now: now}
}

// park records a record the mapper rejected.
func (r *recorder) park(ctx context.Context, endpoint string, raw []byte, cause error) {
	r.save(ctx, unprocessed.Build(r.source, endpoint, raw, cause, r.now))
}

// storeFailed handles a SaveHazard error for h. It returns nil when the record went to the DLQ,
// or the error itself when the cycle must abort (lost leadership, shutdown, a bug in our SQL).
func (r *recorder) storeFailed(ctx context.Context, endpoint string, raw []byte, h model.HazardEvent, err error) error {
	if !unprocessed.Recordable(err) {
		return err
	}
	m := unprocessed.Build(r.source, endpoint, raw, err, r.now)
	m.SourceRefID = h.SourceRefID
	if m.Status == model.StatusRetrying {
		m.Hazard = &h
		m.CorrelationID = correlation.From(ctx)
	}
	r.save(ctx, m)
	return nil
}

// decode unmarshals a fetched response into v. A response that is oversized, poisonous, or not
// valid JSON is parked whole and returned as an error, so the cycle fails without moving its cursor.
func (r *recorder) decode(ctx context.Context, endpoint string, body []byte, fetchErr error, v any) error {
	var over *unprocessed.Oversized
	if fetchErr != nil && !errors.As(fetchErr, &over) {
		return fetchErr
	}
	cause := fetchErr
	if cause == nil {
		cause = unprocessed.Check(body)
	}
	if cause == nil {
		cause = json.Unmarshal(body, v) // a syntax error is malformed_json; a wrong envelope shape is schema_incompatible
	}
	if cause == nil {
		return nil
	}
	r.save(ctx, unprocessed.BuildResponse(r.source, endpoint, body, cause, r.now))
	if r.err != nil {
		return r.err
	}
	return fmt.Errorf("%s: respons diparkir: %w", endpoint, cause)
}

func (r *recorder) save(ctx context.Context, m model.UnprocessedMessage) {
	cid := correlation.From(ctx)
	inserted, err := r.repo.SaveUnprocessed(ctx, m)
	if err != nil {
		r.log.Error("unprocessed_save_failed", "correlation_id", cid, "source", string(m.Source),
			"endpoint", m.Endpoint, "message_id", m.MessageID, "error", err.Error())
		if r.err == nil {
			r.err = fmt.Errorf("simpan unprocessed message %s: %w", m.MessageID, err)
		}
		return
	}
	if !inserted {
		return
	}
	unprocessed.Written(m)
	r.log.Warn("message_unprocessed", "correlation_id", cid, "source", string(m.Source), "endpoint", m.Endpoint,
		"message_id", m.MessageID, "source_ref_id", m.SourceRefID, "status", string(m.Status), "reason", m.Reason)
}
