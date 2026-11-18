package unprocessed

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	retryInterval = 5 * time.Second
	retryBatch    = 50
)

// Worker retries retrying messages until they are recovered. It must run only on the leader,
// with the fenced context: it stops as soon as a write reports a stale epoch.
type Worker struct {
	q   repository.RetryQueue
	log *slog.Logger
}

func NewWorker(q repository.RetryQueue, log *slog.Logger) *Worker {
	return &Worker{q: q, log: log}
}

func (w *Worker) Run(ctx context.Context) {
	w.log.Info("retry_worker_started", "interval", retryInterval.String())
	t := time.NewTicker(retryInterval)
	defer t.Stop()
	for {
		if err := w.tick(ctx); err != nil {
			if ctx.Err() == nil {
				w.log.Error("retry_worker_fenced", "error", err.Error())
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// tick retries every due message once. It returns an error only when the worker must stop.
func (w *Worker) tick(ctx context.Context) error {
	due, err := w.q.DueRetries(ctx, retryBatch)
	if err != nil {
		if mustStop(ctx, err) {
			return err
		}
		w.log.Warn("retry_due_failed", "error", err.Error())
		return nil
	}
	for _, m := range due {
		err := w.retry(ctx, m)
		if mustStop(ctx, err) {
			return err
		}
		if err != nil {
			w.log.Warn("retry_bookkeeping_failed", "message_id", m.MessageID, "error", err.Error())
		}
	}
	return nil
}

func (w *Worker) retry(ctx context.Context, m model.UnprocessedMessage) error {
	if m.Hazard == nil {
		reason := string(model.ReasonStoreRejected) + ": kolom hazard tidak bisa dibaca"
		w.log.Warn("message_parked", "correlation_id", m.CorrelationID, "message_id", m.MessageID,
			"source", string(m.Source), "reason", reason)
		return w.q.ParkRetry(ctx, m.MessageID, reason)
	}
	err := w.q.RecoverRetry(ctx, m)
	switch {
	case err == nil:
		Recovered(m.Source)
		w.log.Info("message_recovered", "correlation_id", m.CorrelationID, "message_id", m.MessageID,
			"source", string(m.Source), "source_ref_id", m.SourceRefID, "attempts", m.Attempts+1)
		return nil
	case mustStop(ctx, err):
		return err
	case errors.Is(err, repository.ErrRejected):
		reason := string(model.ReasonStoreRejected) + ": " + err.Error()
		w.log.Warn("message_parked", "correlation_id", m.CorrelationID, "message_id", m.MessageID,
			"source", string(m.Source), "reason", reason)
		return w.q.ParkRetry(ctx, m.MessageID, reason)
	default:
		w.log.Warn("retry_failed", "correlation_id", m.CorrelationID, "message_id", m.MessageID,
			"source", string(m.Source), "attempts", m.Attempts+1, "error", err.Error())
		return w.q.RetryFailed(ctx, m.MessageID)
	}
}

func mustStop(ctx context.Context, err error) bool {
	return err != nil && (ctx.Err() != nil || errors.Is(err, fence.ErrStaleEpoch))
}
