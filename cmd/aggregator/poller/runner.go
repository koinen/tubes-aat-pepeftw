package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type runner struct {
	source   model.Source
	interval time.Duration
	breaker  *Breaker
	statuses repository.SourceStatusRepository
	cycle    func(ctx context.Context) error
	log      *slog.Logger
	now      func() time.Time

	lastSuccess *time.Time
	lastError   string
}

func (r *runner) run(ctx context.Context) {
	r.restoreStatus(ctx)
	r.log.Info("poller_started", "target", string(r.source), "interval", r.interval.String())
	for {
		r.tick(ctx)
		select {
		case <-ctx.Done():
			r.log.Info("poller_stopped", "target", string(r.source))
			return
		case <-time.After(r.interval):
		}
	}
}

func (r *runner) restoreStatus(ctx context.Context) {
	list, err := r.statuses.ListSourceStatus(ctx)
	if err != nil {
		r.log.Warn("source_status_restore_failed", "target", string(r.source), "error", err.Error())
		return
	}
	for _, st := range list {
		if st.Source == r.source && st.LastSuccessAt != nil {
			t := st.LastSuccessAt.UTC()
			r.lastSuccess = &t
		}
	}
}

func (r *runner) tick(ctx context.Context) {
	cid := uuid.NewString()
	ctx = correlation.With(ctx, cid)
	now := r.now()

	if !r.breaker.Allow(now) {
		r.log.Debug("poll_skipped_breaker_open", "correlation_id", cid, "target", string(r.source))
		r.saveStatus(ctx, cid)
		return
	}

	err := r.cycle(ctx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		r.breaker.Failure(r.now())
		r.lastError = err.Error()
		r.log.Warn("poll_failed", "correlation_id", cid, "target", string(r.source),
			"breaker_state", string(r.breaker.State()), "error", err.Error())
	} else {
		r.breaker.Success()
		t := now.UTC().Truncate(time.Second)
		r.lastSuccess = &t
		r.lastError = ""
	}
	r.saveStatus(ctx, cid)
}

func (r *runner) saveStatus(ctx context.Context, cid string) {
	st := model.SourceStatus{
		Source:        r.source,
		BreakerState:  r.breaker.State(),
		LastSuccessAt: r.lastSuccess,
		LastError:     r.lastError,
	}
	switch {
	case r.lastError == "" && r.breaker.State() == model.BreakerClosed:
		st.Status = model.StatusOK
	case r.lastSuccess != nil:
		st.Status = model.StatusStale
		st.StaleSince = r.lastSuccess
	default:
		st.Status = model.StatusUnavailable
	}
	if err := r.statuses.SaveSourceStatus(ctx, st); err != nil && !errors.Is(err, context.Canceled) {
		r.log.Error("source_status_save_failed", "correlation_id", cid, "target", string(r.source), "error", err.Error())
	}
}
