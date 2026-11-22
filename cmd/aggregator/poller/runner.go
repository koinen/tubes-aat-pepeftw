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

	"tubes-aat-pepeftw/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
)

type runner struct {
	source   model.Source
	interval time.Duration
	breaker  *Breaker
	statuses repository.SourceStatusRepository
	cycle    func(ctx context.Context) pollResult
	sensor   *sensorPantaiPoller
	log      *slog.Logger
	now      func() time.Time

	lastSuccess *time.Time
	lastError   string
}

// pollResult separates evidence about the source from the outcome of local ingestion.
type pollResult struct {
	checked   bool
	sourceErr error
	localErr  error
}

func sourceCycle(cycle func(context.Context) error) func(context.Context) pollResult {
	return func(ctx context.Context) pollResult {
		return pollResult{checked: true, sourceErr: cycle(ctx)}
	}
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
	ctx, span := telemetry.Start(ctx, "poll "+string(r.source))
	span.SetAttributes(attribute.String("correlation_id", cid))
	defer span.End()
	now := r.now()

	if !r.breaker.Allow(now) {
		r.log.Debug("poll_skipped_breaker_open", "correlation_id", cid, "target", string(r.source))
		r.saveStatus(ctx, cid)
		return
	}

	probing := r.sensor != nil && r.breaker.State() == model.BreakerHalfOpen
	var result pollResult
	if probing {
		result = pollResult{checked: true, sourceErr: r.sensor.probe(ctx)}
	} else {
		result = r.cycle(ctx)
	}
	if ctx.Err() != nil {
		return
	}
	if result.sourceErr != nil {
		r.breaker.Failure(r.now())
	} else if result.checked {
		r.breaker.Success()
	}
	if err := errors.Join(result.sourceErr, result.localErr); err != nil {
		r.lastError = err.Error()
		r.log.Warn("poll_failed", "correlation_id", cid, "target", string(r.source),
			"breaker_state", string(r.breaker.State()), "error", err.Error())
	} else if !probing && result.checked {
		t := now.UTC().Truncate(time.Second)
		r.lastSuccess = &t
		r.lastError = ""
	}
	r.saveStatus(ctx, cid)
	if r.sensor != nil && !probing && r.breaker.State() == model.BreakerClosed && ctx.Err() == nil {
		r.sensor.sampleReadiness(ctx)
	}
}

func (r *runner) saveStatus(ctx context.Context, cid string) {
	st := model.SourceStatus{
		Source:        r.source,
		BreakerState:  r.breaker.State(),
		LastSuccessAt: r.lastSuccess,
		LastError:     r.lastError,
	}
	if r.sensor != nil {
		st.StationSilence = r.sensor.stationSilence(st.BreakerState == model.BreakerClosed)
	}
	switch {
	case r.lastSuccess != nil && r.lastError == "" && r.breaker.State() == model.BreakerClosed:
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
