package unprocessed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	// replayInterval is how often the leader looks for a replay job.
	replayInterval = 2 * time.Second
	// replayPage is how many parked rows a replay job reads at a time.
	replayPage = 100
)

// Remap maps one parked payload again. needed lists the declared rules the payload needed.
type Remap func(raw []byte, now time.Time) (h model.HazardEvent, needed []string, err error)

// Remapper is how a replay job of one source maps its parked payloads again.
type Remapper struct {
	// Rules is the canonical form of the adaptation rules this pod declares for the source.
	// A job accepted by a pod with other rules waits for the rollout.
	Rules string
	// Load prepares one job's mapping.
	Load func(ctx context.Context) (Remap, error)
}

// PVMBGRemapper maps PVMBG volcanic reports with the declared rules and volcano_ref loaded fresh
// for each job, so a ref added with load-catalog is used without a restart.
func PVMBGRemapper(refs repository.VolcanoRefRepository, rules mapper.VolcanicAdaptations) Remapper {
	return Remapper{
		Rules: rules.String(),
		Load: func(ctx context.Context) (Remap, error) {
			list, err := refs.ListVolcanoRefs(ctx)
			if err != nil {
				return nil, fmt.Errorf("load volcano_ref: %w", err)
			}
			byID := make(map[string]mapper.Volcano, len(list))
			for _, v := range list {
				byID[v.VolcanoID] = mapper.Volcano{Name: v.Name, Latitude: v.Latitude, Longitude: v.Longitude}
			}
			lookup := func(id string) (mapper.Volcano, bool) {
				v, ok := byID[id]
				return v, ok
			}
			return func(raw []byte, now time.Time) (model.HazardEvent, []string, error) {
				var needed []string
				r := rules.WithObserver(func(rule, _ string) { needed = append(needed, rule) })
				h, err := mapper.Volcanic(json.RawMessage(raw), lookup, r, now)
				return h, needed, err
			}, nil
		},
	}
}

// Replayer runs replay jobs. It must run only on the leader, with the fenced context.
type Replayer struct {
	q         repository.ReplayQueue
	remappers map[model.Source]Remapper
	log       *slog.Logger
	now       func() time.Time
}

func NewReplayer(q repository.ReplayQueue, remappers map[model.Source]Remapper, log *slog.Logger, now func() time.Time) *Replayer {
	return &Replayer{q: q, remappers: remappers, log: log, now: now}
}

// Run looks for a job every replayInterval until the context ends or the leader is fenced out.
func (r *Replayer) Run(ctx context.Context) {
	r.log.Info("replayer_started", "interval", replayInterval.String())
	t := time.NewTicker(replayInterval)
	defer t.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil {
			if ctx.Err() == nil {
				r.log.Error("replayer_fenced", "error", err.Error())
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

// RunOnce takes the next job from where it stopped to its end. It returns an error only when the
// runner must stop (lost leadership or shutdown).
func (r *Replayer) RunOnce(ctx context.Context) error {
	job, found, err := r.q.NextJob(ctx)
	if err != nil {
		if mustStop(ctx, err) {
			return err
		}
		r.log.Warn("replay_next_job_failed", "error", err.Error())
		return nil
	}
	if !found {
		return nil
	}
	err = r.run(ctx, job)
	switch {
	case err == nil:
		return nil
	case mustStop(ctx, err):
		return err
	case errors.Is(err, repository.ErrUnavailable):
		// The job keeps its position; the next tick continues from there.
		r.log.Warn("replay_paused", "correlation_id", job.JobID, "job_id", job.JobID, "source", string(job.Source),
			"error", err.Error())
		return nil
	}
	// Our own bug: running the job again would fail the same way.
	r.log.Error("replay_failed", "correlation_id", job.JobID, "job_id", job.JobID, "source", string(job.Source),
		"error", err.Error())
	if err := r.q.FinishJob(ctx, job.JobID, model.ReplayFailed, err.Error()); err != nil {
		if mustStop(ctx, err) {
			return err
		}
		r.log.Warn("replay_finish_failed", "job_id", job.JobID, "error", err.Error())
	}
	return nil
}

// run takes job from its position to done. Errors are classified by RunOnce.
func (r *Replayer) run(ctx context.Context, job model.ReplayJob) error {
	rm, ok := r.remappers[job.Source]
	if !ok {
		return fmt.Errorf("source %s tidak mendukung replay", job.Source)
	}
	if job.Adaptations != rm.Rules {
		r.log.Info("replay_waiting_for_config", "correlation_id", job.JobID, "job_id", job.JobID,
			"source", string(job.Source), "job_adaptations", job.Adaptations, "pod_adaptations", rm.Rules)
		return nil
	}
	if job.Status == model.ReplayQueued {
		if err := r.q.StartJob(ctx, job.JobID); err != nil {
			return err
		}
		r.log.Info("replay_started", "correlation_id", job.JobID, "job_id", job.JobID, "source", string(job.Source))
	}
	remap, err := rm.Load(ctx)
	if err != nil {
		return err
	}
	pos := job.Position
	for {
		page, err := r.q.ParkedPage(ctx, job.Source, job.Cutoff, pos, replayPage)
		if err != nil {
			return err
		}
		for _, m := range page {
			if err := r.replay(ctx, job, remap, m); err != nil {
				return err
			}
			pos = model.ReplayPosition{ReceivedAt: m.ReceivedAt, MessageID: m.MessageID}
		}
		if len(page) < replayPage {
			break
		}
	}
	if err := r.q.FinishJob(ctx, job.JobID, model.ReplayDone, ""); err != nil {
		return err
	}
	r.log.Info("replay_finished", "correlation_id", job.JobID, "job_id", job.JobID, "source", string(job.Source))
	return nil
}

func (r *Replayer) replay(ctx context.Context, job model.ReplayJob, remap Remap, m model.UnprocessedMessage) error {
	if m.PayloadSize > int64(len(m.Payload)) {
		return r.q.RecordStep(ctx, job.JobID, repository.ReplayStep{Message: m, Outcome: repository.ReplayNotReplayable})
	}
	now := r.now().UTC()
	h, needed, err := remap(m.Payload, now)
	if err != nil {
		failed := Build(m.Source, m.Endpoint, m.Payload, err, now)
		m.Code, m.Reason = failed.Code, failed.Reason
		r.log.Warn("message_still_parked", "correlation_id", job.JobID, "job_id", job.JobID, "message_id", m.MessageID,
			"source", string(m.Source), "reason", m.Reason)
		return r.q.RecordStep(ctx, job.JobID, repository.ReplayStep{Message: m, Outcome: repository.ReplayStillParked})
	}
	m.Hazard, m.CorrelationID = &h, job.JobID
	err = r.q.RecordStep(ctx, job.JobID, repository.ReplayStep{Message: m, Outcome: repository.ReplayRecovered})
	switch {
	case err == nil:
	case mustStop(ctx, err):
		return err
	case errors.Is(err, repository.ErrUnavailable):
		m.Code = model.ReasonStoreUnavailable
		m.Reason = storable(string(model.ReasonStoreUnavailable)+": "+err.Error(), 0)
		r.log.Warn("message_handed_to_retry", "correlation_id", job.JobID, "job_id", job.JobID, "message_id", m.MessageID,
			"source", string(m.Source), "error", err.Error())
		return r.q.RecordStep(ctx, job.JobID, repository.ReplayStep{Message: m, Outcome: repository.ReplayHandedToRetry})
	case errors.Is(err, repository.ErrRejected):
		m.Code = model.ReasonStoreRejected
		m.Reason = storable(string(model.ReasonStoreRejected)+": "+err.Error(), 0)
		r.log.Warn("message_still_parked", "correlation_id", job.JobID, "job_id", job.JobID, "message_id", m.MessageID,
			"source", string(m.Source), "reason", m.Reason)
		return r.q.RecordStep(ctx, job.JobID, repository.ReplayStep{Message: m, Outcome: repository.ReplayStillParked})
	default:
		return err
	}
	Recovered(m.Source)
	adapted := AdaptationObserver(r.log, m.Source, job.JobID)
	for _, rule := range needed {
		adapted(rule, h.SourceRefID)
	}
	r.log.Info("message_recovered", "correlation_id", job.JobID, "job_id", job.JobID, "message_id", m.MessageID,
		"source", string(m.Source), "source_ref_id", h.SourceRefID)
	return nil
}
