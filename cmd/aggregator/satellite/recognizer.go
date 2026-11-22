package satellite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// Window of recomputation, measured from the newest satellite time in the database.
// Observed fires last at most ~16 h, so much older detections can no longer join new ones.
const (
	DetectionWindow = 72 * time.Hour
	// SettleMargin: fires whose earliest detection is older than (window - margin) are frozen,
	// because clusters at the window edge may be truncated.
	SettleMargin = 24 * time.Hour
)

type Store interface {
	ActiveDetections(ctx context.Context) ([]Detection, error)
	ListWildfires(ctx context.Context, since time.Time) ([]model.HazardEvent, error)
	SaveWildfire(ctx context.Context, h model.HazardEvent) (string, error)
	Version(ctx context.Context) (int64, error)
}

type Result struct {
	Detections int
	Fires      int
	Singles    int
	Created    int
	Updated    int
	Retracted  int
	Unchanged  int
	Frozen     int
	Duration   time.Duration
}

type Observer interface {
	FireSaved(latency time.Duration)
	RecomputeDone(res Result)
}

type noObserver struct{}

func (noObserver) FireSaved(time.Duration) {}
func (noObserver) RecomputeDone(Result)    {}

type Recognizer struct {
	store    Store
	log      *slog.Logger
	now      func() time.Time
	interval time.Duration
	obs      Observer
	lastVer  int64
	hasVer   bool
}

func (r *Recognizer) WithObserver(o Observer) *Recognizer {
	if o != nil {
		r.obs = o
	}
	return r
}

func NewRecognizer(store Store, log *slog.Logger, interval time.Duration) *Recognizer {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Recognizer{store: store, log: log, now: time.Now, interval: interval, obs: noObserver{}}
}

func (r *Recognizer) Run(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		if err := r.Tick(ctx); err != nil {
			if errors.Is(err, fence.ErrStaleEpoch) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			r.log.Error("wildfire_recompute_failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (r *Recognizer) Tick(ctx context.Context) error {
	ver, err := r.store.Version(ctx)
	if err != nil {
		return err
	}
	if r.hasVer && ver == r.lastVer {
		return nil
	}
	res, err := r.Recompute(ctx)
	if err != nil {
		return err
	}
	r.lastVer, r.hasVer = ver, true
	r.log.Info("wildfire_recompute", "version", ver,
		"detections", res.Detections, "fires", res.Fires, "singles", res.Singles,
		"created", res.Created, "updated", res.Updated, "retracted", res.Retracted,
		"unchanged", res.Unchanged, "frozen", res.Frozen, "ms", res.Duration.Milliseconds())
	return nil
}

// freezeCutoff: fires with the earliest detection before this time are left untouched.
func freezeCutoff(dets []Detection) time.Time {
	var newest time.Time
	for i := range dets {
		if dets[i].AcqTime.After(newest) {
			newest = dets[i].AcqTime
		}
	}
	if newest.IsZero() {
		return time.Time{}
	}
	return newest.Add(-(DetectionWindow - SettleMargin))
}

func (r *Recognizer) Recompute(ctx context.Context) (Result, error) {
	start := time.Now()
	dets, err := r.store.ActiveDetections(ctx)
	if err != nil {
		return Result{}, err
	}
	cutoff := freezeCutoff(dets)
	existing, err := r.store.ListWildfires(ctx, cutoff)
	if err != nil {
		return Result{}, err
	}
	byRef := make(map[string]model.HazardEvent, len(existing))
	for _, h := range existing {
		byRef[h.SourceRefID] = h
	}

	groups, singles := Cluster(dets)
	res := Result{Detections: len(dets), Fires: len(groups), Singles: singles}
	now := r.now()
	seen := make(map[string]bool, len(groups))

	for _, g := range groups {
		f := BuildFire(g)
		if f.OccurredAt.Before(cutoff) {
			res.Frozen++
			continue
		}
		h := ToHazard(f, now)
		seen[h.SourceRefID] = true
		if cur, ok := byRef[h.SourceRefID]; ok && SameHazard(cur, h) {
			res.Unchanged++
			continue
		}
		out, err := r.store.SaveWildfire(ctx, h)
		if err != nil {
			return res, err
		}
		count(&res, out)
		if (out == "created" || out == "updated") && !f.LastReceived.IsZero() {
			if lat := r.now().Sub(f.LastReceived); lat >= 0 {
				r.obs.FireSaved(lat)
			}
		}
	}

	for ref, cur := range byRef {
		if seen[ref] || isRetracted(cur) {
			continue
		}
		out, err := r.store.SaveWildfire(ctx, retractedCopy(cur))
		if err != nil {
			return res, err
		}
		if out != "unchanged" {
			res.Retracted++
		}
	}
	res.Duration = time.Since(start)
	r.obs.RecomputeDone(res)
	return res, nil
}

func count(res *Result, outcome string) {
	switch outcome {
	case "created":
		res.Created++
	case "updated":
		res.Updated++
	default:
		res.Unchanged++
	}
}

func isRetracted(h model.HazardEvent) bool {
	v, _ := h.Attributes["retracted"].(bool)
	return v
}

func retractedCopy(h model.HazardEvent) model.HazardEvent {
	attrs := make(map[string]any, len(h.Attributes)+1)
	for k, v := range h.Attributes {
		attrs[k] = v
	}
	attrs["retracted"] = true
	h.Attributes = attrs
	h.Severity = model.Severity("NORMAL")
	return h
}

func SameHazard(a, b model.HazardEvent) bool {
	if a.Severity != b.Severity || a.AreaName != b.AreaName ||
		a.Latitude != b.Latitude || a.Longitude != b.Longitude || !a.OccurredAt.Equal(b.OccurredAt) {
		return false
	}
	aj, err1 := json.Marshal(a.Attributes)
	bj, err2 := json.Marshal(b.Attributes)
	return err1 == nil && err2 == nil && bytes.Equal(aj, bj)
}
