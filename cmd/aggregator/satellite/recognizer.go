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

// Store adalah bagian database yang dibutuhkan pengenal kebakaran.
type Store interface {
	ActiveDetections(ctx context.Context) ([]Detection, error)
	ListWildfires(ctx context.Context) ([]model.HazardEvent, error)
	// SaveWildfire mengembalikan "created", "updated", atau "unchanged".
	SaveWildfire(ctx context.Context, h model.HazardEvent) (string, error)
	// Version berubah setiap kali data deteksi berubah (offset terakhir yang diterapkan).
	Version(ctx context.Context) (int64, error)
}

// Result meringkas satu putaran penghitungan ulang.
type Result struct {
	Detections int
	Fires      int
	Singles    int
	Created    int
	Updated    int
	Retracted  int
	Unchanged  int
	Duration   time.Duration
}

type Recognizer struct {
	store    Store
	log      *slog.Logger
	now      func() time.Time
	interval time.Duration
	lastVer  int64
	hasVer   bool
}

func NewRecognizer(store Store, log *slog.Logger, interval time.Duration) *Recognizer {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Recognizer{store: store, log: log, now: time.Now, interval: interval}
}

// Run menghitung ulang kebakaran setiap interval, tetapi hanya bila data deteksi berubah sejak putaran
// sebelumnya. Hasilnya selalu turunan dari isi database, jadi aman dihentikan dan dinyalakan kapan saja.
// Berhenti dengan error bila fencing menolak (bukan lagi leader).
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

// Tick menghitung ulang bila versi data berubah. Bila gagal, putaran berikutnya mencoba lagi.
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
		"unchanged", res.Unchanged, "ms", res.Duration.Milliseconds())
	return nil
}

// Recompute mengklasterkan semua deteksi aktif lalu menyimpan hanya kebakaran yang baru atau berubah.
// Kebakaran tersimpan yang tidak lagi dihasilkan ditandai ditarik (retracted), bukan dihapus.
func (r *Recognizer) Recompute(ctx context.Context) (Result, error) {
	start := time.Now()
	dets, err := r.store.ActiveDetections(ctx)
	if err != nil {
		return Result{}, err
	}
	existing, err := r.store.ListWildfires(ctx)
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
		h := ToHazard(BuildFire(g), now)
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

// retractedCopy menandai kebakaran sebagai ditarik: severity NORMAL dan attributes.retracted = true.
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

// SameHazard membandingkan isi yang bisa berubah antara dua versi kebakaran yang sama.
func SameHazard(a, b model.HazardEvent) bool {
	if a.Severity != b.Severity || a.AreaName != b.AreaName ||
		a.Latitude != b.Latitude || a.Longitude != b.Longitude || !a.OccurredAt.Equal(b.OccurredAt) {
		return false
	}
	aj, err1 := json.Marshal(a.Attributes)
	bj, err2 := json.Marshal(b.Attributes)
	return err1 == nil && err2 == nil && bytes.Equal(aj, bj)
}
