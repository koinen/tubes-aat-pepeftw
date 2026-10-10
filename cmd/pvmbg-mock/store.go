package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

type VolcanicReport struct {
	ReportID         string   `json:"report_id"`
	VolcanoID        string   `json:"volcano_id"`
	AlertLevel       string   `json:"alert_level"`
	EruptionCount24h int      `json:"eruption_count_24h"`
	AshColumnHeightM float64  `json:"ash_column_height_m"`
	ReportedAt       string   `json:"reported_at"`
	ConfidenceLevel  *float64 `json:"confidence_level,omitempty"`

	reportedAt time.Time 
}

var volcanoIDs = []struct {
	id      string
	initial int
}{
	{"MERAPI", 2},
	{"SEMERU", 2},
	{"LEWOTOBI_LAKI_LAKI", 2},
	{"IBU", 2},
	{"SINABUNG", 1},
	{"ANAK_KRAKATAU", 1},
	{"LEWOTOLOK", 1},
	{"DUKONO", 1},
	{"KARANGETANG", 1},
	{"RUANG", 1},
	{"MARAPI", 1},
	{"BROMO", 0},
}

var alertLevels = []string{"Normal", "Waspada", "Siaga", "Awas"}

type Store struct {
	mu      sync.RWMutex
	reports []VolcanicReport
}

func (s *Store) add(r VolcanicReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports = append(s.reports, r)
}

func (s *Store) Since(since time.Time) []VolcanicReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]VolcanicReport, 0)
	for _, r := range s.reports {
		if since.IsZero() || !r.reportedAt.Before(since) {
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b VolcanicReport) int { return a.reportedAt.Compare(b.reportedAt) })
	return out
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.reports)
}

type Generator struct {
	rng    *rand.Rand
	levels map[string]int
}

func newGenerator(rng *rand.Rand) *Generator {
	g := &Generator{rng: rng, levels: map[string]int{}}
	for _, v := range volcanoIDs {
		g.levels[v.id] = v.initial
	}
	return g
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

func (g *Generator) make(at time.Time, reportID string, withConfidence bool) VolcanicReport {
	v := volcanoIDs[g.rng.IntN(len(volcanoIDs))]

	lvl := g.levels[v.id]
	switch p := g.rng.Float64(); {
	case p < 0.08 && lvl < 3:
		lvl++
	case p < 0.16 && lvl > 0:
		lvl--
	}
	g.levels[v.id] = lvl

	var eruptions int
	var ash float64
	switch lvl {
	case 1:
		eruptions = g.rng.IntN(3)
	case 2:
		eruptions = 2 + g.rng.IntN(14)
	case 3:
		eruptions = 10 + g.rng.IntN(51)
	}
	if eruptions > 0 {
		ash = float64(200*(lvl) + g.rng.IntN(1500*lvl))
	}

	ts := at.UTC().Truncate(time.Second)
	r := VolcanicReport{
		ReportID:         reportID,
		VolcanoID:        v.id,
		AlertLevel:       alertLevels[lvl],
		EruptionCount24h: eruptions,
		AshColumnHeightM: ash,
		ReportedAt:       ts.Format(time.RFC3339),
		reportedAt:       ts,
	}
	if withConfidence {
		c := round(0.5+g.rng.Float64()*0.49, 2)
		r.ConfidenceLevel = &c
	}
	return r
}

var seedBase = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func seed(s *Store, count int) *Generator {
	g := newGenerator(rand.New(rand.NewPCG(4031, 2)))
	span := 30 * 24 * time.Hour
	for i := 1; i <= count; i++ {
		at := seedBase.Add(span * time.Duration(i) / time.Duration(count+1))
		at = at.Add(time.Duration(g.rng.IntN(3600)) * time.Second)
		s.add(g.make(at, fmt.Sprintf("PVMBG-VR-SEED-%03d", i), false))
	}
	return g
}

func runGenerator(ctx context.Context, s *Store, g *Generator, st *State, interval time.Duration, log *slog.Logger) {
	g.rng = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 9))
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			id := fmt.Sprintf("PVMBG-VR-%s-%04x", now.UTC().Format("20060102T150405"), g.rng.IntN(0x10000))
			r := g.make(now, id, st.SchemaVersion() == 2)
			s.add(r)
			log.Info("volcanic_report_generated", "report_id", r.ReportID, "volcano_id", r.VolcanoID,
				"alert_level", r.AlertLevel, "schema_version", st.SchemaVersion())
		}
	}
}