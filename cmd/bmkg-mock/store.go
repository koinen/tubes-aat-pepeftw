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

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKm          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`

	eventOccurredAt time.Time 
}

type region struct {
	name     string
	lat, lon float64
	zones    []string
}

var regions = []region{
	{"Laut Banda", -6.5, 129.5, []string{"Maluku Tengah", "Kepulauan Banda", "Seram Bagian Timur"}},
	{"Samudra Hindia Selatan Jawa Barat", -8.2, 107.2, []string{"Pangandaran", "Garut", "Sukabumi"}},
	{"Selat Sunda", -6.4, 105.3, []string{"Pandeglang", "Lampung Selatan", "Serang"}},
	{"Barat Daya Sumatera Barat", -1.8, 99.6, []string{"Kepulauan Mentawai", "Pesisir Selatan", "Padang"}},
	{"Laut Maluku", 1.5, 126.4, []string{"Bitung", "Halmahera Barat", "Ternate"}},
	{"Selatan Bali", -9.2, 115.1, []string{"Badung", "Denpasar", "Klungkung"}},
	{"Laut Flores", -7.6, 121.5, []string{"Flores Timur", "Sikka", "Kepulauan Selayar"}},
	{"Utara Papua", -2.4, 138.0, []string{"Jayapura", "Sarmi", "Biak Numfor"}},
	{"Selatan Nusa Tenggara Barat", -9.3, 117.4, []string{"Lombok Tengah", "Sumbawa", "Dompu"}},
	{"Barat Aceh", 3.9, 95.6, []string{"Aceh Barat", "Nagan Raya", "Simeulue"}},
	{"Laut Sulawesi", 2.6, 122.8, []string{"Tolitoli", "Buol", "Minahasa Utara"}},
	{"Selatan Jawa Timur", -9.1, 112.5, []string{"Malang", "Lumajang", "Blitar"}},
}

type Store struct {
	mu       sync.RWMutex
	events   []SeismicEvent
	warnings []TsunamiWarning
}

func (s *Store) add(ev SeismicEvent, w *TsunamiWarning) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	if w != nil {
		s.warnings = append(s.warnings, *w)
	}
}

func (s *Store) EventsSince(since time.Time) []SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SeismicEvent, 0)
	for _, e := range s.events {
		if since.IsZero() || !e.OccurredAt.Before(since) {
			out = append(out, e)
		}
	}
	slices.SortStableFunc(out, func(a, b SeismicEvent) int { return a.OccurredAt.Compare(b.OccurredAt) })
	return out
}

func (s *Store) WarningsSince(since time.Time) []TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TsunamiWarning, 0)
	for _, w := range s.warnings {
		if since.IsZero() || !w.eventOccurredAt.Before(since) {
			out = append(out, w)
		}
	}
	slices.SortStableFunc(out, func(a, b TsunamiWarning) int { return a.eventOccurredAt.Compare(b.eventOccurredAt) })
	return out
}

func (s *Store) Counts() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events), len(s.warnings)
}

type Generator struct {
	rng         *rand.Rand
	tsunamiRate float64
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

func threatLevelFor(magnitude float64) string {
	switch {
	case magnitude >= 7.6:
		return "Awas"
	case magnitude >= 7.0:
		return "Siaga"
	default:
		return "Waspada"
	}
}

func (g *Generator) make(at time.Time, eventID, warningID string) (SeismicEvent, *TsunamiWarning) {
	r := regions[g.rng.IntN(len(regions))]
	tsunami := g.rng.Float64() < g.tsunamiRate

	var mag, depth float64
	if tsunami {
		mag = 6.5 + g.rng.Float64()*1.5
		depth = 5 + g.rng.Float64()*35
	} else {
		mag = 3.0 + g.rng.Float64()*3.8
		depth = 5 + g.rng.Float64()*295
	}

	ev := SeismicEvent{
		EventID:          eventID,
		Magnitude:        round(mag, 1),
		DepthKm:          round(depth, 1),
		EpicenterLat:     round(r.lat+(g.rng.Float64()-0.5)*0.8, 4),
		EpicenterLon:     round(r.lon+(g.rng.Float64()-0.5)*0.8, 4),
		RegionName:       r.name,
		OccurredAt:       at.UTC().Truncate(time.Second),
		PotentialTsunami: tsunami,
	}
	if !tsunami {
		return ev, nil
	}

	n := 1 + g.rng.IntN(len(r.zones))
	zones := slices.Clone(r.zones[:n])
	w := &TsunamiWarning{
		WarningID:        warningID,
		RelatedEventID:   ev.EventID,
		ThreatLevel:      threatLevelFor(ev.Magnitude),
		AffectedZones:    zones,
		EstimatedArrival: ev.OccurredAt.Add(time.Duration(10+g.rng.IntN(31)) * time.Minute),
		eventOccurredAt:  ev.OccurredAt,
	}
	return ev, w
}

var seedBase = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

const seedTsunamiRate = 0.15

func seed(s *Store, count int) {
	g := &Generator{rng: rand.New(rand.NewPCG(4031, 1)), tsunamiRate: seedTsunamiRate}
	span := 30 * 24 * time.Hour
	for i := 1; i <= count; i++ {
		at := seedBase.Add(span * time.Duration(i) / time.Duration(count+1))
		at = at.Add(time.Duration(g.rng.IntN(3600)) * time.Second)
		ev, w := g.make(at, fmt.Sprintf("BMKG-SE-SEED-%03d", i), fmt.Sprintf("BMKG-TW-SEED-%03d", i))
		s.add(ev, w)
	}
	if _, nw := s.Counts(); nw == 0 {
		g.tsunamiRate = 1
		ev, w := g.make(seedBase.Add(span), fmt.Sprintf("BMKG-SE-SEED-%03d", count+1), fmt.Sprintf("BMKG-TW-SEED-%03d", count+1))
		s.add(ev, w)
	}
}

func runGenerator(ctx context.Context, s *Store, interval time.Duration, tsunamiRate float64, log *slog.Logger) {
	g := &Generator{rng: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7)), tsunamiRate: tsunamiRate}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			suffix := fmt.Sprintf("%s-%04x", now.UTC().Format("20060102T150405"), g.rng.IntN(0x10000))
			ev, w := g.make(now, "BMKG-SE-"+suffix, "BMKG-TW-"+suffix)
			s.add(ev, w)
			if w != nil {
				log.Info("seismic_event_generated", "event_id", ev.EventID, "magnitude", ev.Magnitude,
					"potential_tsunami", true, "warning_id", w.WarningID, "threat_level", w.ThreatLevel)
			} else {
				log.Info("seismic_event_generated", "event_id", ev.EventID, "magnitude", ev.Magnitude,
					"potential_tsunami", false)
			}
		}
	}
}