package poller

import (
	"encoding/json"
	"slices"
	"time"
)

type stationCadence struct {
	latest time.Time
	gaps   [32]time.Duration
	count  int
	next   int
}

func (p *sensorPantaiPoller) trackStations(pages []sensorPage) {
	for _, page := range pages {
		if page.epoch != p.epoch {
			continue
		}
		for _, raw := range page.items {
			var observation struct {
				StationID  string    `json:"station_id"`
				ObservedAt time.Time `json:"observed_at"`
			}
			if err := json.Unmarshal(raw, &observation); err != nil || observation.ObservedAt.IsZero() ||
				observation.ObservedAt.After(page.meta.GeneratedAt) {
				continue
			}
			if _, known := p.stations[observation.StationID]; !known {
				continue
			}
			cadence := p.cadences[observation.StationID]
			if cadence == nil {
				p.cadences[observation.StationID] = &stationCadence{latest: observation.ObservedAt}
				continue
			}
			if gap := observation.ObservedAt.Sub(cadence.latest); gap > 0 {
				cadence.latest = observation.ObservedAt
				cadence.gaps[cadence.next] = gap
				cadence.next = (cadence.next + 1) % len(cadence.gaps)
				cadence.count = min(cadence.count+1, len(cadence.gaps))
			}
		}
	}
}

func (p *sensorPantaiPoller) stationSilence(breakerClosed bool) map[string]*bool {
	status := make(map[string]*bool, len(p.stations))
	for id := range p.stations {
		status[id] = nil
		cadence := p.cadences[id]
		if !breakerClosed || !p.clockSampleCurrent || p.equalPolls >= 3 || cadence == nil || cadence.count == 0 {
			continue
		}
		gaps := cadence.gaps
		slices.Sort(gaps[:cadence.count])
		middle := cadence.count / 2
		median := gaps[middle]
		if cadence.count%2 == 0 {
			median = gaps[middle-1] + (median-gaps[middle-1])/2
		}
		age := p.generatedAt.Sub(cadence.latest)
		silent := age > 0 && (age-1)/10 >= median // age > 10*median, without overflow
		status[id] = &silent
	}
	return status
}
