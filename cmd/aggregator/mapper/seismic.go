package mapper

import (
	"encoding/json"
	"slices"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type TsunamiWarning struct {
	WarningID      string
	RelatedEventID string
	ThreatLevel    model.Severity
	Fields         map[string]any
}

func ParseTsunamiWarning(raw json.RawMessage) (TsunamiWarning, error) {
	f, err := decode(raw)
	if err != nil {
		return TsunamiWarning{}, err
	}
	id, err := f.peekString("warning_id")
	if err != nil {
		return TsunamiWarning{}, err
	}
	f.ref = id
	related, err := f.peekString("related_event_id")
	if err != nil {
		return TsunamiWarning{}, err
	}
	levelRaw, err := f.peekString("threat_level")
	if err != nil {
		return TsunamiWarning{}, err
	}
	level, ok := parseLevel(levelRaw)
	if !ok || level == model.SeverityNormal {
		return TsunamiWarning{}, reject(id, "threat_level %q tidak dikenal (harus Waspada/Siaga/Awas)", levelRaw)
	}
	return TsunamiWarning{WarningID: id, RelatedEventID: related, ThreatLevel: level, Fields: f.attributes()}, nil
}

func SeismicSeverity(magnitude float64, warnings []TsunamiWarning) model.Severity {
	if len(warnings) > 0 {
		best := warnings[0].ThreatLevel
		for _, w := range warnings[1:] {
			if severityRank[w.ThreatLevel] > severityRank[best] {
				best = w.ThreatLevel
			}
		}
		return best
	}
	switch {
	case magnitude < 5.0:
		return model.SeverityNormal
	case magnitude < 6.5:
		return model.SeverityWaspada
	default:
		return model.SeveritySiaga
	}
}

func Seismic(raw json.RawMessage, warnings []TsunamiWarning, now time.Time) (model.HazardEvent, error) {
	f, err := decode(raw)
	if err != nil {
		return model.HazardEvent{}, err
	}

	ref, err := f.takeString("event_id")
	if err != nil {
		return model.HazardEvent{}, err
	}
	f.ref = ref
	area, err := f.takeString("region_name")
	if err != nil {
		return model.HazardEvent{}, err
	}
	lat, err := f.takeNumber("epicenter_lat")
	if err != nil {
		return model.HazardEvent{}, err
	}
	lon, err := f.takeNumber("epicenter_lon")
	if err != nil {
		return model.HazardEvent{}, err
	}
	if !validCoordinate(lat, lon) {
		return model.HazardEvent{}, reject(ref, "koordinat episentrum di luar rentang (%v, %v)", lat, lon)
	}
	occurred, err := f.takeTime("occurred_at")
	if err != nil {
		return model.HazardEvent{}, err
	}

	magnitude, err := f.peekNumber("magnitude")
	if err != nil {
		return model.HazardEvent{}, err
	}
	if _, err := f.peekNumber("depth_km"); err != nil {
		return model.HazardEvent{}, err
	}
	if _, err := f.peekBool("potential_tsunami"); err != nil {
		return model.HazardEvent{}, err
	}

	var related []TsunamiWarning
	for _, w := range warnings {
		if w.RelatedEventID == ref {
			related = append(related, w)
		}
	}
	slices.SortFunc(related, func(a, b TsunamiWarning) int {
		switch {
		case a.WarningID < b.WarningID:
			return -1
		case a.WarningID > b.WarningID:
			return 1
		}
		return 0
	})

	attrs := f.attributes()
	if len(related) > 0 {
		list := make([]any, 0, len(related))
		for _, w := range related {
			list = append(list, w.Fields)
		}
		attrs["tsunami_warnings"] = list
	}

	return model.HazardEvent{
		HazardID:    HazardID(model.SourceBMKG, ref),
		Source:      model.SourceBMKG,
		SourceRefID: ref,
		HazardType:  model.HazardSeismic,
		Severity:    SeismicSeverity(magnitude, related),
		AreaName:    area,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  occurred,
		IngestedAt:  now.UTC().Truncate(time.Second),
		Attributes:  attrs,
	}, nil
}