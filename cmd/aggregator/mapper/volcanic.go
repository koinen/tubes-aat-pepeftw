package mapper

import (
	"encoding/json"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type Volcano struct {
	Name      string
	Latitude  float64
	Longitude float64
}

type VolcanoLookup func(volcanoID string) (v Volcano, ok bool)

// Volcanic maps one PVMBG report. rules are the declared adaptations; the zero value applies none.
func Volcanic(raw json.RawMessage, lookup VolcanoLookup, rules VolcanicAdaptations, now time.Time) (model.HazardEvent, error) {
	f, err := decode(raw)
	if err != nil {
		return model.HazardEvent{}, err
	}
	acted, err := rules.apply(f)
	if err != nil {
		return model.HazardEvent{}, err
	}

	ref, err := f.takeString("report_id")
	if err != nil {
		return model.HazardEvent{}, err
	}
	f.ref = ref

	volcanoID, err := f.peekString("volcano_id")
	if err != nil {
		return model.HazardEvent{}, err
	}
	volcano, ok := lookup(volcanoID)
	if !ok {
		return model.HazardEvent{}, reject(ref, "volcano_id %q tidak ada di tabel volcano_ref", volcanoID)
	}

	levelRaw, err := f.takeString("alert_level")
	if err != nil {
		return model.HazardEvent{}, err
	}
	severity, ok := parseLevel(levelRaw)
	if !ok {
		return model.HazardEvent{}, reject(ref, "alert_level %q tidak dikenal (harus Normal/Waspada/Siaga/Awas)", levelRaw)
	}

	reported, err := f.takeTime("reported_at")
	if err != nil {
		return model.HazardEvent{}, err
	}

	for _, key := range []string{"eruption_count_24h", "ash_column_height_m"} {
		if _, present := f.peek(key); !present {
			if r, ok := rules.optional(key); ok {
				acted = append(acted, r)
				continue
			}
		}
		if _, err := f.peekNumber(key); err != nil {
			return model.HazardEvent{}, err
		}
	}

	rules.notify(acted, ref)
	return model.HazardEvent{
		HazardID:    HazardID(model.SourcePVMBG, ref),
		Source:      model.SourcePVMBG,
		SourceRefID: ref,
		HazardType:  model.HazardVolcanic,
		Severity:    severity,
		AreaName:    volcano.Name,
		Latitude:    volcano.Latitude,
		Longitude:   volcano.Longitude,
		OccurredAt:  reported,
		IngestedAt:  now.UTC().Truncate(time.Second),
		Attributes:  f.attributes(),
	}, nil
}
