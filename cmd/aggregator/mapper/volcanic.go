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

func Volcanic(raw json.RawMessage, lookup VolcanoLookup, now time.Time) (model.HazardEvent, error) {
	f, err := decode(raw)
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

	if _, err := f.peekNumber("eruption_count_24h"); err != nil {
		return model.HazardEvent{}, err
	}
	if _, err := f.peekNumber("ash_column_height_m"); err != nil {
		return model.HazardEvent{}, err
	}

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