package satellite

import (
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// ToHazard memetakan satu kebakaran menjadi HazardEvent WILDFIRE. Hazard ID turunan dari
// source_ref_id, jadi kebakaran yang sama selalu menunjuk baris yang sama.
func ToHazard(f Fire, now time.Time) model.HazardEvent {
	return model.HazardEvent{
		HazardID:    mapper.HazardID(model.SourceSatelit, f.SourceRefID),
		Source:      model.SourceSatelit,
		SourceRefID: f.SourceRefID,
		HazardType:  model.HazardWildfire,
		Severity:    model.Severity(f.Severity),
		AreaName:    NearestProvince(f.Latitude, f.Longitude),
		Latitude:    f.Latitude,
		Longitude:   f.Longitude,
		OccurredAt:  f.OccurredAt,
		IngestedAt:  now.UTC().Truncate(time.Second),
		Attributes: map[string]any{
			"detection_count": f.Count,
			"satellites":      f.Satellites,
			"max_confidence":  f.MaxConfidence,
			"total_frp_mw":    f.TotalFRP,
			"detection_keys":  f.DetectionKeys,
		},
	}
}
