package catalog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const refPrefix = "catalog-"

func ToHazard(row Row, now time.Time) (model.HazardEvent, error) {
	if row.EventID == "" {
		return model.HazardEvent{}, fmt.Errorf("line %d: event_id is empty", row.Line)
	}
	lat, err := number(row, "latitude", row.Latitude)
	if err != nil {
		return model.HazardEvent{}, err
	}
	lon, err := number(row, "longitude", row.Longitude)
	if err != nil {
		return model.HazardEvent{}, err
	}
	depth, err := number(row, "depth_km", row.DepthKm)
	if err != nil {
		return model.HazardEvent{}, err
	}
	mag, err := number(row, "magnitude", row.Magnitude)
	if err != nil {
		return model.HazardEvent{}, err
	}

	raw, err := json.Marshal(map[string]any{
		"event_id":          refPrefix + row.EventID,
		"region_name":       row.Place,
		"epicenter_lat":     lat,
		"epicenter_lon":     lon,
		"occurred_at":       row.Time,
		"magnitude":         mag,
		"depth_km":          depth,
		"potential_tsunami": false,
	})
	if err != nil {
		return model.HazardEvent{}, err
	}

	h, err := mapper.Seismic(raw, nil, now)
	if err != nil {
		return model.HazardEvent{}, fmt.Errorf("line %d: %w", row.Line, err)
	}
	h.Attributes["catalog"] = true
	return h, nil
}

func number(row Row, name, value string) (float64, error) {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("line %d: %s %q is not a number", row.Line, name, value)
	}
	return v, nil
}
