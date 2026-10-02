package fixture

import (
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func fixtureHazards(now time.Time) []model.HazardEvent {
	ago := func(m int) time.Time { return now.Add(-time.Duration(m) * time.Minute) }

	hazards := []model.HazardEvent{
		seismic("a0000000-0000-4000-8000-000000000001", "EQ-0001", "Selat Sunda", -6.10, 105.42, 6.8, model.SeveritySiaga, ago(55)),
		seismic("a0000000-0000-4000-8000-000000000002", "EQ-0002", "Palu", -0.90, 119.87, 4.6, model.SeverityNormal, ago(48)),
		seismic("a0000000-0000-4000-8000-000000000003", "EQ-0003", "Laut Banda", -6.50, 129.50, 5.7, model.SeverityWaspada, ago(40)),
		seismic("a0000000-0000-4000-8000-000000000004", "EQ-0004", "Cianjur", -6.82, 107.14, 5.1, model.SeverityWaspada, ago(31)),
		seismic("a0000000-0000-4000-8000-000000000005", "EQ-0005", "Selat Sunda", -6.20, 105.30, 7.1, model.SeverityAwas, ago(12)),
		volcanic("b0000000-0000-4000-8000-000000000001", "VR-0001", "Merapi", -7.54, 110.45, model.SeverityWaspada, 3, 400, ago(50)),
		volcanic("b0000000-0000-4000-8000-000000000002", "VR-0002", "Semeru", -8.11, 112.92, model.SeveritySiaga, 12, 1500, ago(35)),
		volcanic("b0000000-0000-4000-8000-000000000003", "VR-0003", "Sinabung", 3.17, 98.39, model.SeverityNormal, 0, 0, ago(25)),
		volcanic("b0000000-0000-4000-8000-000000000004", "VR-0004", "Anak Krakatau", -6.10, 105.42, model.SeverityAwas, 20, 2800, ago(8)),
	}

	withConfidence := volcanic("b0000000-0000-4000-8000-000000000005", "VR-0005", "Merapi", -7.54, 110.45, model.SeveritySiaga, 7, 900, ago(3))
	withConfidence.Attributes["confidence_level"] = 0.82
	return append(hazards, withConfidence)
}

func seismic(id, ref, area string, lat, lon, magnitude float64, sev model.Severity, occurred time.Time) model.HazardEvent {
	return model.HazardEvent{
		HazardID:    id,
		Source:      model.SourceBMKG,
		SourceRefID: ref,
		HazardType:  model.HazardSeismic,
		Severity:    sev,
		AreaName:    area,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  occurred,
		IngestedAt:  occurred.Add(2 * time.Second),
		Attributes: map[string]any{
			"magnitude":         magnitude,
			"depth_km":          10.0,
			"potential_tsunami": magnitude >= 6.5,
		},
	}
}

func volcanic(id, ref, volcano string, lat, lon float64, sev model.Severity, eruptions int, ashHeight float64, reported time.Time) model.HazardEvent {
	return model.HazardEvent{
		HazardID:    id,
		Source:      model.SourcePVMBG,
		SourceRefID: ref,
		HazardType:  model.HazardVolcanic,
		Severity:    sev,
		AreaName:    volcano,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  reported,
		IngestedAt:  reported.Add(3 * time.Second),
		Attributes: map[string]any{
			"eruption_count_24h":  eruptions,
			"ash_column_height_m": ashHeight,
		},
	}
}
