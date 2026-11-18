package postgres

import (
	"context"
	"fmt"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/satellite"
)

const selectActiveDetections = `
SELECT detection_key, COALESCE(satellite, ''), acq_time, latitude, longitude,
       COALESCE(confidence, ''), COALESCE(frp, 0), COALESCE(received_at, now())
FROM satellite_detection
WHERE NOT retracted AND COALESCE(confidence, '') <> 'l'
  AND acq_time >= (SELECT max(acq_time) FROM satellite_detection) - ($1 * INTERVAL '1 second')`

const selectWildfires = selectHazardColumns + `
WHERE source = 'SATELIT' AND hazard_type = 'WILDFIRE' AND occurred_at >= $1`

const selectProgress = `
SELECT s.last_offset, (SELECT count(*) FROM satellite_late)
FROM satellite_feed_state s
WHERE s.id`

func (p *PostgresRepository) ActiveDetections(ctx context.Context) ([]satellite.Detection, error) {
	rows, err := p.pool.Query(ctx, selectActiveDetections, satellite.DetectionWindow.Seconds())
	if err != nil {
		return nil, fmt.Errorf("active detections: %w", err)
	}
	defer rows.Close()

	var out []satellite.Detection
	for rows.Next() {
		var d satellite.Detection
		if err := rows.Scan(&d.Key, &d.Satellite, &d.AcqTime, &d.Latitude, &d.Longitude, &d.Confidence, &d.FRP, &d.ReceivedAt); err != nil {
			return nil, fmt.Errorf("active detections: %w", err)
		}
		d.AcqTime = d.AcqTime.UTC()
		d.ReceivedAt = d.ReceivedAt.UTC()
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("active detections: %w", err)
	}
	return out, nil
}

func (p *PostgresRepository) ListWildfires(ctx context.Context, since time.Time) ([]model.HazardEvent, error) {
	rows, err := p.pool.Query(ctx, selectWildfires, since)
	if err != nil {
		return nil, fmt.Errorf("list wildfires: %w", err)
	}
	defer rows.Close()

	var out []model.HazardEvent
	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			return nil, fmt.Errorf("list wildfires: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list wildfires: %w", err)
	}
	return out, nil
}

func (p *PostgresRepository) DetectionVersion(ctx context.Context) (int64, error) {
	var v int64
	if err := p.pool.QueryRow(ctx, `SELECT COALESCE(max(last_offset), -1) FROM satellite_detection`).Scan(&v); err != nil {
		return 0, fmt.Errorf("detection version: %w", err)
	}
	return v, nil
}

func (p *PostgresRepository) Progress(ctx context.Context) (satellite.Progress, error) {
	var pr satellite.Progress
	if err := p.pool.QueryRow(ctx, selectProgress).Scan(&pr.Applied, &pr.Late); err != nil {
		return pr, fmt.Errorf("satellite progress: %w", err)
	}
	return pr, nil
}
