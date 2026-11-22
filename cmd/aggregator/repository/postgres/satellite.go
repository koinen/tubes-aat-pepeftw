package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/satellite"
)

const (
	selectSatState = `SELECT last_offset, max_event_time FROM satellite_feed_state WHERE id`

	updateSatState = `UPDATE satellite_feed_state SET last_offset = $1, max_event_time = $2, updated_at = now() WHERE id`

	insertSatDetection = `
INSERT INTO satellite_detection (detection_key, satellite, acq_time, latitude, longitude, confidence, frp,
                                 first_offset, last_offset, received_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9)
ON CONFLICT (detection_key) DO NOTHING`

	insertSatLate = `
INSERT INTO satellite_late (feed_offset, detection_key, acq_time, watermark, received_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING`

	updateSatDetection = `
UPDATE satellite_detection
SET confidence = COALESCE($2, confidence), frp = COALESCE($3, frp), last_offset = $4
WHERE detection_key = $1`

	retractSatDetection = `UPDATE satellite_detection SET retracted = true, last_offset = $2 WHERE detection_key = $1`
)

type SatelliteState struct {
	LastOffset   int64
	MaxEventTime *time.Time
}

type SatelliteBatchResult struct {
	LastOffset int64
	Detections int
	Duplicates int
	Late       int
	Updates    int
	Retracts   int
	Orphans    int
	Gaps       int
}

func (p *PostgresRepository) SatelliteState(ctx context.Context) (SatelliteState, error) {
	var s SatelliteState
	if err := p.pool.QueryRow(ctx, selectSatState).Scan(&s.LastOffset, &s.MaxEventTime); err != nil {
		return SatelliteState{}, fmt.Errorf("read satellite state: %w", err)
	}
	return s, nil
}

func (p *PostgresRepository) ApplySatelliteBatch(ctx context.Context, events []satellite.Event,
	lateness time.Duration, receivedAt time.Time) (SatelliteBatchResult, error) {

	var res SatelliteBatchResult
	err := withIdempotentRetry(ctx, func() error {
		res = SatelliteBatchResult{}
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			var last int64
			var maxTime *time.Time
			if err := tx.QueryRow(ctx, selectSatState).Scan(&last, &maxTime); err != nil {
				return err
			}
			startOffset := last

			for _, e := range events {
				if e.Offset < 0 || e.Offset <= last {
					continue
				}
				if e.Kind == satellite.KindHeartbeat && e.Offset != last+1 {
					continue
				}
				if e.Offset != last+1 {
					res.Gaps++
				}

				switch e.Kind {
				case satellite.KindDetection:
					d := e.Detection
					if maxTime != nil {
						if wm := maxTime.Add(-lateness); d.AcqTime.Before(wm) {
							if _, err := tx.Exec(ctx, insertSatLate, e.Offset, d.Key, d.AcqTime, wm, receivedAt); err != nil {
								return err
							}
							res.Late++
							break
						}
					}
					tag, err := tx.Exec(ctx, insertSatDetection, d.Key, d.Satellite, d.AcqTime, d.Latitude,
						d.Longitude, d.Confidence, d.FRP, e.Offset, receivedAt)
					if err != nil {
						return err
					}
					if tag.RowsAffected() == 0 {
						res.Duplicates++
					} else {
						res.Detections++
					}
					if maxTime == nil || d.AcqTime.After(*maxTime) {
						t := d.AcqTime
						maxTime = &t
					}

				case satellite.KindUpdate:
					u := e.Update
					tag, err := tx.Exec(ctx, updateSatDetection, u.Key, u.Changes.Confidence, u.Changes.FRP, e.Offset)
					if err != nil {
						return err
					}
					if tag.RowsAffected() == 0 {
						res.Orphans++
					} else {
						res.Updates++
					}

				case satellite.KindRetract:
					tag, err := tx.Exec(ctx, retractSatDetection, e.Retract.Key, e.Offset)
					if err != nil {
						return err
					}
					if tag.RowsAffected() == 0 {
						res.Orphans++
					} else {
						res.Retracts++
					}
				}

				last = e.Offset
			}

			res.LastOffset = last
			if last != startOffset {
				if _, err := tx.Exec(ctx, updateSatState, last, maxTime); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return SatelliteBatchResult{}, fmt.Errorf("apply satellite batch: %w", err)
	}
	return res, nil
}
