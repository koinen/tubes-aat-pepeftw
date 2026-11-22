package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"

	"tubes-aat-pepeftw/internal/telemetry"
)

var _ repository.HazardBatchStore = (*PostgresRepository)(nil)

const insertHazardReturning = insertHazard + `
RETURNING hazard_id`

const getHazards = selectHazardColumns + `
WHERE hazard_id = ANY($1::UUID[])`

// SaveHazards stores hs in one fenced transaction. The hazard inserts go to the server as one
// pipelined batch, then the outbox inserts for the new ones as a second, so the cost is a few round
// trips whatever len(hs) is. A failure rolls back the whole batch.
func (p *PostgresRepository) SaveHazards(ctx context.Context, hs []model.HazardEvent) ([]repository.SaveOutcome, error) {
	if len(hs) == 0 {
		return nil, nil
	}
	type row struct {
		h       model.HazardEvent
		msg     model.HazardMessage
		payload []byte
	}
	rows := make([]row, len(hs))
	for i := range hs {
		h, msg, payload, err := p.hazardMessage(hs[i])
		if err != nil {
			return nil, fmt.Errorf("save hazard %s: %w", hs[i].HazardID, err)
		}
		rows[i] = row{h, msg, payload}
	}

	outcomes := make([]repository.SaveOutcome, len(rows))
	err := withIdempotentRetry(ctx, func() error {
		for i := range outcomes {
			outcomes[i] = repository.OutcomeUnchanged
		}
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			hazards := &pgx.Batch{}
			for _, r := range rows {
				h := r.h
				hazards.Queue(insertHazardReturning, h.HazardID, h.Source, h.SourceRefID, h.HazardType, h.Severity,
					h.AreaName, h.Latitude, h.Longitude, h.OccurredAt, h.IngestedAt, h.Attributes)
			}
			results := tx.SendBatch(ctx, hazards)
			for i := range rows {
				tag, err := results.Exec()
				if err != nil {
					results.Close()
					return err
				}
				if tag.RowsAffected() > 0 {
					outcomes[i] = repository.OutcomeCreated
				}
			}
			if err := results.Close(); err != nil {
				return err
			}

			events := &pgx.Batch{}
			cid, trace := nullable(correlation.From(ctx)), nullable(telemetry.TraceParent(ctx))
			for i, r := range rows {
				if outcomes[i] == repository.OutcomeCreated {
					events.Queue(insertOutbox, r.msg.EventID, r.h.HazardID, r.msg.EventType, r.payload, cid, trace,
						outboxPriority(r.h))
				}
			}
			if events.Len() == 0 {
				return nil
			}
			return tx.SendBatch(ctx, events).Close()
		})
	})
	if err != nil {
		return nil, fmt.Errorf("save %d hazards: %w", len(hs), storeError(ctx, err))
	}
	return outcomes, nil
}

func (p *PostgresRepository) GetHazards(ctx context.Context, ids []string) (map[string]model.HazardEvent, error) {
	out := make(map[string]model.HazardEvent, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := p.pool.Query(ctx, getHazards, ids)
	if err != nil {
		return nil, fmt.Errorf("get %d hazards: %w", len(ids), err)
	}
	defer rows.Close()
	for rows.Next() {
		h, err := scanHazard(rows)
		if err != nil {
			return nil, fmt.Errorf("get %d hazards: %w", len(ids), err)
		}
		out[h.HazardID] = h
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get %d hazards: %w", len(ids), err)
	}
	return out, nil
}
