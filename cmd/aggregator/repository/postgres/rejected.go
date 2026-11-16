package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const insertRejected = `
INSERT INTO rejected_records (source, endpoint, source_ref_id, reason, payload, rejected_at)
VALUES ($1, $2, $3, $4, $5, $6)`

func (p *PostgresRepository) SaveRejected(ctx context.Context, r model.RejectedRecord) error {
	if len(r.Payload) == 0 {
		r.Payload = json.RawMessage("null")
	}
	if r.RejectedAt.IsZero() {
		r.RejectedAt = p.now()
	}

	_, err := p.pool.Exec(ctx, insertRejected,
		r.Source, r.Endpoint, nullable(r.SourceRefID), r.Reason, r.Payload, r.RejectedAt.UTC())
	if err != nil {
		return fmt.Errorf("save rejected %s/%s: %w", r.Source, r.Endpoint, err)
	}
	return nil
}
