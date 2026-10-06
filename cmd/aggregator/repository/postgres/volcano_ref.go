package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var _ repository.VolcanoRefRepository = (*PostgresRepository)(nil)

const selectVolcanoRefs = `
SELECT volcano_id, name, latitude, longitude
FROM volcano_ref
ORDER BY volcano_id`

func (p *PostgresRepository) ListVolcanoRefs(ctx context.Context) ([]model.VolcanoRef, error) {
	rows, err := p.pool.Query(ctx, selectVolcanoRefs)
	if err != nil {
		return nil, fmt.Errorf("list volcano refs: %w", err)
	}
	refs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.VolcanoRef, error) {
		var v model.VolcanoRef
		err := row.Scan(&v.VolcanoID, &v.Name, &v.Latitude, &v.Longitude)
		return v, err
	})
	if err != nil {
		return nil, fmt.Errorf("list volcano refs: %w", err)
	}
	return refs, nil
}
