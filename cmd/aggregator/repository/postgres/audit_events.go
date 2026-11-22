package postgres

import (
	"context"
	"fmt"
	"slices"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const oldestOrder = "ingested_at, hazard_id"

func (p *PostgresRepository) AuditEvents(ctx context.Context, f repository.AuditFilter) (repository.Page[model.HazardEvent], error) {
	var q hazardQuery
	if f.Source != nil {
		q.add("source = ?", string(*f.Source))
	}
	q.newerThan(f.Since)
	q.afterAscending(f.Cursor)
	limit := f.EffectiveLimit()

	items, unavailable, asOf, err := gather(ctx, p, func(ctx context.Context, aost string, s repository.Shard) ([]model.HazardEvent, error) {
		sql, args := q.sql(selectHazardColumns+" "+aost, shardRange(s), oldestOrder, limit+1)
		return queryHazards(ctx, p, sql, args...)
	})
	if err != nil {
		return repository.Page[model.HazardEvent]{}, fmt.Errorf("audit events: %w", err)
	}
	slices.SortFunc(items, oldestFirst)
	page := hazardPage(items, limit, unavailable)
	page.AsOf = asOf
	return page, nil
}
