package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const newestOrder = "ingested_at DESC, hazard_id"

func (p *PostgresRepository) ListHazards(ctx context.Context, f repository.HazardFilter) (repository.Page[model.HazardEvent], error) {
	var q hazardQuery
	if f.Type != nil {
		q.add("hazard_type = ?", string(*f.Type))
	}
	q.newerThan(f.Since)
	q.olderThan(f.Cursor)
	limit := f.EffectiveLimit()

	fastCtx, cancel := context.WithTimeout(ctx, p.fastTimeout)
	sql, args := q.sql(selectHazardColumns, "", newestOrder, limit+1)
	items, err := queryHazards(fastCtx, p, sql, args...)
	cancel()
	if err == nil {
		return hazardPage(items, limit, nil), nil
	}
	if !isUnavailable(err) || ctx.Err() != nil {
		return repository.Page[model.HazardEvent]{}, fmt.Errorf("list hazards: %w", err)
	}

	items, unavailable, asOf, err := gather(ctx, p, func(ctx context.Context, aost string, s repository.Shard) ([]model.HazardEvent, error) {
		sql, args := q.sql(selectHazardColumns+" "+aost, shardRange(s), newestOrder, limit+1)
		return queryHazards(ctx, p, sql, args...)
	})
	if err != nil {
		return repository.Page[model.HazardEvent]{}, fmt.Errorf("list hazards: %w", err)
	}
	slices.SortFunc(items, newestFirst)
	page := hazardPage(items, limit, unavailable)
	page.AsOf = asOf
	return page, nil
}

func queryHazards(ctx context.Context, p *PostgresRepository, sql string, args ...any) ([]model.HazardEvent, error) {
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.HazardEvent, error) {
		return scanHazard(row)
	})
}

func hazardPage(items []model.HazardEvent, limit int, unavailable []string) repository.Page[model.HazardEvent] {
	page := repository.Page[model.HazardEvent]{Items: items, UnavailableUnits: unavailable}
	if page.UnavailableUnits == nil {
		page.UnavailableUnits = []string{}
	}
	page.Partial = len(page.UnavailableUnits) > 0
	more := len(items) > limit
	if more {
		page.Items = items[:limit]
	}
	if n := len(page.Items); n > 0 && (more || page.Partial) {
		last := page.Items[n-1]
		next := repository.Keyset{At: last.IngestedAt, ID: last.HazardID}.Encode()
		page.NextCursor = &next
	}
	if page.Items == nil {
		page.Items = []model.HazardEvent{}
	}
	return page
}

func newestFirst(a, b model.HazardEvent) int {
	if c := b.IngestedAt.Compare(a.IngestedAt); c != 0 {
		return c
	}
	return compareID(a.HazardID, b.HazardID)
}

func oldestFirst(a, b model.HazardEvent) int {
	if c := a.IngestedAt.Compare(b.IngestedAt); c != 0 {
		return c
	}
	return compareID(a.HazardID, b.HazardID)
}

func compareID(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
