package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const analyticsCategory = `CASE
  WHEN hazard_type = 'SEISMIC' AND jsonb_typeof(attributes->'tsunami_warnings') = 'array'
       AND jsonb_array_length(attributes->'tsunami_warnings') > 0 THEN 'TSUNAMI'
  ELSE hazard_type
END`

type analyticsRow struct {
	category string
	bucket   time.Time
	severity string
	area     string
	count    int
}

func (p *PostgresRepository) Analytics(ctx context.Context, f repository.AnalyticsFilter) (repository.Analytics, error) {
	rows, unavailable, asOf, err := gather(ctx, p, func(ctx context.Context, aost string, s repository.Shard) ([]analyticsRow, error) {
		sql := "SELECT " + analyticsCategory + ", date_trunc($3, occurred_at), severity, area_name, count(*)" +
			" FROM hazard_events " + aost +
			fmt.Sprintf(" WHERE occurred_at >= $1 AND occurred_at < $2 AND %s", shardRange(s)) +
			" GROUP BY 1, 2, 3, 4"
		result, err := p.pool.Query(ctx, sql, f.From, f.To, f.Bucket)
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(result, func(row pgx.CollectableRow) (analyticsRow, error) {
			var r analyticsRow
			err := row.Scan(&r.category, &r.bucket, &r.severity, &r.area, &r.count)
			return r, err
		})
	})
	if err != nil {
		return repository.Analytics{}, fmt.Errorf("analytics: %w", err)
	}

	out := merge(rows)
	out.AsOf = asOf
	out.UnavailableUnits = unavailable
	out.Partial = len(unavailable) > 0
	return out, nil
}

func merge(rows []analyticsRow) repository.Analytics {
	type seriesKey struct {
		bucket   time.Time
		category string
	}
	series := map[seriesKey]int{}
	severity := map[string]int{}
	area := map[string]int{}
	total := 0
	for _, r := range rows {
		series[seriesKey{r.bucket.UTC(), r.category}] += r.count
		severity[r.severity] += r.count
		area[r.area] += r.count
		total += r.count
	}

	out := repository.Analytics{Total: total, Series: make([]repository.SeriesPoint, 0, len(series))}
	for k, n := range series {
		out.Series = append(out.Series, repository.SeriesPoint{Bucket: k.bucket, Category: k.category, Count: n})
	}
	sort.Slice(out.Series, func(i, j int) bool {
		if !out.Series[i].Bucket.Equal(out.Series[j].Bucket) {
			return out.Series[i].Bucket.Before(out.Series[j].Bucket)
		}
		return out.Series[i].Category < out.Series[j].Category
	})
	out.BySeverity = sortedCounts(severity)
	out.ByArea = sortedCounts(area)
	return out
}

func sortedCounts(m map[string]int) []repository.NamedCount {
	out := make([]repository.NamedCount, 0, len(m))
	for name, n := range m {
		out = append(out, repository.NamedCount{Name: name, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	return out
}
