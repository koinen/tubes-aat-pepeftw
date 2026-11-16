package postgres

import (
	"fmt"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

type hazardQuery struct {
	where []string
	args  []any
}

func (q *hazardQuery) add(clause string, values ...any) {
	for _, v := range values {
		q.args = append(q.args, v)
		clause = strings.Replace(clause, "?", fmt.Sprintf("$%d", len(q.args)), 1)
	}
	q.where = append(q.where, clause)
}

func (q *hazardQuery) newerThan(since *time.Time) {
	if since != nil {
		q.add("ingested_at >= ?", *since)
	}
}

func (q *hazardQuery) olderThan(k *repository.Keyset) {
	if k != nil {
		q.add("ingested_at <= ? AND (ingested_at < ? OR hazard_id > ?::uuid)", k.At, k.At, k.ID)
	}
}

func (q *hazardQuery) afterAscending(k *repository.Keyset) {
	if k != nil {
		q.add("ingested_at >= ? AND (ingested_at > ? OR hazard_id > ?::uuid)", k.At, k.At, k.ID)
	}
}

func (q *hazardQuery) sql(prefix, extra, order string, limit int) (string, []any) {
	where := q.where
	if extra != "" {
		where = append(copyOf(where), extra)
	}
	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}
	args := append(copyOf(q.args), limit)
	return fmt.Sprintf("%s%s ORDER BY %s LIMIT $%d", prefix, clause, order, len(args)), args
}

func copyOf[T any](in []T) []T {
	return append([]T(nil), in...)
}
