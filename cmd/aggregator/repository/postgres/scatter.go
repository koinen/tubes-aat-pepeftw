package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	defaultFastTimeout  = 1500 * time.Millisecond
	defaultShardTimeout = 2 * time.Second
	snapshotLag         = time.Second
)

type shardQuery[T any] func(ctx context.Context, aost string, s repository.Shard) ([]T, error)

func gather[T any](ctx context.Context, p *PostgresRepository, run shardQuery[T]) ([]T, []string, time.Time, error) {
	asOf := p.snapshot(ctx).Truncate(time.Second)
	aost := fmt.Sprintf("AS OF SYSTEM TIME '%s'", asOf.UTC().Format("2006-01-02 15:04:05"))
	shards := repository.Shards()

	results := make([][]T, len(shards))
	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	for i, s := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardCtx, cancel := context.WithTimeout(ctx, p.shardTimeout)
			defer cancel()
			results[i], errs[i] = run(shardCtx, aost, s)
		}()
	}
	wg.Wait()

	var items []T
	var unavailable []string
	for i, err := range errs {
		if err == nil {
			items = append(items, results[i]...)
			continue
		}
		if isQueryBug(err) {
			return nil, nil, asOf, fmt.Errorf("%s: %w", shards[i].Name, err)
		}
		unavailable = append(unavailable, shards[i].Name)
	}
	if unavailable == nil {
		unavailable = []string{}
	}
	return items, unavailable, asOf, ctx.Err()
}

func (p *PostgresRepository) snapshot(ctx context.Context) time.Time {
	snapCtx, cancel := context.WithTimeout(ctx, p.shardTimeout/2)
	defer cancel()
	var ts time.Time
	if err := p.pool.QueryRow(snapCtx, "SELECT now() - INTERVAL '1 second'").Scan(&ts); err != nil {
		return time.Now().Add(-2 * snapshotLag)
	}
	return ts.UTC()
}

func shardRange(s repository.Shard) string {
	if s.High == "" {
		return fmt.Sprintf("hazard_id >= '%s'::UUID", s.Low)
	}
	return fmt.Sprintf("hazard_id >= '%s'::UUID AND hazard_id < '%s'::UUID", s.Low, s.High)
}

func isQueryBug(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "42") || strings.HasPrefix(pgErr.Code, "22")
	}
	return false
}

func isUnavailable(err error) bool {
	return err != nil && !isQueryBug(err)
}
