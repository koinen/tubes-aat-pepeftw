package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
)

const raiseFence = `UPDATE leader_fence SET epoch = $1 WHERE id AND epoch <= $1`

func checkFence(ctx context.Context, tx pgx.Tx) error {
	epoch, ok := fence.Epoch(ctx)
	if !ok {
		return nil
	}
	tag, err := tx.Exec(ctx, raiseFence, epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: epoch %d", fence.ErrStaleEpoch, epoch)
	}
	return nil
}

func (p *PostgresRepository) fencedTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := checkFence(ctx, tx); err != nil {
			return err
		}
		return fn(tx)
	})
}

func (p *PostgresRepository) fencedExec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := p.fencedTx(ctx, func(tx pgx.Tx) error {
		var err error
		tag, err = tx.Exec(ctx, sql, args...)
		return err
	})
	return tag, err
}

func (p *PostgresRepository) fencedExecRetry(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := withIdempotentRetry(ctx, func() error {
		var err error
		tag, err = p.fencedExec(ctx, sql, args...)
		return err
	})
	return tag, err
}
