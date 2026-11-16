package postgres

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.HazardWriter           = (*PostgresRepository)(nil)
	_ repository.HazardReader           = (*PostgresRepository)(nil)
	_ repository.AuditReader            = (*PostgresRepository)(nil)
	_ repository.CursorRepository       = (*PostgresRepository)(nil)
	_ repository.SourceStatusRepository = (*PostgresRepository)(nil)
	_ repository.RejectedRepository     = (*PostgresRepository)(nil)
	_ repository.OutboxRepository       = (*PostgresRepository)(nil)
)

type PostgresRepository struct {
	pool         *pgxpool.Pool
	now          func() time.Time
	fastTimeout  time.Duration
	shardTimeout time.Duration
}

func New(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool:         pool,
		now:          func() time.Time { return time.Now().UTC().Truncate(time.Second) },
		fastTimeout:  defaultFastTimeout,
		shardTimeout: defaultShardTimeout,
	}
}

func (p *PostgresRepository) WithReadTimeouts(fast, shard time.Duration) *PostgresRepository {
	if fast > 0 {
		p.fastTimeout = fast
	}
	if shard > 0 {
		p.shardTimeout = shard
	}
	return p
}
