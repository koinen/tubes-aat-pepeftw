package postgres

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.HazardWriter           = (*PostgresRepository)(nil)
	_ repository.HazardReader           = (*PostgresRepository)(nil)
	_ repository.CursorRepository       = (*PostgresRepository)(nil)
	_ repository.SourceStatusRepository = (*PostgresRepository)(nil)
	_ repository.RejectedRepository     = (*PostgresRepository)(nil)
)

type PostgresRepository struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func New(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool: pool,
		now:  func() time.Time { return time.Now().UTC().Truncate(time.Second) },
	}
}
