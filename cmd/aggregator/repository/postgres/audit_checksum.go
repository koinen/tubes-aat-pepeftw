package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

func (p *PostgresRepository) AuditChecksum(ctx context.Context, source model.Source) (repository.Checksum, error) {
	refs, unavailable, asOf, err := gather(ctx, p, func(ctx context.Context, aost string, s repository.Shard) ([]string, error) {
		sql := "SELECT source_ref_id FROM hazard_events " + aost + " WHERE source = $1 AND " + shardRange(s)
		rows, err := p.pool.Query(ctx, sql, string(source))
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, pgx.RowTo[string])
	})
	if err != nil {
		return repository.Checksum{}, fmt.Errorf("audit checksum: %w", err)
	}

	slices.Sort(refs)
	sum := sha256.Sum256([]byte(strings.Join(refs, "\n")))
	return repository.Checksum{
		Source:           source,
		Count:            len(refs),
		Checksum:         hex.EncodeToString(sum[:]),
		AsOf:             asOf,
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	}, nil
}
