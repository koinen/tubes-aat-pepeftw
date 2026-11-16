package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	rolePrimary = "primary"
	roleReplica = "replica"
)

func (p *PostgresRepository) HazardLocation(ctx context.Context, hazardID string) (repository.HazardLocation, error) {
	id, err := uuid.Parse(hazardID)
	if err != nil {
		return repository.HazardLocation{}, repository.ErrNotFound
	}
	result := repository.HazardLocation{HazardID: id.String(), Locations: []repository.Location{}}

	existsCtx, cancel := context.WithTimeout(ctx, p.shardTimeout)
	var one int
	err = p.pool.QueryRow(existsCtx, "SELECT 1 FROM hazard_events WHERE hazard_id = $1", id.String()).Scan(&one)
	cancel()
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return repository.HazardLocation{}, repository.ErrNotFound
	case isQueryBug(err):
		return repository.HazardLocation{}, fmt.Errorf("hazard location: %w", err)
	case err != nil:
		result.Partial = true
	}

	rangeCtx, cancel := context.WithTimeout(ctx, p.shardTimeout)
	defer cancel()
	var leaseHolder int64
	var replicas []int64
	var localities []string
	sql := fmt.Sprintf("SELECT lease_holder, replicas, replica_localities FROM [SHOW RANGE FROM TABLE hazard_events FOR ROW ('%s'::UUID)]", id.String())
	if err := p.pool.QueryRow(rangeCtx, sql).Scan(&leaseHolder, &replicas, &localities); err != nil {
		if isQueryBug(err) {
			return repository.HazardLocation{}, fmt.Errorf("hazard location: %w", err)
		}
		result.Partial = true
		return result, nil
	}

	unit := repository.ShardName(repository.ShardOf(id.String()))
	result.Locations = locations(unit, leaseHolder, replicas, localities)
	return result, nil
}

func locations(unit string, leaseHolder int64, replicas []int64, localities []string) []repository.Location {
	out := make([]repository.Location, 0, len(replicas))
	replicaNo := 0
	for i, nodeID := range replicas {
		node := ""
		if i < len(localities) {
			node = nodeName(localities[i])
		}
		if nodeID == leaseHolder {
			out = append([]repository.Location{{Node: node, Unit: unit, Role: rolePrimary}}, out...)
			continue
		}
		replicaNo++
		out = append(out, repository.Location{Node: node, Unit: fmt.Sprintf("%s-r%d", unit, replicaNo), Role: roleReplica})
	}
	return out
}

func nodeName(locality string) string {
	for _, tier := range strings.Split(locality, ",") {
		if name, ok := strings.CutPrefix(tier, "node="); ok {
			return name
		}
	}
	return locality
}
