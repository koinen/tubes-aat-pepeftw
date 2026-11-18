package postgres

import (
	"context"
	"fmt"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var _ repository.FieldReportWriter = (*PostgresRepository)(nil)

const insertFieldReport = `
INSERT INTO field_reports (report_id, subject, idempotency_key, latitude, longitude, reported_at, description,
                           hazard_id, body_sha256, accepted_at, correlation_id, kafka_partition, kafka_offset)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (report_id) DO NOTHING`

const selectFieldReportSHA = `SELECT body_sha256 FROM field_reports WHERE report_id = $1`

// SaveFieldReport is not fenced: every pod consumes its own partitions, and the insert is
// idempotent, so a record read twice (after a rebalance or a crash) is stored once.
func (p *PostgresRepository) SaveFieldReport(ctx context.Context, r model.FieldReport) (repository.FieldReportResult, error) {
	var inserted bool
	err := withIdempotentRetry(ctx, func() error {
		tag, err := p.pool.Exec(ctx, insertFieldReport,
			r.ReportID, r.Subject, r.IdempotencyKey, r.Latitude, r.Longitude, r.ReportedAt.UTC(), r.Description,
			nullable(r.HazardID), r.BodySHA256, r.AcceptedAt.UTC(), nullable(r.CorrelationID), r.Partition, r.Offset)
		inserted = err == nil && tag.RowsAffected() == 1
		return err
	})
	if err != nil {
		return "", fmt.Errorf("save field report %s: %w", r.ReportID, storeError(ctx, err))
	}
	if inserted {
		return repository.FieldReportInserted, nil
	}

	// The row exists and is never changed or deleted, so reading it after the insert is safe.
	var stored string
	err = withIdempotentRetry(ctx, func() error {
		return p.pool.QueryRow(ctx, selectFieldReportSHA, r.ReportID).Scan(&stored)
	})
	if err != nil {
		return "", fmt.Errorf("read field report %s: %w", r.ReportID, storeError(ctx, err))
	}
	if stored != r.BodySHA256 {
		return repository.FieldReportConflict, nil
	}
	return repository.FieldReportDuplicate, nil
}
