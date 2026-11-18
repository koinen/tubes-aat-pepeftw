package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.UnprocessedWriter = (*PostgresRepository)(nil)
	_ repository.RetryQueue        = (*PostgresRepository)(nil)
)

const insertUnprocessed = `
INSERT INTO unprocessed_messages (message_id, source, endpoint, source_ref_id, received_at, reason, retryable,
                                  attempts, status, payload, payload_size, payload_sha256, hazard, correlation_id,
                                  last_attempt_at, next_attempt_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::TEXT, $10, $11, $12, $13, $14,
        now(), CASE WHEN $9::TEXT = 'retrying' THEN now() + INTERVAL '5 seconds' END, now())
ON CONFLICT (message_id) DO NOTHING`

const selectDueRetries = `
SELECT message_id, source, endpoint, source_ref_id, attempts, hazard, correlation_id
FROM unprocessed_messages
WHERE status = 'retrying' AND next_attempt_at <= now() AND hazard IS NOT NULL
ORDER BY next_attempt_at
LIMIT $1`

const markRecovered = `
UPDATE unprocessed_messages
SET status = 'recovered', attempts = attempts + 1, last_attempt_at = now(), next_attempt_at = NULL, updated_at = now()
WHERE message_id = $1 AND status = 'retrying'`

// Backoff after the n-th failed attempt is 5 s · 2^n, capped at 5 min. The exponent is capped too,
// so a message retried for days never overflows the interval.
const markRetryFailed = `
UPDATE unprocessed_messages
SET attempts = attempts + 1, last_attempt_at = now(), updated_at = now(),
    next_attempt_at = now() + LEAST(INTERVAL '5 seconds' * pow(2, LEAST(attempts, 6)), INTERVAL '5 minutes')
WHERE message_id = $1 AND status = 'retrying'`

const markParked = `
UPDATE unprocessed_messages
SET status = 'parked', retryable = false, reason = $2, attempts = attempts + 1,
    last_attempt_at = now(), next_attempt_at = NULL, updated_at = now()
WHERE message_id = $1 AND status = 'retrying'`

func (p *PostgresRepository) SaveUnprocessed(ctx context.Context, m model.UnprocessedMessage) (bool, error) {
	var hazard []byte
	if m.Hazard != nil {
		var err error
		if hazard, err = json.Marshal(m.Hazard); err != nil {
			return false, fmt.Errorf("save unprocessed %s: %w", m.MessageID, err)
		}
	}
	payload := m.Payload
	if payload == nil {
		payload = []byte{}
	}
	tag, err := p.fencedExecRetry(ctx, insertUnprocessed,
		m.MessageID, m.Source, nullable(m.Endpoint), nullable(m.SourceRefID), m.ReceivedAt.UTC(), m.Reason, m.Retryable,
		m.Attempts, string(m.Status), payload, m.PayloadSize, m.PayloadSHA256, hazard, nullable(m.CorrelationID))
	if err != nil {
		return false, fmt.Errorf("save unprocessed %s: %w", m.MessageID, storeError(ctx, err))
	}
	return tag.RowsAffected() == 1, nil
}

func (p *PostgresRepository) DueRetries(ctx context.Context, limit int) ([]model.UnprocessedMessage, error) {
	rows, err := p.pool.Query(ctx, selectDueRetries, limit)
	if err != nil {
		return nil, fmt.Errorf("due retries: %w", storeError(ctx, err))
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.UnprocessedMessage, error) {
		var (
			m                  model.UnprocessedMessage
			endpoint, ref, cid *string
			hazard             []byte
		)
		if err := row.Scan(&m.MessageID, &m.Source, &endpoint, &ref, &m.Attempts, &hazard, &cid); err != nil {
			return m, err
		}
		m.Endpoint, m.SourceRefID, m.CorrelationID = deref(endpoint), deref(ref), deref(cid)
		m.Status, m.Retryable = model.StatusRetrying, true
		// An unreadable hazard leaves m.Hazard nil instead of failing the batch; the worker parks it.
		var h model.HazardEvent
		if err := json.Unmarshal(hazard, &h); err == nil {
			m.Hazard = &h
		}
		return m, nil
	})
	if err != nil {
		return nil, fmt.Errorf("due retries: %w", storeError(ctx, err))
	}
	return out, nil
}

// RecoverRetry saves the hazard, its outbox event, and the recovered status in one transaction.
// If the hazard already exists (a later poll saved it), the message is still recovered.
func (p *PostgresRepository) RecoverRetry(ctx context.Context, m model.UnprocessedMessage) error {
	if m.Hazard == nil {
		return fmt.Errorf("recover %s: no hazard to save", m.MessageID)
	}
	ctx = correlation.With(ctx, m.CorrelationID)
	h, msg, payload, err := p.hazardMessage(*m.Hazard)
	if err != nil {
		return fmt.Errorf("recover %s: %w", m.MessageID, err)
	}
	err = withIdempotentRetry(ctx, func() error {
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			if _, err := insertHazardWithOutbox(ctx, tx, h, msg, payload); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, markRecovered, m.MessageID)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("recover %s: %w", m.MessageID, storeError(ctx, err))
	}
	return nil
}

func (p *PostgresRepository) RetryFailed(ctx context.Context, messageID string) error {
	if _, err := p.fencedExecRetry(ctx, markRetryFailed, messageID); err != nil {
		return fmt.Errorf("retry failed %s: %w", messageID, storeError(ctx, err))
	}
	return nil
}

func (p *PostgresRepository) ParkRetry(ctx context.Context, messageID, reason string) error {
	if _, err := p.fencedExecRetry(ctx, markParked, messageID, reason); err != nil {
		return fmt.Errorf("park %s: %w", messageID, storeError(ctx, err))
	}
	return nil
}

// storeError tags a write error for callers outside this package: ErrRejected when the store refused
// a value (SQLSTATE class 22 data exception, class 23 constraint violation), ErrUnavailable for outages. Lost leadership, shutdown, and our own SQL
// bugs (class 42) pass through untagged, so they are never mistaken for a message failure.
func storeError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, fence.ErrStaleEpoch) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "22"), strings.HasPrefix(pgErr.Code, "23"):
			return fmt.Errorf("%w: %w", repository.ErrRejected, err)
		case strings.HasPrefix(pgErr.Code, "42"):
			return err
		}
	}
	return fmt.Errorf("%w: %w", repository.ErrUnavailable, err)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
