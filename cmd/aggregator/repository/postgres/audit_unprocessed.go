package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const listUnprocessed = `
SELECT message_id, source, received_at, reason, retryable, attempts, status
FROM unprocessed_messages
WHERE status IN ('retrying', 'parked')
  AND ($1::text IS NULL OR source = $1)
  AND ($2::timestamptz IS NULL OR received_at > $2 OR (received_at = $2 AND message_id > $3))
ORDER BY received_at, message_id
LIMIT $4`

func (p *PostgresRepository) Unprocessed(ctx context.Context, f repository.AuditFilter) (repository.Page[repository.UnprocessedMessage], error) {
	var source *string
	if f.Source != nil {
		s := string(*f.Source)
		source = &s
	}
	var after, afterID any
	if f.Cursor != nil {
		after, afterID = f.Cursor.At, f.Cursor.ID
	}
	limit := f.EffectiveLimit()

	queryCtx, cancel := context.WithTimeout(ctx, p.shardTimeout)
	defer cancel()
	rows, err := p.pool.Query(queryCtx, listUnprocessed, source, after, afterID, limit+1)
	if err != nil {
		return repository.Page[repository.UnprocessedMessage]{}, unprocessedError(err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (repository.UnprocessedMessage, error) {
		var m repository.UnprocessedMessage
		err := row.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.Reason, &m.Retryable, &m.Attempts, &m.Status)
		m.ReceivedAt = m.ReceivedAt.UTC()
		return m, err
	})
	if err != nil {
		return repository.Page[repository.UnprocessedMessage]{}, unprocessedError(err)
	}

	page := repository.Page[repository.UnprocessedMessage]{Items: items, UnavailableUnits: []string{}}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[limit-1]
		next := repository.Keyset{At: last.ReceivedAt, ID: last.MessageID}.Encode()
		page.NextCursor = &next
	}
	if page.Items == nil {
		page.Items = []repository.UnprocessedMessage{}
	}
	return page, nil
}

func unprocessedError(err error) error {
	if isUnavailable(err) {
		return fmt.Errorf("unprocessed: %w: %v", repository.ErrUnavailable, err)
	}
	return fmt.Errorf("unprocessed: %w", err)
}
