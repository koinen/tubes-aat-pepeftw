package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tubes-aat-pepeftw/cmd/aggregator/correlation"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

var (
	_ repository.ReplayQueue = (*PostgresRepository)(nil)
	_ repository.ReplayJobs  = (*PostgresRepository)(nil)
)

const replayJobColumns = `job_id::TEXT, source, status, requested_by, adaptations, cutoff, last_received_at, last_message_id,
       selected, recovered, still_parked, handed_to_retry, not_replayable, error, created_at, started_at, finished_at`

// cutoff and created_at are both now(), the transaction's timestamp, so they are equal.
const insertReplayJob = `
INSERT INTO replay_requests (source, requested_by, adaptations, cutoff, created_at)
VALUES ($1, $2, $3, now(), now())
ON CONFLICT (source) WHERE status IN ('queued', 'running') DO NOTHING
RETURNING ` + replayJobColumns

const selectActiveReplayJob = `
SELECT ` + replayJobColumns + `
FROM replay_requests
WHERE source = $1 AND status IN ('queued', 'running')`

const selectReplayJob = `
SELECT ` + replayJobColumns + `
FROM replay_requests
WHERE job_id = $1`

const selectNextReplayJob = `
SELECT ` + replayJobColumns + `
FROM replay_requests
WHERE status IN ('queued', 'running')
ORDER BY status = 'running' DESC, created_at, job_id
LIMIT 1`

const startReplayJob = `
UPDATE replay_requests SET status = 'running', started_at = now()
WHERE job_id = $1 AND status = 'queued'`

const finishReplayJob = `
UPDATE replay_requests SET status = $2, error = $3, finished_at = now()
WHERE job_id = $1 AND status IN ('queued', 'running')`

// Truncated rows have payload_size > length(payload). Rows written before C3 have no payload_size
// and were stored intact.
const selectParkedPage = `
SELECT message_id, source, endpoint, source_ref_id, received_at, reason, attempts, payload,
       COALESCE(payload_size, length(payload)), COALESCE(payload_sha256, '')
FROM unprocessed_messages
WHERE source = $1 AND status = 'parked' AND received_at <= $2
  AND ($3::TIMESTAMPTZ IS NULL OR (received_at, message_id) > ($3::TIMESTAMPTZ, $4::TEXT))
ORDER BY received_at, message_id
LIMIT $5`

const markReplayRecovered = `
UPDATE unprocessed_messages
SET status = 'recovered', attempts = attempts + 1, last_attempt_at = now(), next_attempt_at = NULL, updated_at = now()
WHERE message_id = $1 AND status = 'parked'`

const markReplayStillParked = `
UPDATE unprocessed_messages
SET reason = $2, attempts = attempts + 1, last_attempt_at = now(), updated_at = now()
WHERE message_id = $1 AND status = 'parked'`

// The only parked → retrying transition.
const markReplayHandedToRetry = `
UPDATE unprocessed_messages
SET status = 'retrying', retryable = true, reason = $2, hazard = $3, correlation_id = $4, attempts = attempts + 1,
    last_attempt_at = now(), next_attempt_at = now() + INTERVAL '5 seconds', updated_at = now()
WHERE message_id = $1 AND status = 'parked'`

// The job moves only forward, so a transaction retried after an ambiguous commit counts nothing twice.
// %s is a counter column from replayCounters, never input.
const advanceReplayJob = `
UPDATE replay_requests
SET selected = selected + 1, %s = %s + 1, last_received_at = $2, last_message_id = $3
WHERE job_id = $1 AND status = 'running'
  AND (last_received_at IS NULL OR (last_received_at, last_message_id) < ($2::TIMESTAMPTZ, $3::TEXT))`

var replayCounters = map[repository.ReplayOutcome]string{
	repository.ReplayRecovered:     "recovered",
	repository.ReplayStillParked:   "still_parked",
	repository.ReplayHandedToRetry: "handed_to_retry",
	repository.ReplayNotReplayable: "not_replayable",
}

// RequestReplay inserts a queued job, or returns the source's active job when there is one.
// It is not fenced: every pod accepts replay requests.
func (p *PostgresRepository) RequestReplay(ctx context.Context, source model.Source, requestedBy, adaptations string) (model.ReplayJob, error) {
	// The active job can end between the insert and the select; then the next insert succeeds.
	for range 3 {
		job, err := scanReplayJob(p.pool.QueryRow(ctx, insertReplayJob, source, requestedBy, adaptations))
		if !errors.Is(err, pgx.ErrNoRows) {
			return job, wrapReplayErr(ctx, "request replay", err)
		}
		job, err = scanReplayJob(p.pool.QueryRow(ctx, selectActiveReplayJob, source))
		if !errors.Is(err, pgx.ErrNoRows) {
			return job, wrapReplayErr(ctx, "request replay", err)
		}
	}
	return model.ReplayJob{}, fmt.Errorf("request replay %s: the active job kept changing", source)
}

func (p *PostgresRepository) ReplayJob(ctx context.Context, jobID string) (model.ReplayJob, bool, error) {
	job, err := scanReplayJob(p.pool.QueryRow(ctx, selectReplayJob, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ReplayJob{}, false, nil
	}
	return job, err == nil, wrapReplayErr(ctx, "replay job", err)
}

func (p *PostgresRepository) NextJob(ctx context.Context) (model.ReplayJob, bool, error) {
	job, err := scanReplayJob(p.pool.QueryRow(ctx, selectNextReplayJob))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ReplayJob{}, false, nil
	}
	return job, err == nil, wrapReplayErr(ctx, "next replay job", err)
}

func (p *PostgresRepository) StartJob(ctx context.Context, jobID string) error {
	_, err := p.fencedExecRetry(ctx, startReplayJob, jobID)
	return wrapReplayErr(ctx, "start replay "+jobID, err)
}

func (p *PostgresRepository) FinishJob(ctx context.Context, jobID string, status model.ReplayStatus, errText string) error {
	_, err := p.fencedExecRetry(ctx, finishReplayJob, jobID, string(status), nullable(errText))
	return wrapReplayErr(ctx, "finish replay "+jobID, err)
}

func (p *PostgresRepository) ParkedPage(ctx context.Context, source model.Source, cutoff time.Time,
	after model.ReplayPosition, limit int) ([]model.UnprocessedMessage, error) {
	var afterAt *time.Time
	if after.MessageID != "" {
		t := after.ReceivedAt.UTC()
		afterAt = &t
	}
	rows, err := p.pool.Query(ctx, selectParkedPage, source, cutoff.UTC(), afterAt, after.MessageID, limit)
	if err != nil {
		return nil, wrapReplayErr(ctx, "parked page", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (model.UnprocessedMessage, error) {
		var (
			m             model.UnprocessedMessage
			endpoint, ref *string
		)
		err := row.Scan(&m.MessageID, &m.Source, &endpoint, &ref, &m.ReceivedAt, &m.Reason, &m.Attempts,
			&m.Payload, &m.PayloadSize, &m.PayloadSHA256)
		m.Endpoint, m.SourceRefID, m.Status = deref(endpoint), deref(ref), model.StatusParked
		return m, err
	})
	return out, wrapReplayErr(ctx, "parked page", err)
}

// RecordStep applies one row's outcome and advances the job in one fenced transaction.
// After an ambiguous commit the retried transaction finds the hazard already stored, so created can
// be false for a hazard this call did store; an adaptation is then undercounted, never counted twice.
func (p *PostgresRepository) RecordStep(ctx context.Context, jobID string, s repository.ReplayStep) (bool, error) {
	counter, ok := replayCounters[s.Outcome]
	if !ok {
		return false, fmt.Errorf("record replay step %s: unknown outcome %q", s.Message.MessageID, s.Outcome)
	}
	m := s.Message
	var (
		hazard  model.HazardEvent
		msg     model.HazardMessage
		payload []byte
		stored  []byte
	)
	if s.Outcome == repository.ReplayRecovered || s.Outcome == repository.ReplayHandedToRetry {
		if m.Hazard == nil {
			return false, fmt.Errorf("record replay step %s: no hazard", m.MessageID)
		}
		var err error
		if hazard, msg, payload, err = p.hazardMessage(*m.Hazard); err != nil {
			return false, fmt.Errorf("record replay step %s: %w", m.MessageID, err)
		}
		if stored, err = json.Marshal(m.Hazard); err != nil {
			return false, fmt.Errorf("record replay step %s: %w", m.MessageID, err)
		}
	}
	ctx = correlation.With(ctx, m.CorrelationID)
	var created bool
	err := withIdempotentRetry(ctx, func() error {
		created = false
		return p.fencedTx(ctx, func(tx pgx.Tx) error {
			var err error
			switch s.Outcome {
			case repository.ReplayRecovered:
				if created, err = insertHazardWithOutbox(ctx, tx, hazard, msg, payload); err == nil {
					_, err = tx.Exec(ctx, markReplayRecovered, m.MessageID)
				}
			case repository.ReplayStillParked:
				_, err = tx.Exec(ctx, markReplayStillParked, m.MessageID, m.Reason)
			case repository.ReplayHandedToRetry:
				_, err = tx.Exec(ctx, markReplayHandedToRetry, m.MessageID, m.Reason, stored, nullable(m.CorrelationID))
			}
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, fmt.Sprintf(advanceReplayJob, counter, counter), jobID, m.ReceivedAt.UTC(), m.MessageID)
			return err
		})
	})
	if err != nil {
		return false, wrapReplayErr(ctx, "record replay step "+m.MessageID, err)
	}
	return created, nil
}

func scanReplayJob(row pgx.Row) (model.ReplayJob, error) {
	var (
		j         model.ReplayJob
		lastAt    *time.Time
		lastID    *string
		errText   *string
		status    string
		jobSource string
	)
	err := row.Scan(&j.JobID, &jobSource, &status, &j.RequestedBy, &j.Adaptations, &j.Cutoff, &lastAt, &lastID,
		&j.Selected, &j.Recovered, &j.StillParked, &j.HandedToRetry, &j.NotReplayable, &errText,
		&j.CreatedAt, &j.StartedAt, &j.FinishedAt)
	if err != nil {
		return model.ReplayJob{}, err
	}
	j.Source, j.Status, j.Error = model.Source(jobSource), model.ReplayStatus(status), deref(errText)
	if lastAt != nil && lastID != nil {
		j.Position = model.ReplayPosition{ReceivedAt: *lastAt, MessageID: *lastID}
	}
	return j, nil
}

func wrapReplayErr(ctx context.Context, what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", what, storeError(ctx, err))
}
