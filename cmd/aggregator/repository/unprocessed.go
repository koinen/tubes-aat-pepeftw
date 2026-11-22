package repository

import (
	"context"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type UnprocessedWriter interface {
	// SaveUnprocessed inserts m unless its message_id is already stored. inserted reports which.
	SaveUnprocessed(ctx context.Context, m model.UnprocessedMessage) (inserted bool, err error)
}

// RetryQueue is the retry worker's view of retrying unprocessed messages.
type RetryQueue interface {
	// DueRetries returns up to limit retrying messages whose next attempt is due, oldest due first.
	// A message whose stored hazard cannot be read is returned with Hazard nil.
	DueRetries(ctx context.Context, limit int) ([]model.UnprocessedMessage, error)
	// RecoverRetry saves m.Hazard (with its outbox event) and marks m recovered, in one transaction.
	RecoverRetry(ctx context.Context, m model.UnprocessedMessage) error
	// RetryFailed counts a failed attempt and schedules the next one with capped backoff.
	RetryFailed(ctx context.Context, messageID string) error
	// ParkRetry moves a retrying message to parked with the given reason.
	ParkRetry(ctx context.Context, messageID, reason string) error
}

// ReplayOutcome is what a replay job did with one parked row.
type ReplayOutcome string

const (
	// ReplayRecovered saves Message.Hazard with its outbox event (correlation_id =
	// Message.CorrelationID) and marks the row recovered, attempts + 1.
	ReplayRecovered ReplayOutcome = "recovered"
	// ReplayStillParked keeps the row parked: attempts + 1, last_attempt_at = now(), and
	// Message.Reason as the latest failure.
	ReplayStillParked ReplayOutcome = "still_parked"
	// ReplayHandedToRetry moves the row parked → retrying with Message.Hazard,
	// Message.CorrelationID and Message.Reason, attempts + 1, due in 5 s.
	ReplayHandedToRetry ReplayOutcome = "handed_to_retry"
	// ReplayNotReplayable leaves a truncated row unchanged.
	ReplayNotReplayable ReplayOutcome = "not_replayable"
)

// ReplayStep is one row's result in a replay job.
type ReplayStep struct {
	Message model.UnprocessedMessage
	Outcome ReplayOutcome
}

// ReplayQueue is the replay runner's view of replay_requests and parked unprocessed messages.
// Every write is fenced by the leader epoch.
type ReplayQueue interface {
	// NextJob returns the running job if there is one, otherwise the oldest queued job.
	NextJob(ctx context.Context) (job model.ReplayJob, found bool, err error)
	// StartJob moves a queued job to running and sets started_at.
	StartJob(ctx context.Context, jobID string) error
	// ParkedPage returns up to limit parked messages of source received at or before cutoff and
	// after the position, in (received_at, message_id) order.
	ParkedPage(ctx context.Context, source model.Source, cutoff time.Time, after model.ReplayPosition, limit int) ([]model.UnprocessedMessage, error)
	// RecordStep applies a step to its row, adds it to the job's counters, and moves the job's
	// position to the row, in one transaction. created is true only for a recovered step that stored
	// a new hazard; a hazard that already existed (stored live by a later poll) leaves it false.
	RecordStep(ctx context.Context, jobID string, step ReplayStep) (created bool, err error)
	// FinishJob ends a job as done or failed (errText says why) and sets finished_at.
	FinishJob(ctx context.Context, jobID string, status model.ReplayStatus, errText string) error
}

// ReplayJobs is the replay API's view of replay_requests.
type ReplayJobs interface {
	// RequestReplay creates a queued job for source, or returns the source's active (queued or
	// running) job, so a second request while one is active gets the same job.
	RequestReplay(ctx context.Context, source model.Source, requestedBy, adaptations string) (model.ReplayJob, error)
	// ReplayJob returns a job by id. found is false for an unknown id.
	ReplayJob(ctx context.Context, jobID string) (job model.ReplayJob, found bool, err error)
}

// FieldReportResult is what saving a field report found.
type FieldReportResult string

const (
	// FieldReportInserted: the report is stored now.
	FieldReportInserted FieldReportResult = "inserted"
	// FieldReportDuplicate: the same report was already stored (same report_id, same body_sha256).
	FieldReportDuplicate FieldReportResult = "duplicate"
	// FieldReportConflict: another body is stored under the same report_id. It is kept.
	FieldReportConflict FieldReportResult = "conflict"
)

// FieldReportWriter stores field reports. Writes are not fenced: every aggregator pod consumes.
type FieldReportWriter interface {
	// SaveFieldReport inserts r unless its report_id is already stored, and compares the stored
	// body_sha256 when it is.
	SaveFieldReport(ctx context.Context, r model.FieldReport) (FieldReportResult, error)
}
