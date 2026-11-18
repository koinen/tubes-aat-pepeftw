package model

import "time"

// ReplayStatus is a replay job's state: queued → running → done | failed.
type ReplayStatus string

const (
	ReplayQueued  ReplayStatus = "queued"
	ReplayRunning ReplayStatus = "running"
	ReplayDone    ReplayStatus = "done"
	// ReplayFailed is only for our own bugs, never for a message that fails again.
	ReplayFailed ReplayStatus = "failed"
)

// ReplayPosition is the last row a replay job finished, in (received_at, message_id) order.
// The zero value is the start.
type ReplayPosition struct {
	ReceivedAt time.Time
	MessageID  string
}

// Before reports whether p comes before m in replay order, so m is still to be replayed.
func (p ReplayPosition) Before(m UnprocessedMessage) bool {
	if p.MessageID == "" {
		return true
	}
	if !p.ReceivedAt.Equal(m.ReceivedAt) {
		return p.ReceivedAt.Before(m.ReceivedAt)
	}
	return p.MessageID < m.MessageID
}

// ReplayJob is one replay of a source's parked messages (a row of replay_requests).
type ReplayJob struct {
	JobID       string
	Source      Source
	Status      ReplayStatus
	RequestedBy string
	// Adaptations is the canonical rule string of the pod that accepted the request.
	Adaptations string
	// Cutoff is the job's created_at: only rows received until then are selected.
	Cutoff   time.Time
	Position ReplayPosition

	Selected      int64
	Recovered     int64
	StillParked   int64
	HandedToRetry int64
	NotReplayable int64

	Error      string
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
}
