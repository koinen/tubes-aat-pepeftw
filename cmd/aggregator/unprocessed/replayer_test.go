package unprocessed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"tubes-aat-pepeftw/cmd/aggregator/fence"
	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	jobID       = "11111111-2222-3333-4444-555555555555"
	renameRule  = "rename:alert_status=alert_level"
	renamedV3   = `{"report_id":"r3","volcano_id":"MERAPI","alert_status":"Siaga","eruption_count_24h":12,"ash_column_height_m":1500,"reported_at":"2026-11-18T00:05:00Z"}`
	unknownPeak = `{"report_id":"r9","volcano_id":"ANAK_KRAKATAU","alert_level":"Waspada","eruption_count_24h":1,"ash_column_height_m":300,"reported_at":"2026-11-18T00:06:00Z"}`
)

var (
	replayTime = time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	jobCreated = time.Date(2026, 11, 18, 0, 30, 0, 0, time.UTC)
	merapi     = model.VolcanoRef{VolcanoID: "MERAPI", Name: "Merapi", Latitude: -7.54, Longitude: 110.44}
)

// fakeReplayQueue keeps unprocessed rows and replay jobs in memory with the selection rules of the
// real store: parked rows of the job's source, received_at <= cutoff, after the job's position,
// in (received_at, message_id) order.
type fakeReplayQueue struct {
	rows  []model.UnprocessedMessage
	jobs  []model.ReplayJob
	steps []repository.ReplayStep
	pages []int // size of each page read
	refs  []model.VolcanoRef
	// hazards holds the source_ref_id of every stored hazard; a recovered step stores its hazard.
	hazards map[string]bool

	stepErr func(repository.ReplayStep) error
	pageErr error
	refsErr error
}

func (f *fakeReplayQueue) NextJob(context.Context) (model.ReplayJob, bool, error) {
	for _, status := range []model.ReplayStatus{model.ReplayRunning, model.ReplayQueued} {
		for _, j := range f.jobs {
			if j.Status == status {
				return j, true, nil
			}
		}
	}
	return model.ReplayJob{}, false, nil
}

func (f *fakeReplayQueue) job(id string) *model.ReplayJob {
	for i := range f.jobs {
		if f.jobs[i].JobID == id {
			return &f.jobs[i]
		}
	}
	panic("no job " + id)
}

func (f *fakeReplayQueue) StartJob(_ context.Context, id string) error {
	f.job(id).Status = model.ReplayRunning
	return nil
}

func (f *fakeReplayQueue) FinishJob(_ context.Context, id string, status model.ReplayStatus, errText string) error {
	j := f.job(id)
	j.Status, j.Error = status, errText
	return nil
}

func (f *fakeReplayQueue) ParkedPage(_ context.Context, source model.Source, cutoff time.Time,
	after model.ReplayPosition, limit int) ([]model.UnprocessedMessage, error) {
	if f.pageErr != nil {
		return nil, f.pageErr
	}
	var out []model.UnprocessedMessage
	for _, m := range f.rows {
		if m.Source == source && m.Status == model.StatusParked && !m.ReceivedAt.After(cutoff) && after.Before(m) {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return model.ReplayPosition{ReceivedAt: out[i].ReceivedAt, MessageID: out[i].MessageID}.Before(out[j])
	})
	if len(out) > limit {
		out = out[:limit]
	}
	f.pages = append(f.pages, len(out))
	return out, nil
}

func (f *fakeReplayQueue) RecordStep(_ context.Context, id string, s repository.ReplayStep) (bool, error) {
	if f.stepErr != nil {
		if err := f.stepErr(s); err != nil {
			return false, err
		}
	}
	created := false
	f.steps = append(f.steps, s)
	row := f.row(s.Message.MessageID)
	j := f.job(id)
	j.Selected++
	switch s.Outcome {
	case repository.ReplayRecovered:
		row.Status = model.StatusRecovered
		j.Recovered++
		if f.hazards == nil {
			f.hazards = map[string]bool{}
		}
		created = !f.hazards[s.Message.Hazard.SourceRefID]
		f.hazards[s.Message.Hazard.SourceRefID] = true
	case repository.ReplayStillParked:
		row.Attempts++
		row.Code, row.Reason = s.Message.Code, s.Message.Reason
		j.StillParked++
	case repository.ReplayHandedToRetry:
		row.Status, row.Hazard, row.CorrelationID = model.StatusRetrying, s.Message.Hazard, s.Message.CorrelationID
		row.Attempts++
		row.Code, row.Reason = s.Message.Code, s.Message.Reason
		j.HandedToRetry++
	case repository.ReplayNotReplayable:
		j.NotReplayable++
	}
	j.Position = model.ReplayPosition{ReceivedAt: s.Message.ReceivedAt, MessageID: s.Message.MessageID}
	return created, nil
}

func (f *fakeReplayQueue) row(id string) *model.UnprocessedMessage {
	for i := range f.rows {
		if f.rows[i].MessageID == id {
			return &f.rows[i]
		}
	}
	panic("no row " + id)
}

func (f *fakeReplayQueue) ListVolcanoRefs(context.Context) ([]model.VolcanoRef, error) {
	if f.refsErr != nil {
		return nil, f.refsErr
	}
	return f.refs, nil
}

func parked(id, payload string, receivedAt time.Time) model.UnprocessedMessage {
	return model.UnprocessedMessage{
		MessageID: id, Source: model.SourcePVMBG, Endpoint: mapper.EndpointVolcanicReports,
		ReceivedAt: receivedAt, Code: model.ReasonSchemaIncompatible, Reason: "schema_incompatible: old",
		Status: model.StatusParked, Attempts: 1, Payload: []byte(payload), PayloadSize: int64(len(payload)),
	}
}

func queuedJob(adaptations string) model.ReplayJob {
	return model.ReplayJob{JobID: jobID, Source: model.SourcePVMBG, Status: model.ReplayQueued,
		Adaptations: adaptations, Cutoff: jobCreated, CreatedAt: jobCreated}
}

func newTestReplayer(t *testing.T, q *fakeReplayQueue, rules string, logs io.Writer) *Replayer {
	t.Helper()
	a, err := mapper.ParseVolcanicAdaptations(rules)
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	return NewReplayer(q, map[model.Source]Remapper{model.SourcePVMBG: PVMBGRemapper(q, a)}, log,
		func() time.Time { return replayTime })
}

// After the operator declares the rule, a row parked by the v3 rename becomes a hazard whose
// outbox event carries the job id, and the job ends done.
func TestReplayRecoversARowTheDeclaredRuleNowMaps(t *testing.T) {
	q := &fakeReplayQueue{
		rows: []model.UnprocessedMessage{parked("m1", renamedV3, jobCreated.Add(-time.Hour))},
		jobs: []model.ReplayJob{queuedJob(renameRule)},
		refs: []model.VolcanoRef{merapi},
	}
	var logs bytes.Buffer
	if err := newTestReplayer(t, q, renameRule, &logs).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(q.steps) != 1 || q.steps[0].Outcome != repository.ReplayRecovered {
		t.Fatalf("steps %+v, want one recovered", q.steps)
	}
	m := q.steps[0].Message
	if m.Hazard == nil || m.Hazard.SourceRefID != "r3" || m.Hazard.Severity != model.SeveritySiaga {
		t.Errorf("hazard %+v, want r3 SIAGA", m.Hazard)
	}
	if m.CorrelationID != jobID {
		t.Errorf("correlation_id %q, want the job id", m.CorrelationID)
	}
	if !m.Hazard.IngestedAt.Equal(replayTime) {
		t.Errorf("ingested_at %v, want the replay time %v", m.Hazard.IngestedAt, replayTime)
	}
	j := q.jobs[0]
	if j.Status != model.ReplayDone || j.Selected != 1 || j.Recovered != 1 {
		t.Errorf("job %s selected %d recovered %d, want done 1 1", j.Status, j.Selected, j.Recovered)
	}
	want := map[string]any{"msg": "adaptation_applied", "source": "PVMBG", "source_ref_id": "r3",
		"rule": renameRule, "correlation_id": jobID}
	if countLogLines(t, logs.String(), want) != 1 {
		t.Errorf("want one log line with %v in\n%s", want, logs.String())
	}
}

// A report parked as v3 can also be stored live by a later poll that already had the rule (the
// overlap fetches it again); that poll counted its adaptation. Replaying the parked row recovers the
// message without a new hazard, so the adaptation is not counted a second time.
func TestReplayOfAReportWhoseHazardExistsCountsTheRecoveryButNoAdaptation(t *testing.T) {
	q := &fakeReplayQueue{
		rows:    []model.UnprocessedMessage{parked("m1", renamedV3, jobCreated.Add(-time.Hour))},
		jobs:    []model.ReplayJob{queuedJob(renameRule)},
		refs:    []model.VolcanoRef{merapi},
		hazards: map[string]bool{"r3": true},
	}
	recoveredBefore := counterValue(t, recovered.WithLabelValues("PVMBG"))
	adaptedBefore := counterValue(t, adaptationApplied.WithLabelValues("PVMBG", renameRule))
	var logs bytes.Buffer
	if err := newTestReplayer(t, q, renameRule, &logs).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	j := q.jobs[0]
	if j.Status != model.ReplayDone || j.Recovered != 1 || q.row("m1").Status != model.StatusRecovered {
		t.Errorf("job %s recovered %d, row %s; want done 1 recovered", j.Status, j.Recovered, q.row("m1").Status)
	}
	if got := counterValue(t, recovered.WithLabelValues("PVMBG")) - recoveredBefore; got != 1 {
		t.Errorf("bnpb_unprocessed_recovered_total +%v, want +1", got)
	}
	if got := counterValue(t, adaptationApplied.WithLabelValues("PVMBG", renameRule)) - adaptedBefore; got != 0 {
		t.Errorf("bnpb_adaptation_applied_total +%v, want +0", got)
	}
	if n := countLogLines(t, logs.String(), map[string]any{"msg": "adaptation_applied"}); n != 0 {
		t.Errorf("%d adaptation_applied log lines, want 0 in\n%s", n, logs.String())
	}
	want := map[string]any{"msg": "message_recovered", "message_id": "m1", "hazard_created": false}
	if countLogLines(t, logs.String(), want) != 1 {
		t.Errorf("want one log line with %v in\n%s", want, logs.String())
	}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

func countLogLines(t *testing.T, logs string, want map[string]any) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
			}
		}
		if match {
			n++
		}
	}
	return n
}

// A job reads parked rows in pages and replays each selected row once, in (received_at, message_id)
// order, including rows that share a received_at across a page boundary. Rows received after the
// cutoff, of another source, or not parked are left alone.
func TestReplayWalksEveryParkedRowOnceInOrder(t *testing.T) {
	q := &fakeReplayQueue{jobs: []model.ReplayJob{queuedJob(renameRule)}, refs: []model.VolcanoRef{merapi}}
	// 230 selected rows, stored out of order; rows 90..119 share one received_at, so the tie spans
	// the boundary between the first and second page.
	for i := 229; i >= 0; i-- {
		at := jobCreated.Add(-time.Duration(230-i) * time.Second)
		if i >= 90 && i < 120 {
			at = jobCreated.Add(-200 * time.Second)
		}
		q.rows = append(q.rows, parked(fmt.Sprintf("m%03d", i), renamedV3, at))
	}
	afterCutoff := parked("late", renamedV3, jobCreated.Add(time.Second))
	other := parked("bmkg", renamedV3, jobCreated.Add(-time.Hour))
	other.Source = model.SourceBMKG
	retrying := parked("retrying", renamedV3, jobCreated.Add(-time.Hour))
	retrying.Status = model.StatusRetrying
	recovered := parked("recovered", renamedV3, jobCreated.Add(-time.Hour))
	recovered.Status = model.StatusRecovered
	q.rows = append(q.rows, afterCutoff, other, retrying, recovered)

	if err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(q.steps) != 230 {
		t.Fatalf("%d steps, want 230", len(q.steps))
	}
	seen := map[string]bool{}
	for i, s := range q.steps {
		id := s.Message.MessageID
		if seen[id] || strings.HasPrefix(id, "late") || !strings.HasPrefix(id, "m") {
			t.Fatalf("step %d replayed %q (twice or outside the selection)", i, id)
		}
		seen[id] = true
		if i > 0 {
			prev := q.steps[i-1].Message
			if !(model.ReplayPosition{ReceivedAt: prev.ReceivedAt, MessageID: prev.MessageID}).Before(s.Message) {
				t.Fatalf("step %d (%s) is not after step %d (%s)", i, id, i-1, prev.MessageID)
			}
		}
	}
	if got := fmt.Sprint(q.pages); got != "[100 100 30]" {
		t.Errorf("pages %s, want [100 100 30]", got)
	}
	if j := q.jobs[0]; j.Status != model.ReplayDone || j.Selected != 230 || j.Recovered != 230 {
		t.Errorf("job %s selected %d recovered %d, want done 230 230", j.Status, j.Selected, j.Recovered)
	}
	for _, id := range []string{"late", "bmkg", "retrying", "recovered"} {
		if r := q.row(id); r.Attempts != 1 || (id == "late" && r.Status != model.StatusParked) {
			t.Errorf("row %s was touched: %+v", id, r.Status)
		}
	}
}

// A row stored truncated cannot be mapped again: it is counted not_replayable and left as it is.
func TestTruncatedRowIsNotReplayable(t *testing.T) {
	cut := parked("big", renamedV3, jobCreated.Add(-time.Hour))
	cut.Payload, cut.PayloadSize = cut.Payload[:40], 2<<20
	q := &fakeReplayQueue{
		rows: []model.UnprocessedMessage{cut},
		jobs: []model.ReplayJob{queuedJob(renameRule)},
		refs: []model.VolcanoRef{merapi},
	}
	if err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(q.steps) != 1 || q.steps[0].Outcome != repository.ReplayNotReplayable {
		t.Fatalf("steps %+v, want one not_replayable", q.steps)
	}
	if j := q.jobs[0]; j.Status != model.ReplayDone || j.Selected != 1 || j.NotReplayable != 1 {
		t.Errorf("job %s selected %d not_replayable %d, want done 1 1", j.Status, j.Selected, j.NotReplayable)
	}
	if r := q.row("big"); r.Status != model.StatusParked || r.Attempts != 1 || r.Reason != "schema_incompatible: old" {
		t.Errorf("truncated row changed: %s attempts %d reason %q", r.Status, r.Attempts, r.Reason)
	}
}

// A row that fails again stays parked with the latest failure as its reason, and the job goes on.
func TestRowThatStillFailsStaysParkedWithTheNewReason(t *testing.T) {
	q := &fakeReplayQueue{
		rows: []model.UnprocessedMessage{
			parked("m1", unknownPeak, jobCreated.Add(-2*time.Hour)),
			parked("m2", renamedV3, jobCreated.Add(-time.Hour)),
		},
		jobs: []model.ReplayJob{queuedJob(renameRule)},
		refs: []model.VolcanoRef{merapi},
	}
	if err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(q.steps) != 2 || q.steps[0].Outcome != repository.ReplayStillParked || q.steps[1].Outcome != repository.ReplayRecovered {
		t.Fatalf("steps %+v, want still_parked then recovered", q.steps)
	}
	r := q.row("m1")
	if r.Status != model.StatusParked || r.Attempts != 2 || r.Code != model.ReasonSchemaIncompatible ||
		!strings.Contains(r.Reason, "ANAK_KRAKATAU") || r.Reason == "schema_incompatible: old" {
		t.Errorf("row m1: %s attempts %d code %s reason %q", r.Status, r.Attempts, r.Code, r.Reason)
	}
	if j := q.jobs[0]; j.Status != model.ReplayDone || j.Selected != 2 || j.StillParked != 1 || j.Recovered != 1 {
		t.Errorf("job %+v, want done, selected 2, still_parked 1, recovered 1", j)
	}
}

// When the store fails to save a replayed hazard: an outage hands the row to the retry worker with
// the hazard and the job id, and a value the store refuses keeps the row parked as store_rejected.
func TestStoreFailureOnRecover(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		outcome  repository.ReplayOutcome
		status   model.UnprocessedStatus
		code     model.ReasonCode
		handed   int64
		stillPkd int64
	}{
		{"unavailable", fmt.Errorf("recover m1: %w: range unavailable", repository.ErrUnavailable),
			repository.ReplayHandedToRetry, model.StatusRetrying, model.ReasonStoreUnavailable, 1, 0},
		{"rejected", fmt.Errorf("recover m1: %w: 23514 check violation", repository.ErrRejected),
			repository.ReplayStillParked, model.StatusParked, model.ReasonStoreRejected, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeReplayQueue{
				rows: []model.UnprocessedMessage{parked("m1", renamedV3, jobCreated.Add(-time.Hour))},
				jobs: []model.ReplayJob{queuedJob(renameRule)},
				refs: []model.VolcanoRef{merapi},
				stepErr: func(s repository.ReplayStep) error {
					if s.Outcome == repository.ReplayRecovered {
						return tt.err
					}
					return nil
				},
			}
			if err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if len(q.steps) != 1 || q.steps[0].Outcome != tt.outcome {
				t.Fatalf("steps %+v, want one %s", q.steps, tt.outcome)
			}
			r := q.row("m1")
			if r.Status != tt.status || r.Code != tt.code {
				t.Errorf("row %s %s, want %s %s", r.Status, r.Code, tt.status, tt.code)
			}
			if tt.status == model.StatusRetrying && (r.Hazard == nil || r.Hazard.SourceRefID != "r3" || r.CorrelationID != jobID) {
				t.Errorf("retrying row lacks what the worker needs: hazard %v correlation %q", r.Hazard, r.CorrelationID)
			}
			if !strings.HasPrefix(r.Reason, string(tt.code)+": recover m1: ") || r.Attempts != 2 {
				t.Errorf("reason %q attempts %d, want %s: … and 2", r.Reason, r.Attempts, tt.code)
			}
			j := q.jobs[0]
			if j.Status != model.ReplayDone || j.Selected != 1 || j.Recovered != 0 || j.HandedToRetry != tt.handed || j.StillParked != tt.stillPkd {
				t.Errorf("job %+v", j)
			}
		})
	}
}

// A leader that loses its epoch mid-job stops and leaves the job running. The next leader resumes
// after the last row the job finished, so no row is replayed twice and the counters add up, and it
// logs that it resumed and from where.
func TestNextLeaderResumesAJobWhereItStopped(t *testing.T) {
	q := &fakeReplayQueue{
		rows: []model.UnprocessedMessage{
			parked("m1", renamedV3, jobCreated.Add(-5*time.Minute)),
			parked("m2", unknownPeak, jobCreated.Add(-4*time.Minute)), // stays parked, so selectable again
			parked("m3", renamedV3, jobCreated.Add(-3*time.Minute)),
			parked("m4", unknownPeak, jobCreated.Add(-2*time.Minute)),
			parked("m5", renamedV3, jobCreated.Add(-1*time.Minute)),
		},
		jobs: []model.ReplayJob{queuedJob(renameRule)},
		refs: []model.VolcanoRef{merapi},
	}
	q.stepErr = func(s repository.ReplayStep) error {
		if s.Message.MessageID == "m3" {
			return fmt.Errorf("recover m3: %w: epoch 3", fence.ErrStaleEpoch)
		}
		return nil
	}
	err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background())
	if !errors.Is(err, fence.ErrStaleEpoch) {
		t.Fatalf("RunOnce error %v, want stale epoch", err)
	}
	if j := q.jobs[0]; j.Status != model.ReplayRunning || j.Position.MessageID != "m2" || j.Selected != 2 {
		t.Fatalf("after the fenced-out leader: job %s at %q selected %d, want running at m2 with 2", j.Status, j.Position.MessageID, j.Selected)
	}

	q.stepErr = nil
	var logs bytes.Buffer
	if err := newTestReplayer(t, q, renameRule, &logs).RunOnce(context.Background()); err != nil {
		t.Fatalf("next leader RunOnce: %v", err)
	}
	resumed := map[string]any{"msg": "replay_resumed", "job_id": jobID, "source": "PVMBG", "last_message_id": "m2"}
	if countLogLines(t, logs.String(), resumed) != 1 || countLogLines(t, logs.String(), map[string]any{"msg": "replay_started"}) != 0 {
		t.Errorf("want one line with %v and no replay_started in\n%s", resumed, logs.String())
	}
	var ids []string
	for _, s := range q.steps {
		ids = append(ids, s.Message.MessageID)
	}
	if got := strings.Join(ids, ","); got != "m1,m2,m3,m4,m5" {
		t.Errorf("steps %s, want m1,m2,m3,m4,m5", got)
	}
	j := q.jobs[0]
	if j.Status != model.ReplayDone || j.Selected != 5 || j.Recovered != 3 || j.StillParked != 2 {
		t.Errorf("job %s selected %d recovered %d still_parked %d, want done 5 3 2", j.Status, j.Selected, j.Recovered, j.StillParked)
	}
}

// During a rollout, a job accepted by a pod with the new rules waits on a leader that still has
// the old ones, whether it is queued or was already running under a previous leader. The leader
// says so once, not on every tick it waits.
func TestJobWaitsForTheRulesItWasAcceptedWith(t *testing.T) {
	for _, status := range []model.ReplayStatus{model.ReplayQueued, model.ReplayRunning} {
		t.Run(string(status), func(t *testing.T) {
			job := queuedJob(renameRule)
			job.Status = status
			q := &fakeReplayQueue{
				rows: []model.UnprocessedMessage{parked("m1", renamedV3, jobCreated.Add(-time.Hour))},
				jobs: []model.ReplayJob{job},
				refs: []model.VolcanoRef{merapi},
			}
			var logs bytes.Buffer
			r := newTestReplayer(t, q, "", &logs)
			for range 3 {
				if err := r.RunOnce(context.Background()); err != nil {
					t.Fatalf("RunOnce: %v", err)
				}
			}
			if len(q.steps) != 0 || q.jobs[0].Status != status || q.row("m1").Status != model.StatusParked {
				t.Errorf("job ran on a leader without its rules: %d steps, job %s", len(q.steps), q.jobs[0].Status)
			}
			want := map[string]any{"msg": "replay_waiting_for_config", "job_id": jobID}
			if countLogLines(t, logs.String(), want) != 1 {
				t.Errorf("want one log line with %v in\n%s", want, logs.String())
			}
		})
	}
}

// A job with nothing to replay ends done with every counter at 0.
func TestJobWithNoParkedRowsIsDone(t *testing.T) {
	q := &fakeReplayQueue{jobs: []model.ReplayJob{queuedJob("")}, refs: []model.VolcanoRef{merapi}}
	if err := newTestReplayer(t, q, "", io.Discard).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	j := q.jobs[0]
	if j.Status != model.ReplayDone || j.Selected+j.Recovered+j.StillParked+j.HandedToRetry+j.NotReplayable != 0 {
		t.Errorf("job %+v, want done with 0 counters", j)
	}
}

// Each job loads volcano_ref when it starts, so "add the ref with load-catalog, then replay"
// works without restarting the leader.
func TestEachJobUsesTheCurrentVolcanoRefs(t *testing.T) {
	q := &fakeReplayQueue{
		rows: []model.UnprocessedMessage{parked("m9", unknownPeak, jobCreated.Add(-time.Hour))},
		jobs: []model.ReplayJob{queuedJob("")},
		refs: []model.VolcanoRef{merapi},
	}
	rp := newTestReplayer(t, q, "", io.Discard)
	if err := rp.RunOnce(context.Background()); err != nil {
		t.Fatalf("first job: %v", err)
	}
	q.refs = append(q.refs, model.VolcanoRef{VolcanoID: "ANAK_KRAKATAU", Name: "Anak Krakatau", Latitude: -6.10, Longitude: 105.42})
	second := queuedJob("")
	second.JobID, second.Cutoff = "job-2", jobCreated.Add(time.Hour)
	q.jobs = append(q.jobs, second)
	if err := rp.RunOnce(context.Background()); err != nil {
		t.Fatalf("second job: %v", err)
	}
	if len(q.steps) != 2 || q.steps[0].Outcome != repository.ReplayStillParked || q.steps[1].Outcome != repository.ReplayRecovered {
		t.Fatalf("steps %+v, want still_parked then recovered", q.steps)
	}
}

// An outage leaves the job running for the next tick; an error the store does not tag (our own
// SQL bug) fails the job with the error, since running it again would fail the same way.
func TestJobErrors(t *testing.T) {
	unavailable := fmt.Errorf("parked page: %w: connection refused", repository.ErrUnavailable)
	bug := errors.New(`parked page: ERROR: column "recieved_at" does not exist (SQLSTATE 42703)`)
	tests := []struct {
		name    string
		pageErr error
		refsErr error
		status  model.ReplayStatus
		errText string
	}{
		{"page read unavailable", unavailable, nil, model.ReplayRunning, ""},
		{"refs load unavailable", nil, fmt.Errorf("list volcano_ref: %w", repository.ErrUnavailable), model.ReplayRunning, ""},
		{"our own SQL bug", bug, nil, model.ReplayFailed, bug.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeReplayQueue{
				rows:    []model.UnprocessedMessage{parked("m1", renamedV3, jobCreated.Add(-time.Hour))},
				jobs:    []model.ReplayJob{queuedJob(renameRule)},
				refs:    []model.VolcanoRef{merapi},
				pageErr: tt.pageErr, refsErr: tt.refsErr,
			}
			if err := newTestReplayer(t, q, renameRule, io.Discard).RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v; only lost leadership stops the runner", err)
			}
			if j := q.jobs[0]; j.Status != tt.status || j.Error != tt.errText {
				t.Errorf("job %s error %q, want %s %q", j.Status, j.Error, tt.status, tt.errText)
			}
			if r := q.row("m1"); r.Status != model.StatusParked || r.Attempts != 1 {
				t.Errorf("row changed: %s attempts %d", r.Status, r.Attempts)
			}
		})
	}
}

// The endpoint accepts only replayable sources, so a job for another source is our own bug: it
// fails instead of waiting forever.
func TestJobForASourceWithoutRemapperFails(t *testing.T) {
	job := queuedJob("")
	job.Source = model.SourceBMKG
	q := &fakeReplayQueue{jobs: []model.ReplayJob{job}}
	if err := newTestReplayer(t, q, "", io.Discard).RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if j := q.jobs[0]; j.Status != model.ReplayFailed || j.Error != "source BMKG tidak mendukung replay" {
		t.Errorf("job %s error %q", j.Status, j.Error)
	}
}
