package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	opsToken    = "ops-secret"
	auditToken  = "audit-secret"
	podRules    = "rename:alert_status=alert_level"
	existingJob = "0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a10"
)

var jobCreatedAt = time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)

// fakeReplayJobs keeps replay jobs in memory with the store's rule: one active job per source.
type fakeReplayJobs struct {
	jobs []model.ReplayJob
	err  error
}

func (f *fakeReplayJobs) RequestReplay(_ context.Context, source model.Source, requestedBy, adaptations string) (model.ReplayJob, error) {
	if f.err != nil {
		return model.ReplayJob{}, f.err
	}
	for _, j := range f.jobs {
		if j.Source == source && (j.Status == model.ReplayQueued || j.Status == model.ReplayRunning) {
			return j, nil
		}
	}
	j := model.ReplayJob{JobID: fmt.Sprintf("00000000-0000-4000-8000-%012d", len(f.jobs)+1), Source: source,
		Status: model.ReplayQueued, RequestedBy: requestedBy, Adaptations: adaptations, Cutoff: jobCreatedAt, CreatedAt: jobCreatedAt}
	f.jobs = append(f.jobs, j)
	return j, nil
}

func (f *fakeReplayJobs) ReplayJob(_ context.Context, id string) (model.ReplayJob, bool, error) {
	if f.err != nil {
		return model.ReplayJob{}, false, f.err
	}
	for _, j := range f.jobs {
		if j.JobID == id {
			return j, true, nil
		}
	}
	return model.ReplayJob{}, false, nil
}

func replayApp(jobs *fakeReplayJobs) func(method, target, token string) (int, map[string]any) {
	app := New(Deps{
		Replay: jobs, ReplayRules: map[model.Source]string{model.SourcePVMBG: podRules},
		OpsToken: opsToken, AuditToken: auditToken, InternalKey: "internal",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return func(method, target, token string) (int, map[string]any) {
		req := httptest.NewRequest(method, target, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := app.Test(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

// A replay request queues a job that records who asked and the rules of the pod that accepted it.
func TestReplayRequestQueuesAJob(t *testing.T) {
	jobs := &fakeReplayJobs{}
	call := replayApp(jobs)

	status, body := call(http.MethodPost, "/internal/unprocessed/replay?source=PVMBG", opsToken)
	if status != http.StatusAccepted || body["status"] != "queued" || body["job_id"] != "00000000-0000-4000-8000-000000000001" {
		t.Fatalf("got %d %v, want 202 with the new queued job", status, body)
	}
	if len(jobs.jobs) != 1 || jobs.jobs[0].RequestedBy != "ops-token" || jobs.jobs[0].Adaptations != podRules {
		t.Errorf("stored %+v, want requested_by ops-token and the pod's rules", jobs.jobs)
	}
}

// Only the ops token can call the replay API: the assistant's audit token and no token get 401.
func TestReplayNeedsTheOpsToken(t *testing.T) {
	jobs := &fakeReplayJobs{jobs: []model.ReplayJob{{JobID: existingJob, Source: model.SourcePVMBG, Status: model.ReplayDone}}}
	call := replayApp(jobs)
	for _, tt := range []struct{ method, target string }{
		{http.MethodPost, "/internal/unprocessed/replay?source=PVMBG"},
		{http.MethodGet, "/internal/unprocessed/replay/" + existingJob},
	} {
		for _, token := range []string{"", auditToken, "wrong"} {
			status, body := call(tt.method, tt.target, token)
			if status != http.StatusUnauthorized || errorCode(body) != "unauthorized" {
				t.Errorf("%s %s with token %q: got %d %v, want 401 unauthorized", tt.method, tt.target, token, status, body)
			}
		}
	}
	if len(jobs.jobs) != 1 {
		t.Errorf("a rejected request created a job")
	}
}

func TestReplayRequestNeedsAReplayableSource(t *testing.T) {
	for _, tt := range []struct{ query, message string }{
		{"", "source wajib diisi"},
		{"?source=GEMPA", "source harus BMKG, PVMBG, SATELIT atau SENSOR_PANTAI"},
		{"?source=BMKG", "source tidak mendukung replay"},
		{"?source=SENSOR_PANTAI", "source tidak mendukung replay"},
	} {
		jobs := &fakeReplayJobs{}
		status, body := replayApp(jobs)(http.MethodPost, "/internal/unprocessed/replay"+tt.query, opsToken)
		e, _ := body["error"].(map[string]any)
		if status != http.StatusBadRequest || errorCode(body) != "bad_request" || e["message"] != tt.message {
			t.Errorf("%q: got %d %v, want 400 %q", tt.query, status, body, tt.message)
		}
		if len(jobs.jobs) != 0 {
			t.Errorf("%q: created a job", tt.query)
		}
	}
}

// While a job for the source is queued or running, another request gets that job, not a new one.
func TestReplayRequestReturnsTheActiveJob(t *testing.T) {
	jobs := &fakeReplayJobs{jobs: []model.ReplayJob{{JobID: existingJob, Source: model.SourcePVMBG, Status: model.ReplayRunning}}}
	status, body := replayApp(jobs)(http.MethodPost, "/internal/unprocessed/replay?source=PVMBG", opsToken)
	if status != http.StatusAccepted || body["job_id"] != existingJob || body["status"] != "running" {
		t.Errorf("got %d %v, want 202 with the running job", status, body)
	}
	if len(jobs.jobs) != 1 {
		t.Errorf("%d jobs, want the existing one only", len(jobs.jobs))
	}
}

func TestReplayJobShowsItsProgress(t *testing.T) {
	started := jobCreatedAt.Add(2 * time.Second)
	jobs := &fakeReplayJobs{jobs: []model.ReplayJob{{
		JobID: existingJob, Source: model.SourcePVMBG, Status: model.ReplayRunning, RequestedBy: "ops-token",
		Adaptations: podRules, Cutoff: jobCreatedAt, CreatedAt: jobCreatedAt, StartedAt: &started,
		Selected: 7, Recovered: 4, StillParked: 1, HandedToRetry: 1, NotReplayable: 1,
	}}}
	status, body := replayApp(jobs)(http.MethodGet, "/internal/unprocessed/replay/"+existingJob, opsToken)
	if status != http.StatusOK {
		t.Fatalf("got %d %v, want 200", status, body)
	}
	want := map[string]any{
		"job_id": existingJob, "source": "PVMBG", "status": "running", "requested_by": "ops-token",
		"adaptations": podRules, "cutoff": "2026-11-18T03:00:00Z", "created_at": "2026-11-18T03:00:00Z",
		"started_at": "2026-11-18T03:00:02Z", "finished_at": nil, "error": nil,
		"selected": 7.0, "recovered": 4.0, "still_parked": 1.0, "handed_to_retry": 1.0, "not_replayable": 1.0,
	}
	for k, v := range want {
		if got, ok := body[k]; !ok || got != v {
			t.Errorf("%s = %v, want %v", k, got, v)
		}
	}
	if len(body) != len(want) {
		t.Errorf("body has %d fields, want %d: %v", len(body), len(want), body)
	}
}

func TestUnknownReplayJobIsNotFound(t *testing.T) {
	jobs := &fakeReplayJobs{jobs: []model.ReplayJob{{JobID: existingJob, Source: model.SourcePVMBG, Status: model.ReplayDone}}}
	for _, id := range []string{"0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a11", "not-a-uuid", "0b7c9a52-6f0e-4c3e-9d0a-2f4b8e1c7a1z"} {
		status, body := replayApp(jobs)(http.MethodGet, "/internal/unprocessed/replay/"+id, opsToken)
		if status != http.StatusNotFound || errorCode(body) != "not_found" {
			t.Errorf("%s: got %d %v, want 404 not_found", id, status, body)
		}
	}
}

func TestReplayAPIWhenTheStoreIsDown(t *testing.T) {
	jobs := &fakeReplayJobs{err: fmt.Errorf("request replay: %w: connection refused", repository.ErrUnavailable)}
	call := replayApp(jobs)
	for _, tt := range []struct{ method, target string }{
		{http.MethodPost, "/internal/unprocessed/replay?source=PVMBG"},
		{http.MethodGet, "/internal/unprocessed/replay/" + existingJob},
	} {
		status, body := call(tt.method, tt.target, opsToken)
		if status != http.StatusServiceUnavailable || errorCode(body) != "unavailable" {
			t.Errorf("%s: got %d %v, want 503 unavailable", tt.method, status, body)
		}
	}
}
