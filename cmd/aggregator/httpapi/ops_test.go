package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

// fakeAudit serves one page of unprocessed messages and remembers the filter it was asked for.
type fakeAudit struct {
	repository.AuditReader
	page repository.Page[repository.UnprocessedMessage]
	got  *repository.AuditFilter
}

func (f *fakeAudit) Unprocessed(_ context.Context, filter repository.AuditFilter) (repository.Page[repository.UnprocessedMessage], error) {
	f.got = &filter
	return f.page, nil
}

func opsApp(jobs *fakeReplayJobs) func(req *http.Request) (int, map[string]any) {
	return opsAppWithAudit(jobs, &fakeAudit{})
}

func opsAppWithAudit(jobs *fakeReplayJobs, audit *fakeAudit) func(req *http.Request) (int, map[string]any) {
	app := New(Deps{
		Audit: audit, Replay: jobs, ReplayRules: map[model.Source]string{model.SourcePVMBG: podRules},
		OpsToken: opsToken, AuditToken: auditToken, InternalKey: "internal",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return func(req *http.Request) (int, map[string]any) {
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

func replayRequest(requestedBy string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/internal/unprocessed/replay?source=PVMBG", nil)
	req.Header.Set("Authorization", "Bearer "+opsToken)
	if requestedBy != "" {
		req.Header.Set("X-Requested-By", requestedBy)
	}
	return req
}

// client-api forwards the JWT subject; the job records it as the requester.
func TestReplayRecordsTheForwardedRequester(t *testing.T) {
	jobs := &fakeReplayJobs{}
	status, body := opsApp(jobs)(replayRequest("  bnpb-ops "))
	if status != http.StatusAccepted {
		t.Fatalf("got %d %v, want 202", status, body)
	}
	if got := jobs.jobs[0].RequestedBy; got != "bnpb-ops" {
		t.Errorf("requested_by = %q, want bnpb-ops", got)
	}
}

// The requester is a label, not data: a long header is cut to 128 characters.
func TestReplayCapsTheRequester(t *testing.T) {
	jobs := &fakeReplayJobs{}
	long := strings.Repeat("é", 200)
	if status, body := opsApp(jobs)(replayRequest(long)); status != http.StatusAccepted {
		t.Fatalf("got %d %v, want 202", status, body)
	}
	if got := jobs.jobs[0].RequestedBy; got != strings.Repeat("é", 128) {
		t.Errorf("requested_by has %d characters, want the first 128", utf8.RuneCountInString(got))
	}
}

func unprocessedRequest(target, token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

// Ops lists failed messages on its own route: same filter and body as the audit list, but with OPS_TOKEN.
func TestOpsListsUnprocessedMessages(t *testing.T) {
	next := "next-page"
	audit := &fakeAudit{page: repository.Page[repository.UnprocessedMessage]{
		Items: []repository.UnprocessedMessage{{
			MessageID: "m-1", Source: "PVMBG", ReceivedAt: jobCreatedAt, Reason: "schema_incompatible: alert_level missing",
			Retryable: false, Attempts: 1, Status: "parked",
		}},
		NextCursor: &next,
	}}
	status, body := opsAppWithAudit(&fakeReplayJobs{}, audit)(unprocessedRequest("/internal/unprocessed?source=PVMBG&limit=5", opsToken))
	if status != http.StatusOK {
		t.Fatalf("got %d %v, want 200", status, body)
	}
	if audit.got == nil || audit.got.Source == nil || *audit.got.Source != model.SourcePVMBG || audit.got.Limit != 5 {
		t.Errorf("filter %+v, want source PVMBG and limit 5", audit.got)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 || body["next_cursor"] != next {
		t.Fatalf("body %v, want one item and next_cursor", body)
	}
	want := map[string]any{
		"message_id": "m-1", "source": "PVMBG", "received_at": "2026-11-18T03:00:00Z",
		"reason": "schema_incompatible: alert_level missing", "retryable": false, "attempts": 1.0, "status": "parked",
	}
	item, _ := items[0].(map[string]any)
	for k, v := range want {
		if item[k] != v {
			t.Errorf("%s = %v, want %v", k, item[k], v)
		}
	}
	if len(item) != len(want) {
		t.Errorf("item has %d fields, want %d (no payload): %v", len(item), len(want), item)
	}
}

// The assistant's audit token cannot read the ops list, and the ops token cannot read the audit list.
func TestOpsAndAuditListsKeepTheirTokens(t *testing.T) {
	call := opsApp(&fakeReplayJobs{})
	for _, tt := range []struct{ target, token string }{
		{"/internal/unprocessed", ""},
		{"/internal/unprocessed", auditToken},
		{"/internal/audit/unprocessed", opsToken},
	} {
		status, body := call(unprocessedRequest(tt.target, tt.token))
		if status != http.StatusUnauthorized || errorCode(body) != "unauthorized" {
			t.Errorf("%s with %q: got %d %v, want 401 unauthorized", tt.target, tt.token, status, body)
		}
	}
}
