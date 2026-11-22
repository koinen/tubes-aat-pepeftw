package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

type sourceStatusRows []model.SourceStatus

func (s sourceStatusRows) ListSourceStatus(context.Context) ([]model.SourceStatus, error) {
	return s, nil
}

func (s sourceStatusRows) SaveSourceStatus(context.Context, model.SourceStatus) error { return nil }

func TestSensorStatusBeforeFirstPollAndWhenDisabled(t *testing.T) {
	read := func(enabled bool, statuses sourceStatusRows) map[model.Source]model.SourceStatus {
		t.Helper()
		app := New(Deps{Statuses: statuses, SensorPantaiEnabled: enabled, InternalKey: "test",
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
		req := httptest.NewRequest(http.MethodGet, "/internal/sources/status", nil)
		req.Header.Set("X-Internal-Key", "test")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body struct {
			Data map[model.Source]model.SourceStatus
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("status response: %s, %v", resp.Status, err)
		}
		return body.Data
	}
	if st := read(true, nil)[model.SourceSensorPantai]; st.Status != model.StatusUnavailable || st.LastSuccessAt != nil {
		t.Fatalf("configured sensor without a snapshot: %+v", st)
	}
	old := sourceStatusRows{{Source: model.SourceSensorPantai, Status: model.StatusOK}}
	if _, ok := read(false, old)[model.SourceSensorPantai]; ok {
		t.Fatal("disabled sensor should be omitted even if there is a previous snapshot")
	}
}
