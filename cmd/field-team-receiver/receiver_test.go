package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const criticalAlert = `{"hazard_id":"hz-1","event_id":"ev-1","source":"SENSOR_PANTAI","severity":"AWAS",` +
	`"attributes":{"fetched_at":"2026-11-18T03:00:00.500Z","critical":true}}`

func TestAlertRequiresAnAcknowledgedDeliveryReceipt(t *testing.T) {
	receivedAt := time.Date(2026, 11, 18, 3, 0, 2, 0, time.UTC)
	var response *httptest.ResponseRecorder
	var records []*kgo.Record
	handler := newReceiver("field-team-receipts", func(_ context.Context, r *kgo.Record) error {
		if response.Flushed || response.Body.Len() != 0 || response.Code != http.StatusOK {
			t.Error("response written before receipt was acknowledged")
		}
		records = append(records, r)
		if len(records) == 1 {
			return errors.New("Kafka unavailable")
		}
		return nil
	}, func() time.Time { return receivedAt })

	for _, want := range []int{http.StatusServiceUnavailable, http.StatusNoContent} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/alerts", strings.NewReader(criticalAlert)))
		if response.Code != want {
			t.Fatalf("POST status = %d, want %d", response.Code, want)
		}
		list := httptest.NewRecorder()
		handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/alerts", nil))
		var recent []struct {
			Alert struct {
				HazardID string `json:"hazard_id"`
			} `json:"alert"`
		}
		if err := json.Unmarshal(list.Body.Bytes(), &recent); err != nil {
			t.Fatal(err)
		}
		if want == http.StatusServiceUnavailable && len(recent) != 0 {
			t.Errorf("failed delivery appeared in the demo list: %+v", recent)
		}
		if want == http.StatusNoContent && (len(recent) != 1 || recent[0].Alert.HazardID != "hz-1") {
			t.Errorf("acknowledged alert missing from the demo list: %+v", recent)
		}
	}
	if len(records) != 2 {
		t.Fatalf("receipt attempts = %d, want 2", len(records))
	}
	record := records[1]
	if record.Topic != "field-team-receipts" || string(record.Key) != "hz-1" {
		t.Fatalf("receipt topic=%q key=%q", record.Topic, record.Key)
	}
	var body map[string]any
	if err := json.Unmarshal(record.Value, &body); err != nil {
		t.Fatal(err)
	}
	if body["hazard_id"] != "hz-1" || body["received_at"] != "2026-11-18T03:00:02Z" ||
		body["fetched_at"] != "2026-11-18T03:00:00.5Z" || body["latency_ms"] != float64(1500) {
		t.Errorf("unexpected receipt: %s", record.Value)
	}
}

func TestReceiverCountsReplicaLocalDuplicatesWithoutRepeatingLatency(t *testing.T) {
	var receipts int
	handler := newReceiver("field-team-receipts", func(context.Context, *kgo.Record) error {
		receipts++
		return nil
	}, func() time.Time { return time.Date(2026, 11, 18, 3, 0, 2, 0, time.UTC) })

	for range 2 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/alerts", strings.NewReader(criticalAlert)))
		if response.Code != http.StatusNoContent {
			t.Fatalf("POST status = %d", response.Code)
		}
	}
	if receipts != 2 {
		t.Errorf("receipts = %d, want one for each acknowledgment", receipts)
	}
	response := httptest.NewRecorder()
	handler.metricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`bnpb_field_alert_duplicates_total 1`,
		`bnpb_field_alert_latency_seconds_count{source="SENSOR_PANTAI"} 1`,
		`bnpb_field_alert_latency_seconds_sum{source="SENSOR_PANTAI"} 1.5`,
	} {
		if !strings.Contains(response.Body.String(), want+"\n") {
			t.Errorf("missing metric %q", want)
		}
	}
}

func TestAlertWithoutFetchTimeStillGetsAReceipt(t *testing.T) {
	var value []byte
	handler := newReceiver("field-team-receipts", func(_ context.Context, r *kgo.Record) error {
		value = r.Value
		return nil
	}, func() time.Time { return time.Date(2026, 11, 18, 3, 0, 2, 0, time.UTC) })

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/alerts", strings.NewReader(
		`{"hazard_id":"hz-2","source":"PVMBG","severity":"SIAGA","emitted_at":"2026-11-18T03:00:00Z"}`)))
	if response.Code != http.StatusNoContent {
		t.Fatalf("POST status = %d, want %d", response.Code, http.StatusNoContent)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(value, &body); err != nil {
		t.Fatal(err)
	}
	if body["fetched_at"] != nil || body["latency_ms"] != nil || string(body["hazard_id"]) != `"hz-2"` {
		t.Errorf("unexpected receipt: %s", value)
	}
	response = httptest.NewRecorder()
	handler.metricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(response.Body.String(), `source="PVMBG"`) {
		t.Error("PVMBG alert without fetched_at contributed a latency sample")
	}
}
