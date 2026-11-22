package unprocessed

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

var now = time.Date(2026, 11, 18, 2, 40, 11, 0, time.UTC)

func TestMapperRejectionIsParked(t *testing.T) {
	raw := []byte(`{"report_id":"r4","eruption_count_24h":"12"}`)
	cause := &mapper.RejectError{Code: model.ReasonSchemaIncompatible, SourceRefID: "r4", Reason: "eruption_count_24h bukan angka"}

	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, cause, now)

	if m.Status != model.StatusParked || m.Retryable {
		t.Errorf("status %q retryable %v, want parked/false", m.Status, m.Retryable)
	}
	if m.Reason != "schema_incompatible: eruption_count_24h bukan angka" {
		t.Errorf("reason %q", m.Reason)
	}
	if m.SourceRefID != "r4" || m.Attempts != 1 || !m.ReceivedAt.Equal(now) {
		t.Errorf("got ref %q attempts %d received %v", m.SourceRefID, m.Attempts, m.ReceivedAt)
	}
	if string(m.Payload) != string(raw) || m.PayloadSize != int64(len(raw)) {
		t.Errorf("payload not stored intact: %q size %d", m.Payload, m.PayloadSize)
	}
}

func TestMessageIDIdentifiesContentNotArrival(t *testing.T) {
	raw := []byte(`{"report_id":"r3","alert_status":"Siaga"}`)
	cause := &mapper.RejectError{Code: model.ReasonSchemaIncompatible, Reason: "alert_level hilang"}
	first := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, cause, now)

	refetch := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, append([]byte(nil), raw...), cause, now.Add(3*time.Second))
	if refetch.MessageID != first.MessageID {
		t.Errorf("re-fetch from the cursor overlap got a new message_id")
	}

	changed := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, []byte(`{"report_id":"r3","alert_status":"Awas"}`), cause, now)
	otherEndpoint := Build(model.SourcePVMBG, mapper.EndpointSeismicEvents, raw, cause, now)
	otherSource := Build(model.SourceBMKG, mapper.EndpointVolcanicReports, raw, cause, now)
	for name, m := range map[string]model.UnprocessedMessage{"payload": changed, "endpoint": otherEndpoint, "source": otherSource} {
		if m.MessageID == first.MessageID {
			t.Errorf("different %s, same message_id", name)
		}
	}
	if len(first.MessageID) != 64 {
		t.Errorf("message_id %q is not a hex sha256", first.MessageID)
	}
}

func TestRecordOverOneMiBIsStoredTruncated(t *testing.T) {
	raw := bytes.Repeat([]byte("a"), 1<<20+1)
	cause := &mapper.RejectError{Code: model.ReasonSchemaIncompatible, SourceRefID: "r9", Reason: "alert_level hilang"}

	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, cause, now)

	if len(m.Payload) != 64<<10 || m.PayloadSize != 1<<20+1 {
		t.Errorf("stored %d of %d bytes, want 65536 of 1048577", len(m.Payload), m.PayloadSize)
	}
	sum := sha256.Sum256(raw)
	if m.PayloadSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("sha256 is not of the full payload")
	}
	if m.Code != model.ReasonTooLarge || m.Status != model.StatusParked {
		t.Errorf("code %q status %q, want too_large/parked", m.Code, m.Status)
	}
	if m.Reason != "too_large: record 1048577 bytes > 1048576, disimpan 65536 byte pertama; schema_incompatible: alert_level hilang" {
		t.Errorf("reason %q", m.Reason)
	}
}

// A whole response between 1 and 10 MiB that is not valid JSON keeps its real code: too_large is
// only for a record. It is still stored truncated, so replay counts it not_replayable.
func TestLargeMalformedResponseKeepsItsRealCode(t *testing.T) {
	body := append([]byte(`{"reports":[`), bytes.Repeat([]byte(" "), 2<<20)...)
	cause := json.Unmarshal(body, &struct{}{})

	m := BuildResponse(model.SourcePVMBG, mapper.EndpointVolcanicReports, body, cause, now)

	if m.Code != model.ReasonMalformedJSON || m.Status != model.StatusParked {
		t.Errorf("code %q status %q, want malformed_json/parked", m.Code, m.Status)
	}
	if m.Reason != "malformed_json: unexpected end of JSON input; respons 2097164 bytes > 1048576, disimpan 65536 byte pertama" {
		t.Errorf("reason %q", m.Reason)
	}
	if len(m.Payload) != 64<<10 || m.PayloadSize != 2<<20+12 {
		t.Errorf("stored %d of %d bytes, want 65536 of 2097164", len(m.Payload), m.PayloadSize)
	}
	if record := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, body, cause, now); m.MessageID != record.MessageID {
		t.Errorf("the label changed the message_id")
	}
}

func TestRecordAtOneMiBIsStoredIntact(t *testing.T) {
	raw := bytes.Repeat([]byte("a"), 1<<20)
	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, raw, &mapper.RejectError{Code: model.ReasonSchemaIncompatible, Reason: "x"}, now)
	if len(m.Payload) != 1<<20 || m.Code != model.ReasonSchemaIncompatible {
		t.Errorf("stored %d bytes with code %q, want intact schema_incompatible", len(m.Payload), m.Code)
	}
}

func TestTextFieldsAreAlwaysStorable(t *testing.T) {
	endpoint := "det\xffection" + strings.Repeat("x", 200)
	cause := errors.New("unknown event type \"\xfe\"")
	m := Build(model.SourceSatelit, endpoint, []byte(`{}`), cause, now)

	if !utf8.ValidString(m.Endpoint) || len(m.Endpoint) > MaxEndpoint {
		t.Errorf("endpoint %q (%d bytes) would be refused by the store", m.Endpoint, len(m.Endpoint))
	}
	if !utf8.ValidString(m.Reason) {
		t.Errorf("reason %q is not valid UTF-8", m.Reason)
	}
}
