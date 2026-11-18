package unprocessed

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
)

func TestCheckPassesANormalResponse(t *testing.T) {
	body := []byte(`{"reports":[{"report_id":"r1","note":"[[{{ not nesting }}]]","x":{"y":[1,2]}}]}`)
	if err := Check(body); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestCheckCatchesPoisonBeforeDecoding(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want model.ReasonCode
	}{
		{"invalid UTF-8", []byte("{\"reports\":[{\"note\":\"\xff\xfe\"}]}"), model.ReasonInvalidUTF8},
		{"10,000 levels of nesting", []byte(strings.Repeat("[", 10000) + strings.Repeat("]", 10000)), model.ReasonTooDeep},
		{"one level past the limit", []byte(strings.Repeat("{\"a\":", MaxDepth) + "[]" + strings.Repeat("}", MaxDepth)), model.ReasonTooDeep},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, tt.body, Check(tt.body), now)
			if m.Code != tt.want || m.Status != model.StatusParked {
				t.Errorf("code %q status %q, want %q/parked", m.Code, m.Status, tt.want)
			}
			if !bytes.Equal(m.Payload, tt.body) {
				t.Errorf("payload not stored intact")
			}
		})
	}
}

func TestCheckAllowsExactlyMaxDepth(t *testing.T) {
	body := []byte(strings.Repeat("[", MaxDepth) + strings.Repeat("]", MaxDepth))
	if err := Check(body); err != nil {
		t.Errorf("depth %d rejected: %v", MaxDepth, err)
	}
}

func TestTruncatedJSONIsMalformed(t *testing.T) {
	body := []byte(`{"reports":[{"report_id":"r1"`)
	var v any
	err := json.Unmarshal(body, &v)
	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, body, err, now)
	if m.Code != model.ReasonMalformedJSON {
		t.Errorf("code %q, want malformed_json", m.Code)
	}
}

func TestOversizedResponseKeepsPrefixSizeAndHash(t *testing.T) {
	full := bytes.Repeat([]byte("x"), 11<<20)
	over := &Oversized{Prefix: full[:PrefixSize], Size: int64(len(full)), SHA256: sha256.Sum256(full), Limit: 10 << 20}

	m := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, nil, over, now)

	if m.Code != model.ReasonTooLarge || m.Status != model.StatusParked {
		t.Errorf("code %q status %q, want too_large/parked", m.Code, m.Status)
	}
	if len(m.Payload) != PrefixSize || m.PayloadSize != 11<<20 {
		t.Errorf("stored %d of %d", len(m.Payload), m.PayloadSize)
	}
	whole := Build(model.SourcePVMBG, mapper.EndpointVolcanicReports, full, &mapper.RejectError{Code: model.ReasonSchemaIncompatible}, now)
	if m.MessageID != whole.MessageID || m.PayloadSHA256 != whole.PayloadSHA256 {
		t.Errorf("message_id/sha256 of a streamed body differ from the same body held in memory")
	}
	if m.Reason != "too_large: respons 11534336 bytes > 10485760, disimpan 65536 byte pertama" {
		t.Errorf("reason %q", m.Reason)
	}
}
