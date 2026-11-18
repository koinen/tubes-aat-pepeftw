package forwarded

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

var now = time.Date(2026, 11, 18, 4, 0, 0, 0, time.UTC)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const truncated = `{"event_id":"ev-1","hazard_id":"hz`

// forwardedBy is the record a consumer produced to topic unprocessed for hazard-events/2/17.
func forwardedBy(consumer string, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic: "unprocessed", Partition: 1, Offset: offset,
		Value: []byte(truncated),
		Headers: []kgo.RecordHeader{
			{Key: "topic", Value: []byte("hazard-events")},
			{Key: "partition", Value: []byte("2")},
			{Key: "offset", Value: []byte("17")},
			{Key: "reason", Value: []byte("malformed_json: unexpected end of JSON input")},
			{Key: "consumer", Value: []byte(consumer)},
			{Key: "correlation_id", Value: []byte("corr-1")},
		},
	}
}

// A forwarded record is parked as INTERNAL under its original topic position, with the reason the
// consumer gave and the value intact.
func TestDecideParksAtTheOriginalPosition(t *testing.T) {
	m := Decide(forwardedBy("notifier", 40), now)

	if m.Source != model.SourceInternal || m.Endpoint != "hazard-events/2/17" || m.Status != model.StatusParked {
		t.Errorf("source %s endpoint %s status %s", m.Source, m.Endpoint, m.Status)
	}
	if m.Code != model.ReasonMalformedJSON {
		t.Errorf("code %s, want malformed_json", m.Code)
	}
	if !strings.HasPrefix(m.Reason, "malformed_json: unexpected end of JSON input") || !strings.Contains(m.Reason, "notifier") {
		t.Errorf("reason %q", m.Reason)
	}
	if !bytes.Equal(m.Payload, []byte(truncated)) || m.CorrelationID != "corr-1" || m.Retryable {
		t.Errorf("payload %q correlation %q retryable %v", m.Payload, m.CorrelationID, m.Retryable)
	}
}

// Three consumer groups forward the same record; the message_id is the same, so it is one row.
func TestTheSameRecordFromEveryConsumerIsOneMessage(t *testing.T) {
	sum := sha256.Sum256([]byte(truncated))
	h := sha256.New()
	h.Write([]byte("INTERNAL|hazard-events/2/17|"))
	h.Write(sum[:])
	want := hex.EncodeToString(h.Sum(nil))

	for i, consumer := range []string{"notifier", "audit-log", "dashboard-updater"} {
		if got := Decide(forwardedBy(consumer, int64(40+i)), now).MessageID; got != want {
			t.Errorf("%s: message_id %s, want %s", consumer, got, want)
		}
	}
}

// Bytes produced straight to topic unprocessed have no origin; they are still parked, under their
// own position.
func TestDecideParksARecordWithoutHeaders(t *testing.T) {
	r := &kgo.Record{Topic: "unprocessed", Partition: 0, Offset: 5, Value: []byte("garbage")}
	m := Decide(r, now)
	if m.Source != model.SourceInternal || m.Endpoint != "unprocessed/0/5" || m.Code != model.ReasonSchemaIncompatible {
		t.Errorf("source %s endpoint %s code %s", m.Source, m.Endpoint, m.Code)
	}
	if !strings.Contains(m.Reason, "header") {
		t.Errorf("reason %q does not say which header", m.Reason)
	}
}

func TestDecideKeepsAnUnknownReasonAsDetail(t *testing.T) {
	r := forwardedBy("notifier", 40)
	r.Headers[3].Value = []byte("cosmic_ray: bit flip")
	m := Decide(r, now)
	if m.Endpoint != "hazard-events/2/17" || m.Code != model.ReasonSchemaIncompatible || !strings.Contains(m.Reason, "cosmic_ray: bit flip") {
		t.Errorf("endpoint %s code %s reason %q", m.Endpoint, m.Code, m.Reason)
	}
}

type fakeStore struct {
	fail  []error
	saved []model.UnprocessedMessage
}

func (s *fakeStore) SaveUnprocessed(_ context.Context, m model.UnprocessedMessage) (bool, error) {
	if len(s.fail) > 0 {
		err := s.fail[0]
		s.fail = s.fail[1:]
		return false, err
	}
	s.saved = append(s.saved, m)
	return true, nil
}

func handler(store *fakeStore) *Handler {
	h := NewHandler(store, quiet)
	h.now = func() time.Time { return now }
	h.wait = func(ctx context.Context, _ int) error { return ctx.Err() }
	return h
}

// The record is committed only once it is parked: while the store fails, the same record is retried.
func TestHandleRetriesUntilParked(t *testing.T) {
	store := &fakeStore{fail: []error{errors.New("connection refused"), errors.New("connection refused")}}
	if err := handler(store).Handle(context.Background(), forwardedBy("notifier", 40)); err != nil {
		t.Fatal(err)
	}
	if len(store.saved) != 1 || store.saved[0].Endpoint != "hazard-events/2/17" {
		t.Errorf("saved %+v", store.saved)
	}
}

func TestHandleStopsWhenCtxEnds(t *testing.T) {
	store := &fakeStore{fail: []error{errors.New("connection refused")}}
	ctx, cancel := context.WithCancel(context.Background())
	h := handler(store)
	h.wait = func(context.Context, int) error { cancel(); return ctx.Err() }
	if err := h.Handle(ctx, forwardedBy("notifier", 40)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle = %v, want context.Canceled", err)
	}
	if len(store.saved) != 0 {
		t.Errorf("saved %d", len(store.saved))
	}
}
