package poison

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeProducer records what it is asked to produce and fails the first fails attempts.
type fakeProducer struct {
	fails    int
	attempts int
	produced []*kgo.Record
}

func (p *fakeProducer) produce(_ context.Context, r *kgo.Record) error {
	p.attempts++
	if p.attempts <= p.fails {
		return errors.New("NOT_ENOUGH_REPLICAS")
	}
	p.produced = append(p.produced, r)
	return nil
}

func forwarder(p *fakeProducer) (*Forwarder, *[]int) {
	var waits []int
	f := &Forwarder{
		produce:  p.produce,
		topic:    "unprocessed",
		consumer: "notifier",
		log:      quiet,
		wait: func(ctx context.Context, attempt int) error {
			waits = append(waits, attempt)
			return ctx.Err()
		},
	}
	return f, &waits
}

func headers(r *kgo.Record) map[string]string {
	out := map[string]string{}
	for _, h := range r.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}

func poisonRecord() *kgo.Record {
	return &kgo.Record{
		Topic: "hazard-events", Partition: 2, Offset: 17,
		Key:   []byte("hz-1"),
		Value: []byte("{\"hazard_id\": \"\xff\"}"),
		Headers: []kgo.RecordHeader{
			{Key: "correlation_id", Value: []byte("corr-1")},
			{Key: "traceparent", Value: []byte("00-abc-def-01")},
			{Key: "other", Value: []byte("dropped")},
		},
	}
}

// The value goes to topic unprocessed byte for byte, with where it came from, why, and who sent it.
func TestForwardSendsTheValueUnchangedWithItsOrigin(t *testing.T) {
	p := &fakeProducer{}
	f, _ := forwarder(p)
	rec := poisonRecord()

	if err := f.Forward(context.Background(), rec, Check(rec.Value)); err != nil {
		t.Fatal(err)
	}
	if len(p.produced) != 1 {
		t.Fatalf("produced %d records, want 1", len(p.produced))
	}
	out := p.produced[0]
	if out.Topic != "unprocessed" || !bytes.Equal(out.Value, rec.Value) || string(out.Key) != "hz-1" {
		t.Errorf("produced topic %q key %q value %q", out.Topic, out.Key, out.Value)
	}
	want := map[string]string{
		HeaderTopic: "hazard-events", HeaderPartition: "2", HeaderOffset: "17",
		HeaderReason:     "invalid_utf8: body bukan UTF-8 yang valid",
		HeaderConsumer:   "notifier",
		"correlation_id": "corr-1", "traceparent": "00-abc-def-01",
	}
	got := headers(out)
	if len(got) != len(want) {
		t.Errorf("headers %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("header %s = %q, want %q", k, got[k], v)
		}
	}
}

// Until the produce succeeds the record must not be committed: Forward keeps trying, with backoff.
func TestForwardRetriesUntilTheProduceSucceeds(t *testing.T) {
	p := &fakeProducer{fails: 2}
	f, waits := forwarder(p)

	if err := f.Forward(context.Background(), poisonRecord(), errors.New("x")); err != nil {
		t.Fatal(err)
	}
	if p.attempts != 3 || len(p.produced) != 1 {
		t.Errorf("attempts %d, produced %d; want 3 and 1", p.attempts, len(p.produced))
	}
	if len(*waits) != 2 || (*waits)[0] != 1 || (*waits)[1] != 2 {
		t.Errorf("waits %v, want [1 2]", *waits)
	}
}

// When ctx ends before the produce succeeds, Forward reports it so the caller does not commit.
func TestForwardGivesUpOnlyWhenCtxEnds(t *testing.T) {
	p := &fakeProducer{fails: 1 << 30}
	f, _ := forwarder(p)
	ctx, cancel := context.WithCancel(context.Background())
	f.wait = func(ctx context.Context, attempt int) error {
		if attempt == 3 {
			cancel()
		}
		return ctx.Err()
	}

	err := f.Forward(ctx, poisonRecord(), errors.New("x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Forward = %v, want context.Canceled", err)
	}
	if len(p.produced) != 0 {
		t.Errorf("produced %d records", len(p.produced))
	}
}

// Every way a value can fail its check or decode gets one of the aggregator's reason codes.
func TestReasonNamesTheCode(t *testing.T) {
	var v struct {
		HazardID string `json:"hazard_id"`
	}
	decode := func(s string) error { return json.Unmarshal([]byte(s), &v) }
	cases := []struct {
		name string
		err  error
		code string
	}{
		{"invalid utf-8", Check([]byte("\"\xff\"")), "invalid_utf8"},
		{"too deep", Check([]byte(strings.Repeat("[", MaxDepth+1))), "too_deep"},
		{"truncated", decode(`{"hazard_id": "a`), "malformed_json"},
		{"empty", decode(``), "malformed_json"},
		{"wrong type", decode(`{"hazard_id": 7}`), "schema_incompatible"},
		{"missing field", errors.New("missing hazard_id"), "schema_incompatible"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.err == nil {
				t.Fatal("no error to classify")
			}
			got := Reason(c.err)
			if !strings.HasPrefix(got, c.code+": ") {
				t.Errorf("Reason = %q, want code %s", got, c.code)
			}
		})
	}
}
