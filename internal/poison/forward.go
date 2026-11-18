package poison

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Headers on a record forwarded to topic unprocessed. The value is the original value, unchanged.
const (
	HeaderTopic     = "topic"
	HeaderPartition = "partition"
	HeaderOffset    = "offset"
	HeaderReason    = "reason" // "<code>: <detail>"
	HeaderConsumer  = "consumer"
)

// carried are the original record's headers that a forwarded record keeps.
var carried = []string{"correlation_id", "traceparent"}

// Reason codes a consumer can report, besides the Kind values.
const (
	MalformedJSON      = "malformed_json"
	SchemaIncompatible = "schema_incompatible"
)

// MaxForwardBytes is the largest record a forwarding client may produce. Topic unprocessed takes
// 2 MiB, so a record that fit its source topic (~1 MiB) always fits with the headers added.
const MaxForwardBytes = 2 << 20

// Reason is "<code>: <detail>" for an error from Check or from decoding a value: a poison Kind,
// malformed_json for a JSON syntax error, otherwise schema_incompatible.
func Reason(err error) string {
	var (
		bad    *Error
		syntax *json.SyntaxError
	)
	switch {
	case errors.As(err, &bad):
		return string(bad.Kind) + ": " + bad.Detail
	case errors.As(err, &syntax):
		return MalformedJSON + ": " + err.Error()
	}
	return SchemaIncompatible + ": " + err.Error()
}

// Forwarder sends a value a consumer could not process to topic unprocessed, for the aggregator
// to park as an INTERNAL unprocessed message.
type Forwarder struct {
	produce  func(ctx context.Context, r *kgo.Record) error
	topic    string
	consumer string
	log      *slog.Logger
	// wait sleeps before the next attempt (attempt counts from 1); it returns ctx's error if ctx ends first.
	wait func(ctx context.Context, attempt int) error
}

const attemptTimeout = 10 * time.Second

// NewForwarder produces through client, which must allow records up to MaxForwardBytes
// (kgo.ProducerBatchMaxBytes). consumer names the sending service in the record's headers.
func NewForwarder(client *kgo.Client, topic, consumer string, log *slog.Logger) *Forwarder {
	produce := func(ctx context.Context, r *kgo.Record) error {
		ctx, cancel := context.WithTimeout(ctx, attemptTimeout)
		defer cancel()
		// franz-go keeps the produce ctx in the record (Record.Context) and fails it once that ctx
		// is done, so every attempt sends its own copy; r itself is never produced.
		rec := *r
		return client.ProduceSync(ctx, &rec).FirstErr()
	}
	return &Forwarder{produce: produce, topic: topic, consumer: consumer, log: log, wait: Backoff}
}

// Forward sends rec's value, which failed with cause, to topic unprocessed. It retries until the
// produce succeeds and returns an error only when ctx ends first; then rec must not be committed.
func (f *Forwarder) Forward(ctx context.Context, rec *kgo.Record, cause error) error {
	reason := Reason(cause)
	out := &kgo.Record{
		Topic: f.topic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: HeaderTopic, Value: []byte(rec.Topic)},
			{Key: HeaderPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: HeaderOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: HeaderReason, Value: []byte(reason)},
			{Key: HeaderConsumer, Value: []byte(f.consumer)},
		},
	}
	correlationID := ""
	for _, h := range rec.Headers {
		for _, k := range carried {
			if h.Key == k {
				out.Headers = append(out.Headers, h)
			}
		}
		if h.Key == "correlation_id" {
			correlationID = string(h.Value)
		}
	}
	attrs := []any{"correlation_id", correlationID, "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset}

	for attempt := 1; ; attempt++ {
		err := f.produce(ctx, out)
		if err == nil {
			f.log.Warn("message_invalid", append(attrs, "reason", reason, "forwarded_to", f.topic)...)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		f.log.Error("unprocessed_forward_failed", append(attrs, "attempt", attempt, "error", err.Error())...)
		if err := f.wait(ctx, attempt); err != nil {
			return err
		}
	}
}

const (
	retryBase = 500 * time.Millisecond
	retryMax  = 30 * time.Second
)

// Backoff waits 0.5 s doubling up to 30 s before attempt+1; it returns ctx's error if ctx ends first.
func Backoff(ctx context.Context, attempt int) error {
	d := retryMax
	if attempt < 7 {
		d = min(retryBase<<(attempt-1), retryMax)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
