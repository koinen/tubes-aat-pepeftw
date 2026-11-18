// Package unprocessed turns a message the system could not process into an unprocessed message:
// its message_id, reason code, status, and the stored payload.
package unprocessed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/mapper"
	"tubes-aat-pepeftw/cmd/aggregator/model"
	"tubes-aat-pepeftw/cmd/aggregator/repository"
)

const (
	// MaxIntact is the largest payload stored as-is.
	MaxIntact = 1 << 20
	// PrefixSize is how much of a larger payload is kept.
	PrefixSize = 64 << 10
	// MaxEndpoint caps the endpoint text, which for the satellite feed is the sender's SSE event type.
	MaxEndpoint = 64
)

// Build describes one record (a report, an event), which failed with cause, as an unprocessed
// message received at now. A record over MaxIntact that was rejected is labelled too_large.
func Build(source model.Source, endpoint string, raw []byte, cause error, now time.Time) model.UnprocessedMessage {
	return build(source, endpoint, raw, cause, now, true)
}

// BuildResponse describes a whole response that could not be decoded. It keeps the real code at
// any size; a body over MaxIntact is still stored as a prefix. For an *Oversized cause, body is
// ignored: the prefix, size and hash come from the cause.
func BuildResponse(source model.Source, endpoint string, body []byte, cause error, now time.Time) model.UnprocessedMessage {
	return build(source, endpoint, body, cause, now, false)
}

func build(source model.Source, endpoint string, raw []byte, cause error, now time.Time, record bool) model.UnprocessedMessage {
	code, ref, detail := classify(cause)
	endpoint = storable(endpoint, MaxEndpoint)

	payload, size, sum := raw, int64(len(raw)), [32]byte{}
	var over *Oversized
	if errors.As(cause, &over) {
		payload, size, sum = over.Prefix, over.Size, over.SHA256
		detail = fmt.Sprintf("%s, disimpan %d byte pertama", detail, len(over.Prefix))
	} else {
		sum = sha256.Sum256(raw)
		if len(raw) > MaxIntact {
			payload = raw[:PrefixSize]
		}
	}

	status := model.StatusParked
	if code == model.ReasonStoreUnavailable {
		status = model.StatusRetrying
	}
	switch {
	case over != nil || len(raw) <= MaxIntact || status != model.StatusParked:
	case record:
		detail = fmt.Sprintf("record %d bytes > %d, disimpan %d byte pertama; %s: %s", len(raw), MaxIntact, PrefixSize, code, detail)
		code = model.ReasonTooLarge
	default:
		detail = fmt.Sprintf("%s; respons %d bytes > %d, disimpan %d byte pertama", detail, len(raw), MaxIntact, PrefixSize)
	}

	return model.UnprocessedMessage{
		MessageID:     messageID(source, endpoint, sum),
		Source:        source,
		Endpoint:      endpoint,
		SourceRefID:   storable(ref, 0),
		ReceivedAt:    now.UTC(),
		Code:          code,
		Reason:        storable(string(code)+": "+detail, 0),
		Retryable:     status == model.StatusRetrying,
		Status:        status,
		Attempts:      1,
		Payload:       payload,
		PayloadSize:   size,
		PayloadSHA256: hex.EncodeToString(sum[:]),
	}
}

func classify(cause error) (code model.ReasonCode, ref, detail string) {
	var (
		re     *mapper.RejectError
		poison *Error
		over   *Oversized
		syntax *json.SyntaxError
	)
	switch {
	case errors.As(cause, &re):
		return re.Code, re.SourceRefID, re.Reason
	case errors.As(cause, &poison):
		return poison.Code, "", poison.Detail
	case errors.As(cause, &over):
		return model.ReasonTooLarge, "", over.Error()
	case errors.As(cause, &syntax):
		return model.ReasonMalformedJSON, "", cause.Error()
	case errors.Is(cause, repository.ErrUnavailable):
		return model.ReasonStoreUnavailable, "", cause.Error()
	case errors.Is(cause, repository.ErrRejected):
		return model.ReasonStoreRejected, "", cause.Error()
	}
	return model.ReasonSchemaIncompatible, "", cause.Error()
}

// messageID is sha256(source | endpoint | sha256(payload)). Hashing the payload's hash, not the
// payload, lets an oversized body that was only streamed get the same id as one held in memory.
func messageID(source model.Source, endpoint string, payloadSum [32]byte) string {
	h := sha256.New()
	h.Write([]byte(string(source) + "|" + endpoint + "|"))
	h.Write(payloadSum[:])
	return hex.EncodeToString(h.Sum(nil))
}

// storable makes text the store accepts: valid UTF-8, and at most max bytes when max > 0.
// Endpoint, reason, and source_ref_id can carry bytes the sender chose.
func storable(s string, max int) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	if max > 0 && len(s) > max {
		s = strings.ToValidUTF8(s[:max], "")
	}
	return s
}
