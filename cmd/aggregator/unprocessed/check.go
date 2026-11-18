package unprocessed

import (
	"fmt"
	"unicode/utf8"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// MaxDepth is the deepest JSON nesting a source body may have. Real responses are under 5 levels.
const MaxDepth = 32

// Error is a poison verdict about a body as a whole.
type Error struct {
	Code   model.ReasonCode
	Detail string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Detail }

// Check rejects a body that must not be decoded: invalid UTF-8 (encoding/json would silently
// replace it), or nesting deeper than MaxDepth. It scans without allocating.
func Check(body []byte) error {
	if !utf8.Valid(body) {
		return &Error{Code: model.ReasonInvalidUTF8, Detail: "body bukan UTF-8 yang valid"}
	}
	depth, inString, escaped := 0, false, false
	for i, c := range body {
		switch {
		case escaped:
			escaped = false
		case inString:
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{' || c == '[':
			if depth++; depth > MaxDepth {
				return &Error{Code: model.ReasonTooDeep, Detail: fmt.Sprintf("nesting lebih dari %d level (byte %d)", MaxDepth, i)}
			}
		case c == '}' || c == ']':
			depth--
		}
	}
	return nil
}

// Oversized is a response body larger than Limit. Only its first PrefixSize bytes were kept;
// the rest was streamed through the hash.
type Oversized struct {
	Prefix []byte
	Size   int64
	SHA256 [32]byte
	Limit  int64
}

func (e *Oversized) Error() string {
	return fmt.Sprintf("respons %d bytes > %d", e.Size, e.Limit)
}
