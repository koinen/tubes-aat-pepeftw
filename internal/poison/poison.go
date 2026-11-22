// Package poison rejects a JSON body that must not be decoded at all. Every entry point runs it
// before encoding/json: the aggregator's pollers and consumers, client-api's field reports, and
// the hazard-events consumers, which forward what they cannot read to topic unprocessed (Forwarder).
package poison

import (
	"fmt"
	"unicode/utf8"
)

// MaxDepth is the deepest JSON nesting a body may have. Real bodies are under 5 levels.
const MaxDepth = 32

// Kind names the poison. Its values are the aggregator's reason codes (Lampiran A).
type Kind string

const (
	InvalidUTF8 Kind = "invalid_utf8"
	TooDeep     Kind = "too_deep"
)

// Error is a poison verdict about a body as a whole.
type Error struct {
	Kind   Kind
	Detail string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Detail }

// Check rejects a body that must not be decoded: invalid UTF-8 (encoding/json would silently
// replace it), or nesting deeper than MaxDepth. It scans without allocating.
func Check(body []byte) error {
	if !utf8.Valid(body) {
		return &Error{Kind: InvalidUTF8, Detail: "body bukan UTF-8 yang valid"}
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
				return &Error{Kind: TooDeep, Detail: fmt.Sprintf("nesting lebih dari %d level (byte %d)", MaxDepth, i)}
			}
		case c == '}' || c == ']':
			depth--
		}
	}
	return nil
}
