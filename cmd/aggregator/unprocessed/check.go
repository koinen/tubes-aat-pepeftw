package unprocessed

import (
	"fmt"

	"tubes-aat-pepeftw/internal/poison"
)

// MaxDepth is the deepest JSON nesting a source body may have (see internal/poison).
const MaxDepth = poison.MaxDepth

// Check rejects a body that must not be decoded: invalid UTF-8 or nesting deeper than MaxDepth.
// Its *poison.Error becomes the reason code of the same name (see classify).
func Check(body []byte) error { return poison.Check(body) }

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
