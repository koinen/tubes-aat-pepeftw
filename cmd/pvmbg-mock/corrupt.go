package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"

	"github.com/gofiber/fiber/v3"
)

// Corruption kinds for POST /admin/corrupt. Each one is a P3 K3 poison input. A test-only admin
// extension: off by default, and it turns itself off after the requested number of responses.
const (
	corruptTruncated   = "truncated"
	corruptDeep        = "deep"
	corruptInvalidUTF8 = "invalid_utf8"
	corruptOversized   = "oversized"

	corruptDepth    = 10000
	oversizedBytes  = 50 << 20
	maxCorruptCount = 100
)

var corruptKinds = []string{corruptTruncated, corruptDeep, corruptInvalidUTF8, corruptOversized}

const corruptHelp = `body harus {"kind": "truncated"|"deep"|"invalid_utf8"|"oversized", "count": 0..100}; count 0 mematikan`

func validCorruptKind(kind string) bool {
	for _, k := range corruptKinds {
		if k == kind {
			return true
		}
	}
	return false
}

type adminCorrupt struct {
	Kind      string `json:"kind,omitempty"`
	Remaining int    `json:"remaining"`
}

func parseCorruptRequest(body []byte) (kind string, count int, ok bool) {
	var req struct {
		Kind  string `json:"kind"`
		Count *int   `json:"count"`
	}
	if err := json.Unmarshal(body, &req); err != nil || !validCorruptKind(req.Kind) {
		return "", 0, false
	}
	count = 1
	if req.Count != nil {
		count = *req.Count
	}
	if count < 0 || count > maxCorruptCount {
		return "", 0, false
	}
	return req.Kind, count, true
}

// sendCorrupt answers 200 with a broken version of real, the response the client would have got.
func sendCorrupt(c fiber.Ctx, kind string, real []byte) error {
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	switch kind {
	case corruptTruncated:
		return c.Send(real[:len(real)/2])
	case corruptDeep:
		var b bytes.Buffer
		b.WriteString(`{"generated_at":"`)
		b.WriteString(generatedAt(real))
		b.WriteString(`","reports":`)
		b.WriteString(strings.Repeat("[", corruptDepth))
		b.WriteString(strings.Repeat("]", corruptDepth))
		b.WriteString("}")
		return c.Send(b.Bytes())
	case corruptInvalidUTF8:
		return c.Send(bytes.Replace(real, []byte(`"generated_at":"`), []byte("\"generated_at\":\"\xff\xfe"), 1))
	default: // corruptOversized: streamed, so the mock never holds 50 MiB.
		at := generatedAt(real)
		return c.SendStreamWriter(func(w *bufio.Writer) {
			head := `{"generated_at":"` + at + `","reports":[],"padding":"`
			chunk := bytes.Repeat([]byte("x"), 64<<10)
			if _, err := w.WriteString(head); err != nil {
				return
			}
			for n := len(head); n < oversizedBytes; n += len(chunk) {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
			w.WriteString(`"}`)
			w.Flush()
		})
	}
}

func generatedAt(real []byte) string {
	var r struct {
		GeneratedAt string `json:"generated_at"`
	}
	_ = json.Unmarshal(real, &r)
	return r.GeneratedAt
}
