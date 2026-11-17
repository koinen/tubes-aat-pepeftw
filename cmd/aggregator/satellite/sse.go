package satellite

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type RawEvent struct {
	ID   string
	Type string
	Data []byte
}

const maxLine = 1 << 20

func ReadSSE(r io.Reader, fn func(RawEvent) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)

	var cur RawEvent
	var data [][]byte
	has := false

	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			if has {
				cur.Data = bytes.Join(data, []byte("\n"))
				if err := fn(cur); err != nil {
					return err
				}
			}
			cur, data, has = RawEvent{}, nil, false
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			cur.ID, has = value, true
		case "event":
			cur.Type, has = value, true
		case "data":
			data, has = append(data, []byte(value)), true
		}
	}
	return sc.Err()
}
