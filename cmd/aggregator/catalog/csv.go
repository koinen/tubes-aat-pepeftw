package catalog

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

var requiredColumns = []string{"event_id", "time", "latitude", "longitude", "depth_km", "magnitude", "place"}

type Row struct {
	Line      int
	EventID   string
	Time      string
	Latitude  string
	Longitude string
	DepthKm   string
	Magnitude string
	Place     string
}

type Reader struct {
	csv   *csv.Reader
	index map[string]int
	line  int
}

func NewReader(r io.Reader) (*Reader, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read catalog header: %w", err)
	}
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[strings.ToLower(strings.TrimSpace(strings.TrimPrefix(name, string(rune(0xFEFF)))))] = i
	}
	for _, col := range requiredColumns {
		if _, ok := index[col]; !ok {
			return nil, fmt.Errorf("catalog header is missing column %q", col)
		}
	}
	return &Reader{csv: cr, index: index, line: 1}, nil
}

func (r *Reader) Next() (Row, error) {
	rec, err := r.csv.Read()
	if err != nil {
		return Row{}, err
	}
	r.line++
	return Row{
		Line:      r.line,
		EventID:   r.field(rec, "event_id"),
		Time:      r.field(rec, "time"),
		Latitude:  r.field(rec, "latitude"),
		Longitude: r.field(rec, "longitude"),
		DepthKm:   r.field(rec, "depth_km"),
		Magnitude: r.field(rec, "magnitude"),
		Place:     r.field(rec, "place"),
	}, nil
}

func (r *Reader) field(rec []string, name string) string {
	i := r.index[name]
	if i >= len(rec) {
		return ""
	}
	return strings.TrimSpace(rec[i])
}
