package mapper

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const (
	EndpointSeismicEvents   = "seismic-events"
	EndpointTsunamiWarnings = "tsunami-warnings"
	EndpointVolcanicReports = "volcanic-reports"
)

var hazardNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://github.com/koinen/tubes-aat-pepeftw/hazard"))

func HazardID(source model.Source, sourceRefID string) string {
	return uuid.NewSHA1(hazardNamespace, []byte(string(source)+":"+sourceRefID)).String()
}

type RejectError struct {
	SourceRefID string
	Reason      string
}

func (e *RejectError) Error() string {
	if e.SourceRefID == "" {
		return e.Reason
	}
	return e.SourceRefID + ": " + e.Reason
}

func reject(ref, format string, args ...any) *RejectError {
	return &RejectError{SourceRefID: ref, Reason: fmt.Sprintf(format, args...)}
}

func Rejected(source model.Source, endpoint string, raw json.RawMessage, err error, now time.Time) model.RejectedRecord {
	r := model.RejectedRecord{
		Source:     source,
		Endpoint:   endpoint,
		Reason:     err.Error(),
		Payload:    raw,
		RejectedAt: now.UTC(),
	}
	if re, ok := err.(*RejectError); ok {
		r.SourceRefID = re.SourceRefID
		r.Reason = re.Reason
	}
	if !json.Valid(raw) {
		quoted, _ := json.Marshal(string(raw))
		r.Payload = quoted
	}
	return r
}

type fields struct {
	m   map[string]any
	ref string 
}

func decode(raw json.RawMessage) (*fields, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, reject("", "JSON tidak valid atau bukan object: %v", err)
	}
	if m == nil {
		return nil, reject("", "record kosong (null)")
	}
	return &fields{m: m}, nil
}

func (f *fields) take(key string) (any, bool) {
	v, ok := f.m[key]
	delete(f.m, key)
	return v, ok && v != nil
}

func (f *fields) peek(key string) (any, bool) {
	v, ok := f.m[key]
	return v, ok && v != nil
}

func (f *fields) str(v any, ok bool, key string) (string, error) {
	if !ok {
		return "", reject(f.ref, "field wajib %s tidak ada", key)
	}
	s, isStr := v.(string)
	if !isStr || strings.TrimSpace(s) == "" {
		return "", reject(f.ref, "field %s harus string tidak kosong", key)
	}
	return s, nil
}

func (f *fields) num(v any, ok bool, key string) (float64, error) {
	if !ok {
		return 0, reject(f.ref, "field wajib %s tidak ada", key)
	}
	n, isNum := v.(float64)
	if !isNum || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, reject(f.ref, "field %s harus angka", key)
	}
	return n, nil
}

func (f *fields) boolean(v any, ok bool, key string) (bool, error) {
	if !ok {
		return false, reject(f.ref, "field wajib %s tidak ada", key)
	}
	b, isBool := v.(bool)
	if !isBool {
		return false, reject(f.ref, "field %s harus boolean", key)
	}
	return b, nil
}

func (f *fields) timestamp(v any, ok bool, key string) (time.Time, error) {
	s, err := f.str(v, ok, key)
	if err != nil {
		return time.Time{}, err
	}
	t, perr := time.Parse(time.RFC3339, s)
	if perr != nil {
		return time.Time{}, reject(f.ref, "field %s harus waktu RFC 3339, dapat %q", key, s)
	}
	return t.UTC().Truncate(time.Second), nil
}

func (f *fields) takeString(key string) (string, error) {
	v, ok := f.take(key)
	return f.str(v, ok, key)
}
func (f *fields) takeNumber(key string) (float64, error) {
	v, ok := f.take(key)
	return f.num(v, ok, key)
}
func (f *fields) takeTime(key string) (time.Time, error) {
	v, ok := f.take(key)
	return f.timestamp(v, ok, key)
}

func (f *fields) peekNumber(key string) (float64, error) {
	v, ok := f.peek(key)
	return f.num(v, ok, key)
}
func (f *fields) peekBool(key string) (bool, error) {
	v, ok := f.peek(key)
	return f.boolean(v, ok, key)
}
func (f *fields) peekString(key string) (string, error) {
	v, ok := f.peek(key)
	return f.str(v, ok, key)
}

func (f *fields) attributes() map[string]any {
	return maps.Clone(f.m)
}

func validCoordinate(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}

var severityRank = map[model.Severity]int{
	model.SeverityNormal:  0,
	model.SeverityWaspada: 1,
	model.SeveritySiaga:   2,
	model.SeverityAwas:    3,
}

func parseLevel(s string) (model.Severity, bool) {
	sev := model.Severity(strings.ToUpper(strings.TrimSpace(s)))
	_, ok := severityRank[sev]
	return sev, ok
}