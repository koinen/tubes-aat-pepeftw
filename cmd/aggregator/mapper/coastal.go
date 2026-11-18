package mapper

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

// fetchedAtLayout is RFC 3339 with milliseconds: latency is measured from fetched_at.
const fetchedAtLayout = "2006-01-02T15:04:05.000Z07:00"

// Value limits for an observation. They are physical bounds, not tuned to any sensor profile.
const (
	maxWaveHeightM        = 30
	maxSeaLevelAnomalyAbs = 10
)

// Station is a Sensor Pantai station from GET /v1/stations.
type Station struct {
	Name      string
	Province  string
	Latitude  float64
	Longitude float64
}

type StationLookup func(stationID string) (s Station, ok bool)

// CoastalPage is what Coastal needs from the observations response an observation came in.
type CoastalPage struct {
	GeneratedAt time.Time // sensor time
	FetchedAt   time.Time // wall clock when the response arrived
}

// CoastalResult is the verdict on one observation. A valid observation has only Hazard. A contract
// violation has only Violation, unless it is an unverified alert, which has both.
type CoastalResult struct {
	Hazard    *model.HazardEvent
	Violation *RejectError
}

type observation struct {
	ref       string
	stationID string
	station   Station
	observed  string
	critical  bool
	values    map[string]any // the values that could be read, as attributes
	problems  []string       // value checks that failed
}

// Coastal maps one Sensor Pantai observation and checks it against the base contract.
func Coastal(raw json.RawMessage, page CoastalPage, lookup StationLookup) CoastalResult {
	f, err := decode(raw)
	if err != nil {
		return CoastalResult{Violation: violation(err)}
	}
	var o observation
	if err := o.readIdentity(f, lookup); err != nil {
		return CoastalResult{Violation: violation(err)}
	}
	o.readValues(f, page)
	if len(o.problems) == 0 {
		h := o.hazard(page, "")
		return CoastalResult{Hazard: &h}
	}
	v := &RejectError{Code: model.ReasonContractViolation, SourceRefID: o.ref, Reason: strings.Join(o.problems, "; ")}
	if !o.critical {
		return CoastalResult{Violation: v}
	}
	h := o.hazard(page, v.Reason)
	return CoastalResult{Hazard: &h, Violation: v}
}

// readIdentity reads what an alert needs to be addressed: which observation, where, when, and whether
// the sensor flagged it critical. Without these an observation can only be parked.
func (o *observation) readIdentity(f *fields, lookup StationLookup) error {
	var err error
	if o.ref, err = f.takeString("observation_id"); err != nil {
		return err
	}
	f.ref = o.ref
	if o.stationID, err = f.takeString("station_id"); err != nil {
		return err
	}
	var ok bool
	if o.station, ok = lookup(o.stationID); !ok {
		return reject(o.ref, "station_id %q tidak ada di /v1/stations", o.stationID)
	}
	if o.observed, err = f.takeString("observed_at"); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339, o.observed); err != nil {
		return reject(o.ref, "field observed_at harus waktu RFC 3339, dapat %q", o.observed)
	}
	if o.critical, err = f.peekBool("critical"); err != nil {
		return err
	}
	return nil
}

// readValues runs every value check and records each failure, so a parked record lists all of them.
func (o *observation) readValues(f *fields, page CoastalPage) {
	o.values = map[string]any{}
	fail := func(err error) { o.problems = append(o.problems, err.(*RejectError).Reason) }
	problem := func(format string, args ...any) { fail(reject(o.ref, format, args...)) }

	observed, _ := time.Parse(time.RFC3339, o.observed)
	if observed.After(page.GeneratedAt) {
		problem("observed_at %s setelah generated_at %s", o.observed, page.GeneratedAt.Format(time.RFC3339))
	}
	if sea, err := f.takeNumber("sea_level_anomaly_m"); err != nil {
		fail(err)
	} else {
		o.values["sea_level_anomaly_m"] = sea
		if math.Abs(sea) > maxSeaLevelAnomalyAbs {
			problem("sea_level_anomaly_m %v di luar -%d..%d", sea, maxSeaLevelAnomalyAbs, maxSeaLevelAnomalyAbs)
		}
	}
	if wave, err := f.takeNumber("wave_height_m"); err != nil {
		fail(err)
	} else {
		o.values["wave_height_m"] = wave
		if wave < 0 || wave > maxWaveHeightM {
			problem("wave_height_m %v di luar 0..%d", wave, maxWaveHeightM)
		}
	}
	if quality, err := f.takeString("quality"); err != nil {
		fail(err)
	} else {
		o.values["quality"] = quality
		if quality != "ok" && quality != "suspect" {
			problem("quality %q tidak dikenal (harus ok/suspect)", quality)
		}
	}
}

// hazard builds the HazardEvent. A non-empty violation makes it an unverified alert.
func (o *observation) hazard(page CoastalPage, violation string) model.HazardEvent {
	severity := model.SeverityNormal
	if o.critical {
		severity = model.SeverityAwas
	}
	attrs := map[string]any{
		"station_id":  o.stationID,
		"province":    o.station.Province,
		"critical":    o.critical,
		"verified":    violation == "",
		"observed_at": o.observed,
		"fetched_at":  page.FetchedAt.UTC().Format(fetchedAtLayout),
	}
	for k, v := range o.values {
		attrs[k] = v
	}
	if violation != "" {
		attrs["violation"] = violation
	}
	return model.HazardEvent{
		HazardID:    HazardID(model.SourceSensorPantai, o.ref),
		Source:      model.SourceSensorPantai,
		SourceRefID: o.ref,
		HazardType:  model.HazardCoastal,
		Severity:    severity,
		AreaName:    o.station.Name,
		Latitude:    o.station.Latitude,
		Longitude:   o.station.Longitude,
		OccurredAt:  page.FetchedAt,
		IngestedAt:  page.FetchedAt,
		Attributes:  attrs,
	}
}

// violation turns any rejection of an observation into a contract violation, keeping its reason.
func violation(err error) *RejectError {
	var r *RejectError
	if !errors.As(err, &r) {
		r = &RejectError{Reason: err.Error()}
	}
	v := *r
	v.Code = model.ReasonContractViolation
	return &v
}

// observationKeys are the attributes the sensor itself reported. Two HazardEvents for the same
// observation_id with different values here are two different observations.
var observationKeys = []string{"station_id", "observed_at", "sea_level_anomaly_m", "wave_height_m", "quality", "critical"}

// SameObservation reports whether a and b, two COASTAL HazardEvents, carry the same sensor values.
// When and how we fetched them is ignored.
func SameObservation(a, b model.HazardEvent) bool {
	for _, k := range observationKeys {
		if a.Attributes[k] != b.Attributes[k] {
			return false
		}
	}
	return true
}

// IDConflict is the violation for an observation_id that arrived again with different values.
func IDConflict(ref string) *RejectError {
	return &RejectError{Code: model.ReasonContractViolation, SourceRefID: ref,
		Reason: "id_conflict: observation_id sudah tersimpan dengan nilai berbeda"}
}
