package satellite

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Kind string

const (
	KindDetection Kind = "detection"
	KindUpdate    Kind = "update"
	KindRetract   Kind = "retract"
	KindHeartbeat Kind = "heartbeat"
	// KindParked holds the offset of an event that went to the DLQ. It carries no data.
	KindParked Kind = "parked"
)

type Detection struct {
	Key        string
	Satellite  string
	AcqTime    time.Time
	Latitude   float64
	Longitude  float64
	Confidence string
	FRP        float64
	ReceivedAt time.Time
}

type Changes struct {
	Confidence *string  `json:"confidence"`
	FRP        *float64 `json:"frp"`
}

type Update struct {
	Key     string
	Changes Changes
}

type Retract struct {
	Key    string
	Reason string
}

type Event struct {
	Offset    int64
	Kind      Kind
	Detection *Detection
	Update    *Update
	Retract   *Retract
}

type detectionBody struct {
	Offset    int64 `json:"offset"`
	Detection struct {
		Satellite  string  `json:"satellite"`
		AcqDate    string  `json:"acq_date"`
		AcqTime    int     `json:"acq_time"`
		Latitude   float64 `json:"latitude"`
		Longitude  float64 `json:"longitude"`
		Confidence string  `json:"confidence"`
		FRP        float64 `json:"frp"`
	} `json:"detection"`
}

type updateBody struct {
	Offset  int64   `json:"offset"`
	Key     string  `json:"key"`
	Changes Changes `json:"changes"`
}

type retractBody struct {
	Offset int64  `json:"offset"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

func AcqTimeUTC(date string, hhmm int) (time.Time, error) {
	if hhmm < 0 || hhmm > 2359 || hhmm%100 > 59 {
		return time.Time{}, fmt.Errorf("acq_time %d di luar rentang HHMM", hhmm)
	}
	return time.Parse("2006-01-02 1504", fmt.Sprintf("%s %04d", date, hhmm))
}

func DetectionKey(satellite, date string, hhmm int, lat, lon float64) string {
	return fmt.Sprintf("%s|%s|%d|%.5f|%.5f", satellite, date, hhmm, lat, lon)
}

func Decode(eventType string, data []byte) (Event, error) {
	switch Kind(strings.TrimSpace(eventType)) {
	case KindDetection:
		var b detectionBody
		if err := json.Unmarshal(data, &b); err != nil {
			return Event{}, fmt.Errorf("decode detection: %w", err)
		}
		d := b.Detection
		at, err := AcqTimeUTC(d.AcqDate, d.AcqTime)
		if err != nil {
			return Event{}, fmt.Errorf("decode detection offset %d: %w", b.Offset, err)
		}
		return Event{Offset: b.Offset, Kind: KindDetection, Detection: &Detection{
			Key:        DetectionKey(d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude),
			Satellite:  d.Satellite,
			AcqTime:    at,
			Latitude:   d.Latitude,
			Longitude:  d.Longitude,
			Confidence: strings.ToLower(d.Confidence),
			FRP:        d.FRP,
		}}, nil
	case KindUpdate:
		var b updateBody
		if err := json.Unmarshal(data, &b); err != nil || b.Key == "" {
			return Event{}, fmt.Errorf("decode update: invalid body (%v)", err)
		}
		if b.Changes.Confidence != nil {
			c := strings.ToLower(*b.Changes.Confidence)
			b.Changes.Confidence = &c
		}
		return Event{Offset: b.Offset, Kind: KindUpdate, Update: &Update{Key: b.Key, Changes: b.Changes}}, nil
	case KindRetract:
		var b retractBody
		if err := json.Unmarshal(data, &b); err != nil || b.Key == "" {
			return Event{}, fmt.Errorf("decode retract: invalid body (%v)", err)
		}
		return Event{Offset: b.Offset, Kind: KindRetract, Retract: &Retract{Key: b.Key, Reason: b.Reason}}, nil
	case KindHeartbeat:
		var b struct {
			Offset *int64 `json:"offset"`
		}
		_ = json.Unmarshal(data, &b)
		off := int64(-1)
		if b.Offset != nil {
			off = *b.Offset
		}
		return Event{Offset: off, Kind: KindHeartbeat}, nil
	default:
		return Event{}, fmt.Errorf("unknown event type %q", eventType)
	}
}
