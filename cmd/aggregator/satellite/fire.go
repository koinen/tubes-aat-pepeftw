package satellite

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"time"
)

const (
	SeverityWaspada = "WASPADA"
	SeveritySiaga   = "SIAGA"
	SeverityAwas    = "AWAS"
)

// Fire adalah satu kebakaran hasil pengelompokan, siap dipetakan menjadi HazardEvent WILDFIRE.
type Fire struct {
	SourceRefID   string
	Count         int
	Satellites    []string
	MaxConfidence string
	TotalFRP      float64 // MW, dibulatkan 2 desimal
	Latitude      float64 // titik pusat: rata-rata koordinat anggota
	Longitude     float64
	OccurredAt    time.Time // waktu deteksi paling awal
	Severity      string
	DetectionKeys []string
}

var confidenceRank = map[string]int{"l": 0, "n": 1, "h": 2}

// BuildFire menyusun Fire dari anggota satu kelompok. members harus hasil Cluster (urut waktu lalu kunci),
// sehingga hasilnya sama persis untuk data yang sama berapa kali pun diproses.
func BuildFire(members []Detection) Fire {
	f := Fire{Count: len(members), OccurredAt: members[0].AcqTime, MaxConfidence: members[0].Confidence}

	sats := map[string]bool{}
	var frp, lat, lon float64
	for _, m := range members {
		sats[m.Satellite] = true
		frp += m.FRP
		lat += m.Latitude
		lon += m.Longitude
		f.DetectionKeys = append(f.DetectionKeys, m.Key)
		if confidenceRank[m.Confidence] > confidenceRank[f.MaxConfidence] {
			f.MaxConfidence = m.Confidence
		}
	}
	for s := range sats {
		f.Satellites = append(f.Satellites, s)
	}
	sort.Strings(f.Satellites)
	sort.Strings(f.DetectionKeys)

	n := float64(len(members))
	f.Latitude = round(lat/n, 5)
	f.Longitude = round(lon/n, 5)
	f.TotalFRP = round(frp, 2)
	f.Severity = severity(len(members), frp)

	sum := sha256.Sum256([]byte(members[0].Key))
	f.SourceRefID = "fire-" + hex.EncodeToString(sum[:])[:12]
	return f
}

func severity(count int, frp float64) string {
	switch {
	case count >= 20 || frp >= 100:
		return SeverityAwas
	case count >= 6 || frp >= 30:
		return SeveritySiaga
	default:
		return SeverityWaspada
	}
}

func round(x float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(x*p) / p
}
