package satellite

import (
	"math"
	"sort"
	"time"
)

const (
	// LinkKm dan LinkWindow adalah aturan keterhubungan dua deteksi (spesifikasi, Definisi Kebakaran butir 4).
	LinkKm     = 1.0
	LinkWindow = 360 * time.Minute

	earthKm  = 6371.0088
	gridCell = 0.01 // derajat; 0,01 derajat lintang = 1,105 km, lebih besar dari LinkKm
)

// DistanceKm menghitung jarak lingkaran besar (haversine) dalam kilometer.
func DistanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const rad = math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthKm * math.Asin(math.Min(1, math.Sqrt(a)))
}

// Linked memberi tahu apakah dua deteksi terhubung: jarak <= 1,0 km dan selisih waktu <= 360 menit.
func Linked(a, b *Detection) bool {
	dt := a.AcqTime.Sub(b.AcqTime)
	if dt < 0 {
		dt = -dt
	}
	if dt > LinkWindow {
		return false
	}
	return DistanceKm(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= LinkKm
}

type cellKey struct{ x, y int }

// Cluster mengelompokkan deteksi menjadi komponen terhubung (keterhubungan berantai).
// Hasilnya urut: anggota tiap kelompok urut menurut (waktu, kunci), kelompok urut menurut anggota pertamanya.
// Kelompok berukuran 1 tidak dikembalikan, hanya dihitung di singles. Input harus berkunci unik
// dan sudah bebas dari deteksi low dan yang ditarik. Tidak menangani garis bujur 180 derajat.
func Cluster(dets []Detection) (groups [][]Detection, singles int) {
	n := len(dets)
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		for parent[x] != x {
			parent[x] = parent[parent[x]]
			x = parent[x]
		}
		return x
	}

	grid := make(map[cellKey][]int, n)
	cellOf := func(d *Detection) cellKey {
		return cellKey{int(math.Floor(d.Longitude / gridCell)), int(math.Floor(d.Latitude / gridCell))}
	}
	for i := range dets {
		k := cellOf(&dets[i])
		grid[k] = append(grid[k], i)
	}

	for i := range dets {
		d := &dets[i]
		k := cellOf(d)
		cosLat := math.Max(math.Cos(d.Latitude*math.Pi/180), 0.05)
		dx := int(math.Ceil(LinkKm / (111.32 * cosLat * gridCell)))
		for x := k.x - dx; x <= k.x+dx; x++ {
			for y := k.y - 1; y <= k.y+1; y++ {
				for _, j := range grid[cellKey{x, y}] {
					if j <= i {
						continue
					}
					if Linked(d, &dets[j]) {
						if a, b := find(i), find(j); a != b {
							parent[b] = a
						}
					}
				}
			}
		}
	}

	byRoot := make(map[int][]Detection)
	for i := range dets {
		r := find(i)
		byRoot[r] = append(byRoot[r], dets[i])
	}
	for _, g := range byRoot {
		if len(g) < 2 {
			singles++
			continue
		}
		sort.Slice(g, func(a, b int) bool { return before(&g[a], &g[b]) })
		groups = append(groups, g)
	}
	sort.Slice(groups, func(a, b int) bool { return before(&groups[a][0], &groups[b][0]) })
	return groups, singles
}

func before(a, b *Detection) bool {
	if !a.AcqTime.Equal(b.AcqTime) {
		return a.AcqTime.Before(b.AcqTime)
	}
	return a.Key < b.Key
}
