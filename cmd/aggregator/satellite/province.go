package satellite

import "math"

type provincePoint struct {
	Name     string
	Lat, Lon float64
}

// provincePoints adalah titik acuan per provinsi (titik pusat, ditambah ibu kota atau titik tambahan untuk
// provinsi yang luas). Provinsi terdekat dicari dari titik acuan terdekat, bukan dari batas wilayah resmi.
var provincePoints = []provincePoint{
	{"Aceh", 4.70, 96.75},
	{"Sumatera Utara", 2.10, 99.05},
	{"Sumatera Barat", -0.85, 100.35},
	{"Riau", 0.50, 101.55},
	{"Riau", 0.30, 102.40},
	{"Kepulauan Riau", 0.92, 104.45},
	{"Kepulauan Riau", 3.90, 108.20},
	{"Jambi", -1.60, 103.00},
	{"Sumatera Selatan", -2.99, 104.76},
	{"Sumatera Selatan", -3.90, 103.90},
	{"Kepulauan Bangka Belitung", -2.70, 106.50},
	{"Bengkulu", -3.60, 102.30},
	{"Lampung", -4.90, 105.10},
	{"Banten", -6.40, 106.10},
	{"DKI Jakarta", -6.21, 106.85},
	{"Jawa Barat", -6.90, 107.60},
	{"Jawa Tengah", -7.15, 110.15},
	{"DI Yogyakarta", -7.88, 110.40},
	{"Jawa Timur", -7.75, 112.75},
	{"Bali", -8.40, 115.20},
	{"Nusa Tenggara Barat", -8.60, 117.40},
	{"Nusa Tenggara Timur", -8.60, 121.00},
	{"Nusa Tenggara Timur", -10.17, 123.60},
	{"Kalimantan Barat", -0.03, 109.33},
	{"Kalimantan Barat", 0.90, 111.30},
	{"Kalimantan Barat", -0.05, 110.50},
	{"Kalimantan Tengah", -2.21, 113.92},
	{"Kalimantan Tengah", -1.30, 112.30},
	{"Kalimantan Tengah", -2.50, 111.80},
	{"Kalimantan Tengah", -1.70, 113.40},
	{"Kalimantan Selatan", -3.10, 115.30},
	{"Kalimantan Timur", 0.50, 116.40},
	{"Kalimantan Timur", -0.50, 117.15},
	{"Kalimantan Timur", -1.24, 116.85},
	{"Kalimantan Utara", 3.00, 116.50},
	{"Sulawesi Utara", 1.00, 124.80},
	{"Gorontalo", 0.70, 122.45},
	{"Sulawesi Tengah", -1.40, 121.40},
	{"Sulawesi Barat", -2.50, 119.40},
	{"Sulawesi Selatan", -3.60, 120.00},
	{"Sulawesi Tenggara", -4.10, 122.10},
	{"Maluku Utara", 1.50, 127.80},
	{"Maluku", -3.20, 129.00},
	{"Maluku", -3.70, 128.20},
	{"Papua Barat Daya", -1.00, 131.20},
	{"Papua Barat", -1.30, 133.50},
	{"Papua Tengah", -3.80, 136.00},
	{"Papua", -4.00, 138.50},
	{"Papua Pegunungan", -4.20, 139.20},
	{"Papua Selatan", -7.00, 139.50},
}

// NearestProvince mengembalikan nama provinsi dari titik acuan terdekat. Jarak sama: nama lebih kecil
// secara alfabet, supaya hasilnya selalu sama.
func NearestProvince(lat, lon float64) string {
	best, bestD := "", math.MaxFloat64
	for _, p := range provincePoints {
		d := DistanceKm(lat, lon, p.Lat, p.Lon)
		if d < bestD || (d == bestD && p.Name < best) {
			best, bestD = p.Name, d
		}
	}
	return best
}
