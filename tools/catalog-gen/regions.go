package main

import "math/rand/v2"

type region struct {
	Place  string
	Lat    float64
	Lon    float64
	Spread float64
}

var megathrust = []region{
	{"Aceh", 3.5, 95.5, 0.8},
	{"Sumatera Utara", 1.5, 97.5, 0.8},
	{"Sumatera Barat", -1.0, 99.5, 0.8},
	{"Bengkulu", -3.8, 101.5, 0.8},
	{"Lampung", -5.8, 104.0, 0.7},
	{"Selat Sunda", -6.8, 105.5, 0.5},
	{"Jawa Barat", -7.8, 107.5, 0.7},
	{"Jawa Tengah", -8.5, 110.0, 0.7},
	{"Jawa Timur", -8.9, 112.8, 0.7},
	{"Bali", -9.2, 115.2, 0.5},
	{"Nusa Tenggara Barat", -9.4, 117.5, 0.6},
}

var elsewhere = []region{
	{"Sulawesi Tengah", -1.0, 120.0, 1.2},
	{"Sulawesi Utara", 1.3, 124.8, 1.0},
	{"Maluku", -3.5, 128.5, 1.5},
	{"Maluku Utara", 1.0, 127.5, 1.0},
	{"Papua", -3.5, 138.5, 2.0},
	{"Nusa Tenggara Timur", -8.8, 122.5, 1.0},
	{"Kalimantan Timur", 0.5, 117.0, 1.0},
}

func pickRegion(r *rand.Rand, skew float64) region {
	if r.Float64() < skew {
		return megathrust[r.IntN(len(megathrust))]
	}
	return elsewhere[r.IntN(len(elsewhere))]
}

func findRegion(place string) (region, bool) {
	for _, list := range [][]region{megathrust, elsewhere} {
		for _, reg := range list {
			if reg.Place == place {
				return reg, true
			}
		}
	}
	return region{}, false
}
