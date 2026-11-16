package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"time"
)

func main() {
	count := flag.Int("n", 20000, "number of historical events")
	skew := flag.Float64("skew", 0.8, "share of events on the Sunda megathrust")
	spike := flag.Int("spike", 0, "emit a burst of this many events in one region instead of history")
	spikePlace := flag.String("spike-place", "Selat Sunda", "region for the burst")
	seed := flag.Uint64("seed", 1, "random seed")
	flag.Parse()

	r := rand.New(rand.NewPCG(*seed, *seed^0x9e3779b97f4a7c15))
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	w := csv.NewWriter(out)
	defer w.Flush()

	_ = w.Write([]string{"event_id", "time", "latitude", "longitude", "depth_km", "magnitude", "place"})

	if *spike > 0 {
		reg, ok := findRegion(*spikePlace)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown region %q\n", *spikePlace)
			os.Exit(2)
		}
		start := time.Now().UTC().Truncate(time.Second)
		for i := range *spike {
			at := start.Add(time.Duration(i) * time.Second)
			write(w, fmt.Sprintf("spike-%d-%06d", *seed, i), at, r, reg)
		}
		return
	}

	from := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	span := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).Sub(from)
	for i := range *count {
		at := from.Add(time.Duration(float64(span) * float64(i) / float64(*count)))
		write(w, fmt.Sprintf("hist-%d-%06d", *seed, i), at, r, pickRegion(r, *skew))
	}
}

func write(w *csv.Writer, id string, at time.Time, r *rand.Rand, reg region) {
	lat := reg.Lat + r.NormFloat64()*reg.Spread/2
	lon := reg.Lon + r.NormFloat64()*reg.Spread/2
	mag := math.Min(9.0, 2.5-math.Log10(1-r.Float64()))
	depth := 5 + r.ExpFloat64()*40
	_ = w.Write([]string{
		id,
		at.Format(time.RFC3339),
		strconv.FormatFloat(lat, 'f', 4, 64),
		strconv.FormatFloat(lon, 'f', 4, 64),
		strconv.FormatFloat(math.Min(depth, 650), 'f', 1, 64),
		strconv.FormatFloat(mag, 'f', 1, 64),
		reg.Place,
	})
}
