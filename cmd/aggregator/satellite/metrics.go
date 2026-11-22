package satellite

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PromMetrics struct {
	latency    prometheus.Histogram
	fires      prometheus.Gauge
	singles    prometheus.Gauge
	detections prometheus.Gauge
	applied    prometheus.Gauge
	head       prometheus.Gauge
	lag        prometheus.Gauge
	eta        prometheus.Gauge
	speed      prometheus.Gauge
	late       prometheus.Gauge
}

var (
	promOnce sync.Once
	prom     *PromMetrics
)

func NewPromMetrics() *PromMetrics {
	promOnce.Do(func() {
		prom = &PromMetrics{
			latency: promauto.NewHistogram(prometheus.HistogramOpts{
				Name:    "bnpb_satellite_event_to_hazard_seconds",
				Help:    "Lama dari tiba data deteksi di BNPB sampai HazardEvent WILDFIRE tersimpan.",
				Buckets: []float64{.5, 1, 2, 5, 10, 15, 20, 30, 45, 60, 120},
			}),
			fires:      promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_fires", Help: "Jumlah kebakaran hasil putaran terakhir."}),
			singles:    promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_singles", Help: "Jumlah deteksi tunggal (bukan kebakaran) pada putaran terakhir."}),
			detections: promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_active_detections", Help: "Jumlah deteksi aktif (bukan low, belum ditarik)."}),
			applied:    promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_applied_offset", Help: "Offset feed terakhir yang sudah diterapkan."}),
			head:       promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_feed_head_offset", Help: "Offset terbaru di feed."}),
			lag:        promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_lag_offsets", Help: "Selisih offset feed terbaru dengan yang sudah diterapkan."}),
			eta:        promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_catchup_eta_seconds", Help: "Perkiraan detik sampai mengejar feed."}),
			speed:      promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_feed_speed", Help: "Percepatan waktu feed."}),
			late:       promauto.NewGauge(prometheus.GaugeOpts{Name: "bnpb_satellite_late_detections", Help: "Jumlah deteksi yang melewati batas keterlambatan."}),
		}
	})
	return prom
}

func (p *PromMetrics) FireSaved(d time.Duration) { p.latency.Observe(d.Seconds()) }

func (p *PromMetrics) RecomputeDone(r Result) {
	p.fires.Set(float64(r.Fires))
	p.singles.Set(float64(r.Singles))
	p.detections.Set(float64(r.Detections))
}

func (p *PromMetrics) LagUpdated(l Lag) {
	p.applied.Set(float64(l.Applied))
	p.head.Set(float64(l.Head))
	p.lag.Set(float64(l.LagOffsets))
	p.eta.Set(l.ETA)
	p.speed.Set(l.Speed)
	p.late.Set(float64(l.Late))
}
