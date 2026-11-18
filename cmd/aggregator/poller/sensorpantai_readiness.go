package poller

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var readinessSample = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bnpb_sensor_pantai_healthz_sample_timestamp_seconds",
	Help: "Wall time of the latest advisory Sensor Pantai /healthz result.",
}, []string{"ready"})

func (p *sensorPantaiPoller) sampleReadiness(ctx context.Context) {
	now := p.now()
	if !p.readinessTriedAt.IsZero() && now.Sub(p.readinessTriedAt) < 30*time.Second {
		return
	}
	p.readinessTriedAt = now
	// This advisory request must not hold up the next observations poll for the normal data timeout.
	sampleCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	_, err := p.f.get(sampleCtx, "/healthz", nil)
	if ctx.Err() != nil {
		return
	}
	ready := err == nil
	readinessSample.WithLabelValues(strconv.FormatBool(ready)).Set(float64(p.now().UnixNano()) / 1e9)
	readinessSample.DeleteLabelValues(strconv.FormatBool(!ready))
}
