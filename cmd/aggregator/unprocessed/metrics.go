package unprocessed

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

var (
	written = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bnpb_unprocessed_written_total",
		Help: "Pesan yang baru masuk ke unprocessed_messages, per sumber dan kode alasan.",
	}, []string{"source", "code"})
	recovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bnpb_unprocessed_recovered_total",
		Help: "Pesan unprocessed yang akhirnya tersimpan sebagai HazardEvent.",
	}, []string{"source"})
	adaptationApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bnpb_adaptation_applied_total",
		Help: "Laporan yang menjadi HazardEvent berkat aturan adaptasi yang dideklarasikan, per sumber dan aturan.",
	}, []string{"source", "rule"})
)

// Written counts a newly stored unprocessed message.
func Written(m model.UnprocessedMessage) {
	written.WithLabelValues(string(m.Source), string(m.Code)).Inc()
}

// Recovered counts an unprocessed message that became a HazardEvent.
func Recovered(source model.Source) {
	recovered.WithLabelValues(string(source)).Inc()
}

// AdaptationObserver returns the observer for mapper.VolcanicAdaptations.WithObserver: it counts
// each declared rule a stored report needed and logs it with the report's source_ref_id.
func AdaptationObserver(log *slog.Logger, source model.Source, correlationID string) func(rule, sourceRefID string) {
	return func(rule, sourceRefID string) {
		adaptationApplied.WithLabelValues(string(source), rule).Inc()
		log.Info("adaptation_applied", "correlation_id", correlationID, "source", string(source),
			"source_ref_id", sourceRefID, "rule", rule)
	}
}
