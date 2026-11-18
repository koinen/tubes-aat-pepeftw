package unprocessed

import (
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
)

// Written counts a newly stored unprocessed message.
func Written(m model.UnprocessedMessage) {
	written.WithLabelValues(string(m.Source), string(m.Code)).Inc()
}

// Recovered counts an unprocessed message that became a HazardEvent.
func Recovered(source model.Source) {
	recovered.WithLabelValues(string(source)).Inc()
}
