package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxAlertBytes = 2 << 20
	recentLimit   = 100
)

type alert struct {
	HazardID   string `json:"hazard_id"`
	EventID    string `json:"event_id"`
	Source     string `json:"source"`
	Severity   string `json:"severity"`
	HazardType string `json:"hazard_type"`
	AreaName   string `json:"area_name"`
	Attributes struct {
		FetchedAt string `json:"fetched_at,omitempty"`
		Verified  *bool  `json:"verified,omitempty"`
		Critical  *bool  `json:"critical,omitempty"`
	} `json:"attributes"`
}

type receipt struct {
	HazardID   string     `json:"hazard_id"`
	ReceivedAt time.Time  `json:"received_at"`
	FetchedAt  *time.Time `json:"fetched_at,omitempty"`
	LatencyMS  *int64     `json:"latency_ms,omitempty"`
}

type receiver struct {
	*http.ServeMux
	topic   string
	produce func(context.Context, *kgo.Record) error
	now     func() time.Time

	mu     sync.Mutex
	recent []recentDelivery
	seen   map[string]struct{}
	next   int

	registry   *prometheus.Registry
	latency    *prometheus.HistogramVec
	duplicates prometheus.Counter
}

type recentDelivery struct {
	Alert   alert   `json:"alert"`
	Receipt receipt `json:"receipt"`
}

func newReceiver(topic string, produce func(context.Context, *kgo.Record) error, now func() time.Time) *receiver {
	r := &receiver{
		ServeMux: http.NewServeMux(), topic: topic, produce: produce, now: now,
		seen:     make(map[string]struct{}, recentLimit),
		registry: prometheus.NewRegistry(),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bnpb_field_alert_latency_seconds",
			Help:    "Alert receipt time minus fetch time for newly seen alerts on this replica.",
			Buckets: []float64{0.25, 0.5, 1, 2, 3, 4, 5, 7.5, 10},
		}, []string{"source"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bnpb_field_alert_duplicates_total",
			Help: "Acknowledged alerts already in this replica's recent list.",
		}),
	}
	r.registry.MustRegister(r.latency, r.duplicates)
	r.HandleFunc("POST /alerts", r.postAlert)
	r.HandleFunc("GET /alerts", r.getAlerts)
	return r
}

func (r *receiver) metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{}))
	return mux
}

func (r *receiver) postAlert(w http.ResponseWriter, req *http.Request) {
	receivedAt := r.now().UTC()
	var a alert
	req.Body = http.MaxBytesReader(w, req.Body, maxAlertBytes)
	if err := json.NewDecoder(req.Body).Decode(&a); err != nil || a.HazardID == "" {
		http.Error(w, "invalid alert", http.StatusBadRequest)
		return
	}

	delivery := receipt{HazardID: a.HazardID, ReceivedAt: receivedAt}
	if fetchedAt, err := time.Parse(time.RFC3339, a.Attributes.FetchedAt); err == nil {
		fetchedAt = fetchedAt.UTC()
		ms := receivedAt.Sub(fetchedAt).Milliseconds()
		delivery.FetchedAt = &fetchedAt
		delivery.LatencyMS = &ms
	}
	value, err := json.Marshal(delivery)
	if err != nil {
		http.Error(w, "cannot encode receipt", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), time.Second)
	defer cancel()
	if err := r.produce(ctx, &kgo.Record{Topic: r.topic, Key: []byte(a.HazardID), Value: value}); err != nil {
		http.Error(w, "receipt not acknowledged", http.StatusServiceUnavailable)
		return
	}
	if r.remember(recentDelivery{Alert: a, Receipt: delivery}) {
		r.duplicates.Inc()
	} else if delivery.FetchedAt != nil {
		r.latency.WithLabelValues(a.Source).Observe(receivedAt.Sub(*delivery.FetchedAt).Seconds())
	}
	w.WriteHeader(http.StatusNoContent)
}

// remember reports whether this hazard is already in the replica's bounded demo list.
func (r *receiver) remember(d recentDelivery) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := d.Alert.HazardID
	if _, exists := r.seen[id]; exists {
		return true
	}
	r.seen[id] = struct{}{}
	if len(r.recent) < recentLimit {
		r.recent = append(r.recent, d)
	} else {
		delete(r.seen, r.recent[r.next].Alert.HazardID)
		r.recent[r.next] = d
		r.next = (r.next + 1) % recentLimit
	}
	return false
}

func (r *receiver) getAlerts(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	recent := make([]recentDelivery, 0, len(r.recent))
	recent = append(recent, r.recent[r.next:]...)
	recent = append(recent, r.recent[:r.next]...)
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(recent)
}
