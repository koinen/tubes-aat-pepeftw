package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("field_team_receiver_stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	var brokers []string
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}
	topic := os.Getenv("KAFKA_TOPIC_FIELD_TEAM_RECEIPTS")
	if topic == "" {
		topic = "field-team-receipts"
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		return err
	}
	defer client.Close()

	r := newReceiver(topic, func(ctx context.Context, record *kgo.Record) error {
		err := client.ProduceSync(ctx, record).FirstErr()
		if err != nil {
			slog.Warn("receipt_produce_failed", "hazard_id", string(record.Key), "error", err)
		}
		return err
	}, time.Now)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	alerts := newServer(":"+port, r)
	metrics := newServer(":9090", r.metricsHandler())
	serverErr := make(chan error, 2)
	for _, server := range []*http.Server{alerts, metrics} {
		go func() { serverErr <- server.ListenAndServe() }()
	}

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(serveErr, alerts.Shutdown(shutdown), metrics.Shutdown(shutdown))
}

func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr: addr, Handler: handler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}
