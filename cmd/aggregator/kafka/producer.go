package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/aggregator/model"
)

const deliveryTimeout = 10 * time.Second

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client, topic: topic}, nil
}

func (p *Producer) Publish(ctx context.Context, records []model.OutboxRecord) []error {
	batch := make([]*kgo.Record, len(records))
	position := make(map[*kgo.Record]int, len(records))
	for i, r := range records {
		rec := &kgo.Record{Key: []byte(r.HazardID), Value: r.Payload}
		if r.CorrelationID != "" {
			rec.Headers = []kgo.RecordHeader{{Key: "correlation_id", Value: []byte(r.CorrelationID)}}
		}
		batch[i] = rec
		position[rec] = i
	}

	errs := make([]error, len(records))
	for _, res := range p.client.ProduceSync(ctx, batch...) {
		errs[position[res.Record]] = res.Err
	}
	return errs
}

func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

func (p *Producer) Topic() string {
	return p.topic
}

func (p *Producer) Close() {
	p.client.Close()
}
