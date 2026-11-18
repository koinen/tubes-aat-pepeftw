package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"tubes-aat-pepeftw/cmd/client-api/internal/fieldreport"

	wire "tubes-aat-pepeftw/internal/fieldreport"
	"tubes-aat-pepeftw/internal/telemetry"
)

// FieldReportProducer writes field reports to their topic. It is its own client, apart from the
// revocation consumer. It connects on the first report, so client-api starts without Kafka and
// only POST /field-reports fails while Kafka is down.
type FieldReportProducer struct {
	client *kgo.Client
	topic  string
}

var _ fieldreport.Publisher = (*FieldReportProducer)(nil)

func NewFieldReportProducer(brokers, topic string) (*FieldReportProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitBrokers(brokers)...),
		kgo.DefaultProduceTopic(topic),
		// acks=all with the idempotent producer (franz-go's default): a 202 means every in-sync
		// replica has the report, and a retried batch is not written twice.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(fieldreport.PublishTimeout),
		// A caller is waiting for each report; do not hold it back to fill a batch.
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, fmt.Errorf("create field-report producer: %w", err)
	}
	return &FieldReportProducer{client: cl, topic: topic}, nil
}

// Publish writes m keyed by its report_id and waits for the acknowledgement or ctx's end. A report
// whose wait ended may still be written; the client resends under the same key and the
// aggregator stores it once.
func (p *FieldReportProducer) Publish(ctx context.Context, m wire.Message) error {
	value, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode field report %s: %w", m.ReportID, err)
	}
	rec := &kgo.Record{Topic: p.topic, Key: []byte(m.ReportID), Value: value}
	if m.CorrelationID != "" {
		rec.Headers = []kgo.RecordHeader{{Key: "correlation_id", Value: []byte(m.CorrelationID)}}
	}
	ctx, span := telemetry.Produce(ctx, rec)
	err = p.client.ProduceSync(ctx, rec).FirstErr()
	telemetry.Produced(span, p.topic, err)
	return err
}

func (p *FieldReportProducer) Close() {
	p.client.Close()
}
