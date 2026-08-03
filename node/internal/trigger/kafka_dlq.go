package trigger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaDeadLetterPublisher republishes messages the trigger could not process.
// It is an interface so tests can assert dead-letter behaviour without a broker
// and so a deployment can route dead letters somewhere other than Kafka.
type KafkaDeadLetterPublisher interface {
	// Publish writes msg to topic. It must return an error rather than dropping:
	// the caller withholds the offset commit on error so Kafka redelivers.
	Publish(ctx context.Context, topic string, msg KafkaMessage) error
	Close() error
}

// newKafkaDeadLetterPublisher is the construction seam, mirroring
// newKafkaConsumer so tests can substitute a fake.
var newKafkaDeadLetterPublisher = newKafkaGoDeadLetterPublisher

// kafkaGoWriter is the subset of kafka.Writer used here, extracted so tests can
// exercise kafkaGoDeadLetterPublisher's own logic (header stamping, timeout)
// without a broker.
type kafkaGoWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// kafkaGoDeadLetterPublisher publishes to the same cluster the consumer reads
// from, reusing its broker list and SASL credentials. It deliberately does NOT
// take a separate broker/credential config: a DLQ on a cluster the trigger
// cannot already reach would fail exactly when it is needed, and a second copy
// of the SASL password is a second thing to leak.
type kafkaGoDeadLetterPublisher struct {
	writer    kafkaGoWriter
	closeOnce sync.Once
	closeErr  error
}

// kafkaDeadLetterWriteTimeout bounds one dead-letter publish. It is not
// unbounded because the publish happens on the per-partition serial worker: a
// hung write would stall every subsequent message on that partition, turning a
// single malformed record into a partition-wide outage.
const kafkaDeadLetterWriteTimeout = 10 * time.Second

func newKafkaGoDeadLetterPublisher(cfg KafkaConsumerConfig) (KafkaDeadLetterPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka dead-letter publisher requires brokers")
	}
	transport := kafka.DefaultTransport
	if cfg.SASLMechanism != "" {
		mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, err
		}
		transport = &kafka.Transport{SASL: mechanism}
	}
	return &kafkaGoDeadLetterPublisher{
		writer: &kafka.Writer{
			Addr:      kafka.TCP(cfg.Brokers...),
			Balancer:  &kafka.Hash{},
			Transport: transport,
			// Synchronous so Publish's error return actually reflects the write.
			// An async writer would report success before the broker acked, and
			// the caller would commit the offset against a write that never
			// landed — the one outcome a DLQ exists to prevent.
			Async:        false,
			RequiredAcks: kafka.RequireAll,
		},
	}, nil
}

func (p *kafkaGoDeadLetterPublisher) Publish(ctx context.Context, topic string, msg KafkaMessage) error {
	ctx, cancel := context.WithTimeout(ctx, kafkaDeadLetterWriteTimeout)
	defer cancel()
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: kafkaDeadLetterHeaders(msg),
	})
}

func (p *kafkaGoDeadLetterPublisher) Close() error {
	p.closeOnce.Do(func() { p.closeErr = p.writer.Close() })
	return p.closeErr
}

// kafkaDeadLetterHeaders carries the original message's headers plus provenance
// (source topic/partition/offset and the reason) so a consumer of the DLQ can
// tell where the record came from and why it is here. Provenance goes in
// headers rather than wrapping the value so the payload stays byte-identical to
// what the producer sent — a re-drive can replay it without unwrapping.
func kafkaDeadLetterHeaders(msg KafkaMessage) []kafka.Header {
	headers := make([]kafka.Header, 0, len(msg.Headers)+4)
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	headers = append(headers,
		kafka.Header{Key: "xflow-dlq-reason", Value: []byte("schema")},
		kafka.Header{Key: "xflow-dlq-source-topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "xflow-dlq-source-partition", Value: []byte(fmt.Sprint(msg.Partition))},
		kafka.Header{Key: "xflow-dlq-source-offset", Value: []byte(fmt.Sprint(msg.Offset))},
	)
	return headers
}
