package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Dead-letter reasons. They are stamped into the published record as
// provenance, and they are a closed enum built here: a consumer of the DLQ
// filters on them to tell "the payload could not be used" (schema) from "the
// aggregator was at its buffer cap and parked a perfectly good record instead
// of dropping it" (buffer_overflow). The two need different operator responses,
// so a single value would make the header useless.
const (
	// deadLetterReasonSchema is the invalid-message axis
	// (message_schema.on_invalid=dead_letter).
	deadLetterReasonSchema = "schema"
	// deadLetterReasonOverflow is the aggregate overflow axis
	// (aggregate.on_overflow=dead_letter). It deliberately matches the reason
	// the discard path reports to xflow_trigger_messages_discarded_total, so one
	// word means one thing across the two axes: this record was read and would
	// have been dropped for buffer pressure.
	deadLetterReasonOverflow = "buffer_overflow"
)

// DeadLetterPublisher republishes messages the trigger could not process.
// It is an interface so tests can assert dead-letter behaviour without a broker
// and so a deployment can route dead letters somewhere other than Kafka.
type DeadLetterPublisher interface {
	// Publish writes msg to topic, stamped with reason. It must return an error
	// rather than dropping: the caller withholds the offset commit on error so
	// Kafka redelivers.
	//
	// reason is required rather than derived from msg because it is the one
	// thing about a parked record that the record itself cannot carry. Both
	// axes that reach this interface (schema.go and the aggregate overflow path)
	// publish through one shared publisher, so leaving the reason to the message
	// would make every overflow record claim to be a schema failure.
	Publish(ctx context.Context, topic string, msg Message, reason string) error
	Close() error
}

// newDeadLetterPublisher is the construction seam, mirroring
// newConsumer so tests can substitute a fake.
var newDeadLetterPublisher = newKafkaGoDeadLetterPublisher

// kafkaGoWriter is the subset of kafka.Writer used here, extracted so tests can
// exercise kafkaGoDeadLetterPublisher's own logic (header stamping, timeout)
// without a broker.
type kafkaGoWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
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

// deadLetterWriteTimeout bounds one dead-letter publish. It is not
// unbounded because the publish happens on the per-partition serial worker: a
// hung write would stall every subsequent message on that partition, turning a
// single malformed record into a partition-wide outage.
const deadLetterWriteTimeout = 10 * time.Second

func newKafkaGoDeadLetterPublisher(cfg ConsumerConfig) (DeadLetterPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka dead-letter publisher requires brokers")
	}
	transport := kafkago.DefaultTransport
	if cfg.SASLMechanism != "" {
		mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, err
		}
		transport = &kafkago.Transport{SASL: mechanism}
	}
	return &kafkaGoDeadLetterPublisher{
		writer: &kafkago.Writer{
			Addr:      kafkago.TCP(cfg.Brokers...),
			Balancer:  &kafkago.Hash{},
			Transport: transport,
			// Synchronous so Publish's error return actually reflects the write.
			// An async writer would report success before the broker acked, and
			// the caller would commit the offset against a write that never
			// landed — the one outcome a DLQ exists to prevent.
			Async:        false,
			RequiredAcks: kafkago.RequireAll,
		},
	}, nil
}

func (p *kafkaGoDeadLetterPublisher) Publish(ctx context.Context, topic string, msg Message, reason string) error {
	ctx, cancel := context.WithTimeout(ctx, deadLetterWriteTimeout)
	defer cancel()
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: deadLetterHeaders(msg, reason),
	})
}

func (p *kafkaGoDeadLetterPublisher) Close() error {
	p.closeOnce.Do(func() { p.closeErr = p.writer.Close() })
	return p.closeErr
}

// deadLetterHeaders carries the original message's headers plus provenance
// (source topic/partition/offset and the reason) so a consumer of the DLQ can
// tell where the record came from and why it is here. Provenance goes in
// headers rather than wrapping the value so the payload stays byte-identical to
// what the producer sent — a re-drive can replay it without unwrapping.
func deadLetterHeaders(msg Message, reason string) []kafkago.Header {
	headers := make([]kafkago.Header, 0, len(msg.Headers)+4)
	for k, v := range msg.Headers {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	headers = append(headers,
		kafkago.Header{Key: "xflow-dlq-reason", Value: []byte(reason)},
		kafkago.Header{Key: "xflow-dlq-source-topic", Value: []byte(msg.Topic)},
		kafkago.Header{Key: "xflow-dlq-source-partition", Value: []byte(fmt.Sprint(msg.Partition))},
		kafkago.Header{Key: "xflow-dlq-source-offset", Value: []byte(fmt.Sprint(msg.Offset))},
	)
	return headers
}
