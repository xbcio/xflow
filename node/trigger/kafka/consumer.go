package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const consumerRetryDelay = 100 * time.Millisecond

type kafkaGoConsumer struct {
	reader *kafkago.Reader

	ctx    context.Context
	cancel context.CancelFunc

	messages chan Message
	done     chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newKafkaGoConsumer(cfg ConsumerConfig) (Consumer, error) {
	readerCfg, err := readerConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &kafkaGoConsumer{
		reader:   kafkago.NewReader(readerCfg),
		ctx:      ctx,
		cancel:   cancel,
		messages: make(chan Message, readerCfg.QueueCapacity),
		done:     make(chan struct{}),
	}
	go consumer.run()
	return consumer, nil
}

// readerConfigFor builds the kafka-go reader config, applying cfg.Tuning over
// the values that used to be inlined literals here.
//
// It returns an error rather than letting kafka-go decide: NewReader panics on
// a config its Validate rejects, and these values now come from operator-edited
// params, so a typo must fail this trigger's activation instead of taking the
// whole runner process down.
func readerConfigFor(cfg ConsumerConfig) (kafkago.ReaderConfig, error) {
	startOffset, err := startOffsetFor(cfg.StartOffset)
	if err != nil {
		return kafkago.ReaderConfig{}, err
	}
	tuning := cfg.Tuning
	if tuning.isZero() {
		tuning = defaultTuning()
	}
	if err := tuning.validate(); err != nil {
		return kafkago.ReaderConfig{}, err
	}
	queueCapacity := cfg.MaxInflight
	if queueCapacity <= 0 {
		queueCapacity = defaultTriggerMaxInflight
	}

	// The dialer is built unconditionally. It used to exist only on the SASL
	// path, which left dial_timeout with nowhere to land: kafka-go substituted
	// DefaultDialer and the configured value vanished with no diagnostic.
	dialer := &kafkago.Dialer{
		Timeout:   tuning.DialTimeout,
		DualStack: true,
	}
	if cfg.SASLMechanism != "" {
		mechanism, saslErr := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if saslErr != nil {
			return kafkago.ReaderConfig{}, saslErr
		}
		dialer.SASLMechanism = mechanism
	}

	return kafkago.ReaderConfig{
		Brokers:                cfg.Brokers,
		GroupID:                cfg.Group,
		Topic:                  cfg.Topic,
		StartOffset:            startOffset,
		QueueCapacity:          queueCapacity,
		MinBytes:               tuning.FetchMinBytes,
		MaxBytes:               tuning.FetchMaxBytes,
		MaxWait:                tuning.MaxWait,
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 5 * time.Second,
		ReadLagInterval:        -1,
		RebalanceTimeout:       tuning.RebalanceTimeout,
		SessionTimeout:         tuning.SessionTimeout,
		HeartbeatInterval:      tuning.HeartbeatInterval,
		JoinGroupBackoff:       time.Second,
		RetentionTime:          24 * time.Hour,
		OffsetOutOfRangeError:  false,
		ReadBackoffMin:         100 * time.Millisecond,
		ReadBackoffMax:         time.Second,
		// Synchronous commit. A periodic commit would let an offset land before
		// the side effect it belongs to succeeded, which is exactly the loss the
		// per-partition emit-then-commit worker exists to prevent. Not a knob.
		CommitInterval: 0,
		GroupBalancers: []kafkago.GroupBalancer{kafkago.RangeGroupBalancer{}, kafkago.RoundRobinGroupBalancer{}},
		Dialer:         dialer,
	}, nil
}

// buildSASLMechanism constructs the appropriate kafka-go SASL mechanism.
func buildSASLMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	switch strings.ToLower(mechanism) {
	case "plain":
		return &plain.Mechanism{Username: username, Password: password}, nil
	case "scram-sha-256":
		m, err := scram.Mechanism(scram.SHA256, username, password)
		if err != nil {
			return nil, fmt.Errorf("kafka sasl scram-sha-256: %w", err)
		}
		return m, nil
	case "scram-sha-512":
		m, err := scram.Mechanism(scram.SHA512, username, password)
		if err != nil {
			return nil, fmt.Errorf("kafka sasl scram-sha-512: %w", err)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported sasl_mechanism %q (supported: plain, scram-sha-256, scram-sha-512)", mechanism)
	}
}

func startOffsetFor(offset string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(offset)) {
	case "", "latest", "last", "newest":
		return kafkago.LastOffset, nil
	case "earliest", "first", "oldest", "beginning":
		return kafkago.FirstOffset, nil
	default:
		return 0, fmt.Errorf("kafka start_offset %q is not supported", offset)
	}
}

func (c *kafkaGoConsumer) Messages() <-chan Message { return c.messages }

func (c *kafkaGoConsumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.closeErr = c.reader.Close()
		<-c.done
	})
	return c.closeErr
}

func (c *kafkaGoConsumer) CommitMessages(ctx context.Context, messages ...Message) error {
	commits := make([]kafkago.Message, 0, len(messages))
	for _, msg := range messages {
		commits = append(commits, kafkago.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		})
	}
	return c.reader.CommitMessages(ctx, commits...)
}

func (c *kafkaGoConsumer) run() {
	defer close(c.done)
	defer close(c.messages)
	for {
		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			if !sleepConsumerRetry(c.ctx) {
				return
			}
			continue
		}
		select {
		case c.messages <- messageFromReader(msg):
		case <-c.ctx.Done():
			return
		}
	}
}

func sleepConsumerRetry(ctx context.Context) bool {
	timer := time.NewTimer(consumerRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func messageFromReader(msg kafkago.Message) Message {
	headers := make(map[string]string, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}
	return Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
		Headers:   headers,
	}
}
