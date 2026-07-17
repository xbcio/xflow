package trigger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaConsumerRetryDelay = 100 * time.Millisecond

type kafkaGoConsumer struct {
	reader *kafka.Reader

	ctx    context.Context
	cancel context.CancelFunc

	messages chan KafkaMessage
	done     chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newKafkaGoConsumer(cfg KafkaConsumerConfig) (KafkaConsumer, error) {
	startOffset, err := kafkaStartOffset(cfg.StartOffset)
	if err != nil {
		return nil, err
	}
	queueCapacity := cfg.MaxInflight
	if queueCapacity <= 0 {
		queueCapacity = defaultTriggerMaxInflight
	}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &kafkaGoConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:                cfg.Brokers,
			GroupID:                cfg.Group,
			Topic:                  cfg.Topic,
			StartOffset:            startOffset,
			QueueCapacity:          queueCapacity,
			MinBytes:               1,
			MaxBytes:               10e6,
			WatchPartitionChanges:  true,
			PartitionWatchInterval: 5 * time.Second,
			ReadLagInterval:        -1,
			RebalanceTimeout:       30 * time.Second,
			SessionTimeout:         30 * time.Second,
			HeartbeatInterval:      3 * time.Second,
			JoinGroupBackoff:       time.Second,
			RetentionTime:          24 * time.Hour,
			OffsetOutOfRangeError:  false,
			ReadBackoffMin:         100 * time.Millisecond,
			ReadBackoffMax:         time.Second,
			CommitInterval:         0,
			GroupBalancers:         []kafka.GroupBalancer{kafka.RangeGroupBalancer{}, kafka.RoundRobinGroupBalancer{}},
		}),
		ctx:      ctx,
		cancel:   cancel,
		messages: make(chan KafkaMessage, queueCapacity),
		done:     make(chan struct{}),
	}
	go consumer.run()
	return consumer, nil
}

func kafkaStartOffset(offset string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(offset)) {
	case "", "latest", "last", "newest":
		return kafka.LastOffset, nil
	case "earliest", "first", "oldest", "beginning":
		return kafka.FirstOffset, nil
	default:
		return 0, fmt.Errorf("kafka start_offset %q is not supported", offset)
	}
}

func (c *kafkaGoConsumer) Messages() <-chan KafkaMessage { return c.messages }

func (c *kafkaGoConsumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.closeErr = c.reader.Close()
		<-c.done
	})
	return c.closeErr
}

func (c *kafkaGoConsumer) CommitMessages(ctx context.Context, messages ...KafkaMessage) error {
	commits := make([]kafka.Message, 0, len(messages))
	for _, msg := range messages {
		commits = append(commits, kafka.Message{
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
			if !sleepKafkaConsumerRetry(c.ctx) {
				return
			}
			continue
		}
		select {
		case c.messages <- kafkaMessageFromReader(msg):
		case <-c.ctx.Done():
			return
		}
	}
}

func sleepKafkaConsumerRetry(ctx context.Context) bool {
	timer := time.NewTimer(kafkaConsumerRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func kafkaMessageFromReader(msg kafka.Message) KafkaMessage {
	headers := make(map[string]string, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}
	return KafkaMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
		Headers:   headers,
	}
}
