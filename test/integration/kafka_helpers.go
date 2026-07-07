//go:build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partitions)
	if err != nil {
		t.Fatalf("dial kafka leader: %v", err)
	}
	defer conn.Close()
	// creating via DialLeader implicitly creates the topic on first write;
	// ensure partitions exist
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		// ignore "already exists"
		_ = err
	}
}

func writeKafkaMessages(t *testing.T, brokers []string, topic string, msgs []kafka.Message) {
	t.Helper()
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("write kafka: %v", err)
	}
}

// uniqueTopic returns a timestamped unique topic name to avoid cross-test pollution.
func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// itoa helper to avoid strconv import collisions.
func itoa(i int) string { return strconv.Itoa(i) }
