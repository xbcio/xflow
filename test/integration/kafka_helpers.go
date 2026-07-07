//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Mirror nodes/node/trigger_kafka_test.go:createKafkaIntegrationTopic: dial
	// any broker, resolve the controller, then CreateTopics on the controller
	// connection. CreateTopics is idempotent in kafka-go (TopicAlreadyExists is
	// suppressed internally), so any error here is a real failure.
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka broker: %v", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("get kafka controller: %v", err)
	}
	_ = conn.Close()
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial kafka controller: %v", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create kafka topic %q: %v", topic, err)
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
