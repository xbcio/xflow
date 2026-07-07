//go:build perf

package perf

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func realKafkaBrokers(b *testing.B) []string {
	b.Helper()
	raw := os.Getenv("XFLOW_TEST_KAFKA_BROKERS")
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := kafka.DialContext(ctx, "tcp", out[0])
	if err != nil {
		b.Skipf("kafka unavailable: %v", err)
	}
	_ = c.Close()
	return out
}

// createTopic creates a Kafka topic by dialing the controller node directly,
// mirroring the pattern in test/integration/kafka_helpers.go.
func createTopic(b *testing.B, broker, topic string, partitions int) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		b.Fatalf("dial kafka broker: %v", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		b.Fatalf("get kafka controller: %v", err)
	}
	_ = conn.Close()

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		b.Fatalf("dial kafka controller: %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		b.Fatalf("create kafka topic %q: %v", topic, err)
	}
}
