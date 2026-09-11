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

// realKafkaBrokers resolves the broker list, skipping when Kafka is
// unreachable. Under XFLOW_REQUIRE_KAFKA_INTEGRATION=1 the skip escalates to a
// failure, matching test/integration/harness.go: everything under
// //go:build perf is invisible to a default `go test ./...`, so a skip here is
// a silent one and a gating run must be able to tell "measured" from "absent".
func realKafkaBrokers(tb testing.TB) []string {
	tb.Helper()
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
		if os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1" {
			tb.Fatalf("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: kafka unavailable at %s: %v", out[0], err)
		}
		tb.Skipf("kafka unavailable: %v", err)
	}
	_ = c.Close()
	return out
}

// createTopic creates a Kafka topic by dialing the controller node directly,
// mirroring the pattern in test/integration/kafka_helpers.go — including that
// helper's cleanup: the base name is timestamped, so without a delete each run
// of this benchmark adds three more permanent topics to the broker's log.
func createTopic(tb testing.TB, broker, topic string, partitions int) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		tb.Fatalf("dial kafka broker: %v", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		tb.Fatalf("get kafka controller: %v", err)
	}
	_ = conn.Close()

	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		tb.Fatalf("dial kafka controller: %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		tb.Fatalf("create kafka topic %q: %v", topic, err)
	}
	// Armed only after the create succeeded — see deleteKafkaTopic in
	// test/integration/kafka_helpers.go for why the two failure modes below are
	// reported differently.
	tb.Cleanup(func() { deleteTopic(tb, broker, topic) })
}

// deleteTopic removes a topic created by createTopic. An unreachable broker is
// logged and tolerated (teardown races a benchmark that has already reported);
// a broker that answers and refuses the delete fails the benchmark, because
// that is what a misconfigured delete.topic.enable=false looks like and it
// would otherwise leave createTopic promising a cleanup it never did.
func deleteTopic(tb testing.TB, broker, topic string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		tb.Logf("leaked kafka topic %q: dial broker: %v", topic, err)
		return
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		tb.Logf("leaked kafka topic %q: resolve controller: %v", topic, err)
		return
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		tb.Logf("leaked kafka topic %q: dial controller: %v", topic, err)
		return
	}
	defer controllerConn.Close()

	if err := controllerConn.DeleteTopics(topic); err != nil {
		tb.Errorf("leaked kafka topic %q: broker answered but refused the delete: %v", topic, err)
	}
}
