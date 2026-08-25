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

// newKafkaTopic creates the topic and registers its deletion for the end of the
// test.
//
// The deletion is not tidiness. uniqueTopic mints a fresh name on every run and
// test/env keeps the broker's log on the named volume xflow-kafka-data, so a
// topic nobody deletes outlives `compose down` and only disappears with the
// volume. There are ten call sites, which is ten topics per full run, each with
// its own partition directories and its own entry in every metadata refresh.
// newKafkaTopic creates a topic and arranges for it to be deleted when the test
// ends.
//
// The deletion is not tidiness. uniqueTopic mints a fresh name on every run and
// test/env keeps the broker's log on the named volume xflow-kafka-data, so a
// topic nobody deletes outlives `compose down` and goes away only with the
// volume. Ten call sites means ten new topics per full run, each one more
// partition directory on disk and one more entry in every metadata refresh the
// suite's own consumers pay for.
func newKafkaTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Mirror node/trigger_kafka_test.go:createKafkaIntegrationTopic: dial
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
	// Registered only on success: a cleanup armed before CreateTopics would ask
	// the broker to delete a topic that was never created, and report that
	// UNKNOWN_TOPIC_OR_PARTITION as if the deletion had failed.
	t.Cleanup(func() { deleteKafkaTopic(t, brokers, topic) })
}

// deleteKafkaTopic removes a topic created by newKafkaTopic.
//
// It distinguishes two failures, because they mean opposite things. An
// unreachable broker is the environment going away underneath a run that has
// already finished asserting — normal during teardown, so it is logged and the
// test keeps its verdict. A broker that answers and still refuses the delete is
// a real problem: it is also what a broker with delete.topic.enable=false looks
// like, and that one would otherwise leave this helper claiming a cleanup it
// never performed.
func deleteKafkaTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Logf("leaked kafka topic %q: dial broker: %v", topic, err)
		return
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		t.Logf("leaked kafka topic %q: resolve controller: %v", topic, err)
		return
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Logf("leaked kafka topic %q: dial controller: %v", topic, err)
		return
	}
	defer controllerConn.Close()

	if err := controllerConn.DeleteTopics(topic); err != nil {
		t.Errorf("leaked kafka topic %q: broker answered but refused the delete: %v", topic, err)
	}
}

func writeKafkaMessages(t *testing.T, brokers []string, topic string, msgs []kafka.Message) {
	t.Helper()
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  10,
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Retry until topic metadata has propagated (new topics may not be visible
	// to the leader immediately after CreateTopics returns).
	var lastErr error
	for {
		if err := w.WriteMessages(ctx, msgs...); err == nil {
			return
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("write kafka: %v", lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// uniqueTopic returns a timestamped unique topic name to avoid cross-test pollution.
func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
