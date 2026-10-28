package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// The integration tests run against a real Kafka broker. They follow the gating
// of node/trigger/kafka: XFLOW_TEST_KAFKA_BROKERS names the brokers, and with
// XFLOW_REQUIRE_KAFKA_INTEGRATION=1 a missing or unreachable broker fails the
// test instead of skipping it.

// itStep is the polling interval of every wait in these tests.
const itStep = 50 * time.Millisecond

// brokersOrSkip returns the brokers from XFLOW_TEST_KAFKA_BROKERS.
func brokersOrSkip(t *testing.T) []string {
	t.Helper()
	required := os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1"
	raw := os.Getenv("XFLOW_TEST_KAFKA_BROKERS")
	if raw == "" {
		if required {
			t.Fatal("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: XFLOW_TEST_KAFKA_BROKERS not set")
		}
		t.Skip("set XFLOW_TEST_KAFKA_BROKERS to run the real Kafka integration tests")
	}
	var brokers []string
	for part := range strings.SplitSeq(raw, ",") {
		if b := strings.TrimSpace(part); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("XFLOW_TEST_KAFKA_BROKERS did not contain any brokers")
	}
	conn, err := net.DialTimeout("tcp", brokers[0], 3*time.Second)
	if err != nil {
		if required {
			t.Fatalf("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: broker %s unreachable: %v", brokers[0], err)
		}
		t.Skipf("broker %s unreachable: %v", brokers[0], err)
	}
	_ = conn.Close()
	return brokers
}

// uniqueSuffix is a deployment suffix no other test or run shares: "it-" and
// eight random lowercase hex digits. Every topic and consumer group of a test
// derives from it.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return "it-" + hex.EncodeToString(b)
}

// itEnv is the isolated slice of the broker one test works in: a unique
// deployment suffix, so its topics and consumer groups cannot meet any other
// test's or any real deployment's.
type itEnv struct {
	t       *testing.T
	brokers []string
	suffix  string
	names   topicNames
}

func newITEnv(t *testing.T) *itEnv {
	t.Helper()
	e := &itEnv{t: t, brokers: brokersOrSkip(t), suffix: uniqueSuffix(t)}
	var err error
	if e.names, err = newTopicNames(e.suffix); err != nil {
		t.Fatalf("topic names: %v", err)
	}
	// Registered first, so it runs after the transports and consumers the test
	// registers later have been closed.
	t.Cleanup(e.deleteTopics)
	return e
}

// deleteTopics removes the topics this test's suffix names, and nothing else.
// It deletes all four whether or not the test meant to create them: a topic the
// broker auto-created by mistake is this test's leftover too.
func (e *itEnv) deleteTopics() {
	prefix := topicPrefix + "." + e.suffix + "."
	var doomed []string
	for _, name := range e.names.All() {
		if strings.HasPrefix(name, prefix) {
			doomed = append(doomed, name)
		}
	}
	tr := &kafkago.Transport{}
	defer tr.CloseIdleConnections()
	client := &kafkago.Client{Addr: kafkago.TCP(e.brokers...), Timeout: 10 * time.Second, Transport: tr}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := client.DeleteTopics(ctx, &kafkago.DeleteTopicsRequest{Topics: doomed})
	if err != nil {
		e.t.Logf("cleanup: delete topics %v: %v", doomed, err)
		return
	}
	for name, err := range res.Errors {
		if err != nil && !errors.Is(err, kafkago.UnknownTopicOrPartition) {
			e.t.Logf("cleanup: delete topic %s: %v", name, err)
		}
	}
}

// itConfig is the Config the integration tests run with: topics are created by
// EnsureTopics (3 partitions, 1 replica), and retries and shutdown are quick.
func (e *itEnv) itConfig() Config {
	return Config{
		Brokers:           e.brokers,
		CreateTopics:      true,
		Partitions:        3,
		ReplicationFactor: 1,
		MaxRetryBackoff:   300 * time.Millisecond,
		ShutdownTimeout:   5 * time.Second,
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	}
}

// transport builds a Transport on this test's suffix and closes it at the end.
func (e *itEnv) transport(mutate func(*Config), opts ...Option) *Transport {
	e.t.Helper()
	cfg := e.itConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	tr, err := New(cfg, e.suffix, opts...)
	if err != nil {
		e.t.Fatalf("New: %v", err)
	}
	e.t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// ensure runs EnsureTopics on tr and fails the test if it does not succeed.
func (e *itEnv) ensure(tr *Transport) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 60*time.Second)
	defer cancel()
	if err := tr.EnsureTopics(ctx); err != nil {
		e.t.Fatalf("EnsureTopics: %v", err)
	}
}
