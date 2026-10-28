package xflow

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
)

// TestNewClusterKafkaRequiresRedis: Kafka only carries the tasks, so a cluster
// that selects it still needs RedisAddr. The error must say so rather than
// build something else.
func TestNewClusterKafkaRequiresRedis(t *testing.T) {
	_, err := NewCluster(ClusterConfig{Kafka: &distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}})
	if err == nil || !strings.Contains(err.Error(), "RedisAddr is required") {
		t.Fatalf("NewCluster(Kafka, no Redis) = %v, want the RedisAddr-is-required error", err)
	}
}

// TestNewClusterRejectsInvalidKafka: the selection is validated inside
// NewCluster, before a broker is contacted, and the error carries the field.
func TestNewClusterRejectsInvalidKafka(t *testing.T) {
	mr := miniredis.RunT(t)
	for _, c := range []struct {
		name string
		cfg  distributed.KafkaConfig
		want string
	}{
		{"no brokers", distributed.KafkaConfig{}, "Brokers"},
		{"broker without port", distributed.KafkaConfig{Brokers: []string{"host"}}, "Brokers[0]"},
		{"partitions without create", distributed.KafkaConfig{Brokers: []string{"h:9092"}, Partitions: 3}, "CreateTopics"},
		{"create without counts", distributed.KafkaConfig{Brokers: []string{"h:9092"}, CreateTopics: true}, "Partitions"},
		{"sasl without credentials", distributed.KafkaConfig{Brokers: []string{"h:9092"}, SASLMechanism: "plain"}, "username and a password"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := c.cfg
			eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr(), Kafka: &cfg})
			if err == nil {
				eng.Stop()
				t.Fatal("NewCluster accepted an invalid Kafka configuration")
			}
			if !strings.Contains(err.Error(), "kafka queue") || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %q, want it to name the kafka queue and %q", err, c.want)
			}
		})
	}
}

// TestNewClusterWithoutKafkaKeepsAsynq: a nil Kafka field changes nothing.
func TestNewClusterWithoutKafkaKeepsAsynq(t *testing.T) {
	mr := miniredis.RunT(t)
	eng, err := NewCluster(ClusterConfig{RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatalf("NewCluster without Kafka: %v", err)
	}
	eng.Stop()
}

// TestNewServerKafkaRequiresRedis: the server must not fall back to the
// in-memory backend and ignore the selection.
func TestNewServerKafkaRequiresRedis(t *testing.T) {
	_, err := NewServer(ServerConfig{},
		WithServerInsecureNoRunnerAuth(),
		WithServerKafkaQueue(distributed.KafkaConfig{Brokers: []string{"127.0.0.1:1"}}))
	if err == nil || !strings.Contains(err.Error(), "KafkaQueue requires a Redis") {
		t.Fatalf("NewServer(Kafka, no Redis) = %v, want the KafkaQueue-requires-Redis error", err)
	}
}

// TestNewServerRejectsInvalidKafkaWithRedis: with Redis present the server
// passes the selection to the backend, which rejects an unusable configuration.
func TestNewServerRejectsInvalidKafkaWithRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	_, err := NewServer(ServerConfig{RedisAddr: mr.Addr()},
		WithServerInsecureNoRunnerAuth(),
		WithServerKafkaQueue(distributed.KafkaConfig{}))
	if err == nil || !strings.Contains(err.Error(), "kafka queue") {
		t.Fatalf("NewServer(empty Kafka config) = %v, want a kafka queue error", err)
	}
}
