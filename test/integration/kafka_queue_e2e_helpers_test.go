//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/namespace"
)

// kafkaE2EEnv is the isolation unit of one Redis+Kafka end-to-end test.
//
// Everything the test creates carries one random deployment suffix: the owned
// namespace (and therefore the Kafka topics xflow.<suffix>.task.* and consumer
// groups xflow.<suffix>.consumer.*), the Redis execution keys
// (xflow:ns:<suffix>:...) and, for the Asynq baseline, the suffixed queues. The
// cleanup deletes by that suffix only; it never flushes a database.
type kafkaE2EEnv struct {
	addr    string
	brokers []string
	ns      namespace.Namespace
	suffix  string
	// kafkaUsed is set once any node of this env is built on the Kafka transport,
	// so the baseline test does not touch the broker at teardown.
	kafkaUsed bool
}

// newKafkaE2EEnv gates on the real Redis and Kafka and mints the deployment
// identity. The cleanup is registered before any backend exists, so it runs
// after every backend's own cleanup (t.Cleanup is LIFO).
func newKafkaE2EEnv(t *testing.T) *kafkaE2EEnv {
	t.Helper()
	env := &kafkaE2EEnv{addr: requireRedis(t), brokers: requireKafka(t)}
	env.suffix = "e2e-" + uniqueSuffix(t)
	env.ns = namespace.Namespace(env.suffix)
	t.Cleanup(func() { env.cleanup(t) })
	return env
}

// ctx returns a context carrying the env namespace, bounded by d. Redis keys are
// namespace-scoped, so every submit, poll and signal must use a context built
// here or the engine reads a different (default) namespace.
func (e *kafkaE2EEnv) ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespace.WithNamespace(context.Background(), e.ns), d)
	t.Cleanup(cancel)
	return ctx
}

// pollUntil re-evaluates cond every 25ms until it reports true, fails the test
// on a cond error, and fails with what on ctx expiry. It replaces bare sleeps.
func pollUntil(ctx context.Context, t *testing.T, what string, cond func() (bool, error)) {
	t.Helper()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := cond()
		if err != nil {
			t.Fatalf("waiting for %s: %v", what, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s: %v", what, ctx.Err())
		case <-tick.C:
		}
	}
}

// topics returns the four topics of this deployment, in the order the transport
// names them (internal/queue/kafka/names.go).
func (e *kafkaE2EEnv) topics() []string {
	var out []string
	for _, k := range []string{"default", "batch", "delay", "dlq"} {
		out = append(out, "xflow."+e.suffix+".task."+k)
	}
	return out
}

// groups returns the consumer groups of this deployment (dlq has none).
func (e *kafkaE2EEnv) groups() []string {
	var out []string
	for _, k := range []string{"default", "batch", "delay"} {
		out = append(out, "xflow."+e.suffix+".consumer."+k)
	}
	return out
}

// e2ePartitions is the partition count every E2E deployment topic is created with.
const e2ePartitions = 3

// topicRecordCount returns how many records the topic's partitions hold now
// (last offset minus first offset, summed). Taken before and after a run, the
// difference proves tasks travelled through that topic and not another path.
func topicRecordCount(t *testing.T, env *kafkaE2EEnv, topic string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var total int64
	// A topic created a moment ago answers NotLeaderForPartition or
	// LeaderNotAvailable until its leader metadata has propagated, so those two
	// are retried; any other error is a real failure.
	pollUntil(ctx, t, "offsets of "+topic, func() (bool, error) {
		n, err := listTopicRecords(ctx, env, topic)
		if errors.Is(err, kafka.NotLeaderForPartition) || errors.Is(err, kafka.LeaderNotAvailable) {
			return false, nil
		}
		total = n
		return true, err
	})
	return total
}

func listTopicRecords(ctx context.Context, env *kafkaE2EEnv, topic string) (int64, error) {
	cl := &kafka.Client{Addr: kafka.TCP(env.brokers...), Timeout: 10 * time.Second}
	var reqs []kafka.OffsetRequest
	for p := 0; p < e2ePartitions; p++ {
		reqs = append(reqs, kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p))
	}
	resp, err := cl.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: cl.Addr, Topics: map[string][]kafka.OffsetRequest{topic: reqs}})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, po := range resp.Topics[topic] {
		if po.Error != nil {
			return 0, po.Error
		}
		total += po.LastOffset - po.FirstOffset
	}
	return total, nil
}

// cleanup deletes exactly this env's artifacts. Backends have already been
// stopped by their own cleanups (registered later, so run earlier).
func (e *kafkaE2EEnv) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if e.kafkaUsed {
		e.cleanupKafka(ctx, t)
	}
	e.cleanupRedis(ctx, t)
}

func (e *kafkaE2EEnv) cleanupKafka(ctx context.Context, t *testing.T) {
	t.Helper()
	cl := &kafka.Client{Addr: kafka.TCP(e.brokers...), Timeout: 30 * time.Second}
	// Groups first: a group can only be deleted while it has no members, and the
	// backends are already closed. A refusal is logged, not failed, because the
	// group is then merely an empty record that expires with its offsets.
	gr, err := cl.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{Addr: cl.Addr, GroupIDs: e.groups()})
	if err != nil {
		t.Logf("leaked kafka consumer groups %v: %v", e.groups(), err)
	} else {
		for id, gerr := range gr.Errors {
			if gerr != nil && !errors.Is(gerr, kafka.GroupIdNotFound) {
				t.Logf("leaked kafka consumer group %q: %v", id, gerr)
			}
		}
	}
	tr, err := cl.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Addr: cl.Addr, Topics: e.topics()})
	if err != nil {
		t.Errorf("delete kafka topics %v: %v", e.topics(), err)
		return
	}
	for topic, terr := range tr.Errors {
		if terr != nil && !errors.Is(terr, kafka.UnknownTopicOrPartition) {
			t.Errorf("leaked kafka topic %q: %v", topic, terr)
		}
	}
}

// cleanupRedis removes the keys whose name contains this env's random suffix and
// the suffix's entry in the append-only namespace set. It never flushes: the
// suffix is unique per test, so the pattern cannot match another test's keys.
func (e *kafkaE2EEnv) cleanupRedis(ctx context.Context, t *testing.T) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: e.addr})
	defer func() { _ = rdb.Close() }()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*"+e.suffix+"*", 500).Result()
		if err != nil {
			t.Errorf("scan redis keys for %s: %v", e.suffix, err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("del redis keys for %s: %v", e.suffix, err)
				return
			}
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if err := rdb.SRem(ctx, "xflow:namespaces", e.suffix).Err(); err != nil {
		t.Errorf("srem xflow:namespaces %s: %v", e.suffix, err)
	}
	// Asynq registers every queue it has seen in the shared asynq:queues set.
	// Only the members that carry this env's suffix are removed.
	queues, err := rdb.SMembers(ctx, "asynq:queues").Result()
	if err != nil {
		t.Errorf("smembers asynq:queues: %v", err)
		return
	}
	for _, q := range queues {
		if strings.Contains(q, e.suffix) {
			if err := rdb.SRem(ctx, "asynq:queues", q).Err(); err != nil {
				t.Errorf("srem asynq:queues %s: %v", q, err)
			}
		}
	}
}
