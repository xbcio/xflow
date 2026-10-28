//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// clusterKafkaTopics and clusterKafkaGroups are the names of the default
// deployment. xflow.NewCluster does not take an owned-namespace set, so unlike
// the distributed.New based tests of this suite it cannot be given a unique
// deployment suffix: its Kafka topics and consumer groups are always these.
// Only the Redis keys (the engine namespace) are made unique per run.
var (
	clusterKafkaTopics = []string{"xflow.task.default", "xflow.task.batch", "xflow.task.delay", "xflow.task.dlq"}
	clusterKafkaGroups = []string{"xflow.consumer.default", "xflow.consumer.batch", "xflow.consumer.delay"}
)

// TestKafkaQueueE2E_SDKClusterConfig runs a minimal workflow to a terminal state
// through xflow.NewCluster with ClusterConfig.Kafka set, and checks that the
// configuration took effect: the deployment's topics were created with the
// requested partition count and the tasks travelled through the default topic.
//
// Because the topic and group names cannot be made unique (see
// clusterKafkaTopics), the test refuses to run when any of the four topics
// already exists, so it never deletes a topic it did not create. It deletes only
// those four topics and three groups, and only after its own engine has stopped.
func TestKafkaQueueE2E_SDKClusterConfig(t *testing.T) {
	env := newKafkaE2EEnv(t)
	requireClusterTopicsAbsent(t, env)
	// AddWorkflow writes registry keys named by a hash of the namespace, which the
	// env's suffix scan does not match; remove exactly this namespace's.
	t.Cleanup(func() { deleteWorkflowRegistryKeys(t, env) })
	// Registered after env's own cleanup, so it runs before the Redis cleanup,
	// and before eng.Stop's cleanup below so the groups have no members left.
	t.Cleanup(func() { deleteClusterKafkaNames(t, env) })

	eng, err := xflow.NewCluster(
		xflow.ClusterConfig{
			RedisAddr: env.addr,
			Kafka: &distributed.KafkaConfig{
				Brokers: env.brokers, CreateTopics: true, Partitions: e2ePartitions, ReplicationFactor: 1,
			},
		},
		xflow.WithNamespace(env.ns),
	)
	if err != nil {
		t.Fatalf("NewCluster with Kafka: %v", err)
	}
	t.Cleanup(eng.Stop)

	ctx := env.ctx(t, 60*time.Second)
	wf := xflow.Workflow("kq-cluster-" + env.suffix).Namespace(string(env.ns))
	start := wf.Node("start", node.Start())
	calc := wf.Node("calc", node.Expr(`{"n": $input.n, "doubled": $input.n * 2}`))
	wf.Connect(start, calc)
	id, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	before := topicRecordCount(t, env, clusterKafkaTopics[0])
	run, err := eng.Invoke(ctx, id, xflow.Start(), map[string]any{"n": 7})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	res, err := eng.Wait(ctx, run)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s error %q, want success", res.Status, res.Error)
	}
	out, ok := res.Output["calc"].(map[string]any)
	if !ok || e2eInt(out["doubled"]) != 14 {
		t.Fatalf("calc output = %v, want doubled=14", res.Output["calc"])
	}

	if delta := topicRecordCount(t, env, clusterKafkaTopics[0]) - before; delta < 2 {
		t.Fatalf("default topic received %d records, want at least 2: the tasks did not travel through Kafka", delta)
	}
	for _, tp := range clusterTopicMetadata(t, env) {
		if tp.Error != nil {
			t.Fatalf("topic %q: %v", tp.Name, tp.Error)
		}
		if len(tp.Partitions) != e2ePartitions {
			t.Fatalf("topic %q has %d partitions, want %d from ClusterConfig.Kafka", tp.Name, len(tp.Partitions), e2ePartitions)
		}
	}
}

// deleteWorkflowRegistryKeys removes the workflow registry keys of env's
// namespace, "xflow:wfreg:v2:{ns:<sha256(namespace)>}:*" (see
// backend/providers/distributed/internal/workflowreg.registryPrefix). The hash
// of a unique namespace cannot match another test's keys.
func deleteWorkflowRegistryKeys(t *testing.T, env *kafkaE2EEnv) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: env.addr})
	defer func() { _ = rdb.Close() }()
	sum := sha256.Sum256([]byte(env.ns))
	pattern := "xflow:wfreg:v2:{ns:" + hex.EncodeToString(sum[:]) + "}:*"
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			t.Errorf("scan %s: %v", pattern, err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("del registry keys: %v", err)
				return
			}
		}
		if cursor = next; cursor == 0 {
			return
		}
	}
}

// clusterTopicMetadata reads the four deployment topics' metadata. The request
// does not allow auto-creation, so asking never brings a topic into being.
func clusterTopicMetadata(t *testing.T, env *kafkaE2EEnv) []kafka.Topic {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A transport of its own per call: kafka.Client's default transport is shared
	// and caches metadata for seconds, including "topic unknown", so the probe
	// before the run would otherwise be served again after the topics exist.
	tr := &kafka.Transport{}
	defer tr.CloseIdleConnections()
	cl := &kafka.Client{Addr: kafka.TCP(env.brokers...), Timeout: 20 * time.Second, Transport: tr}
	resp, err := cl.Metadata(ctx, &kafka.MetadataRequest{Addr: cl.Addr, Topics: clusterKafkaTopics})
	if err != nil {
		t.Fatalf("kafka metadata: %v", err)
	}
	return resp.Topics
}

// requireClusterTopicsAbsent fails the test when one of the default
// deployment's topics already exists: it would belong to someone else, and the
// test would otherwise write into it and delete it.
func requireClusterTopicsAbsent(t *testing.T, env *kafkaE2EEnv) {
	t.Helper()
	for _, tp := range clusterTopicMetadata(t, env) {
		if tp.Error == nil {
			t.Fatalf("kafka topic %q already exists; this test would write to and delete a topic it did not create, so it stops", tp.Name)
		}
		if !errors.Is(tp.Error, kafka.UnknownTopicOrPartition) {
			t.Fatalf("kafka topic %q: unexpected metadata error: %v", tp.Name, tp.Error)
		}
	}
}

// deleteClusterKafkaNames removes the four topics and three groups this test
// created, and nothing else. A group can only be deleted once it has no
// members, so a refusal is logged rather than failed.
func deleteClusterKafkaNames(t *testing.T, env *kafkaE2EEnv) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cl := &kafka.Client{Addr: kafka.TCP(env.brokers...), Timeout: 30 * time.Second}
	gr, err := cl.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{Addr: cl.Addr, GroupIDs: clusterKafkaGroups})
	if err != nil {
		t.Logf("leaked kafka consumer groups %v: %v", clusterKafkaGroups, err)
	} else {
		for gid, gerr := range gr.Errors {
			if gerr != nil && !errors.Is(gerr, kafka.GroupIdNotFound) {
				t.Logf("leaked kafka consumer group %q: %v", gid, gerr)
			}
		}
	}
	tr, err := cl.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Addr: cl.Addr, Topics: clusterKafkaTopics})
	if err != nil {
		t.Errorf("delete kafka topics %v: %v", clusterKafkaTopics, err)
		return
	}
	for topic, terr := range tr.Errors {
		if terr != nil && !errors.Is(terr, kafka.UnknownTopicOrPartition) {
			t.Errorf("leaked kafka topic %q: %v", topic, terr)
		}
	}
}
