//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// integrationTriggerRuntime is a minimal types.TriggerRuntime that records
// emitted events and lets tests wait for a desired count.
type integrationTriggerRuntime struct {
	mu         sync.Mutex
	emits      []*types.TriggerEvent
	emitSignal chan struct{}
}

func newIntegrationTriggerRuntime() *integrationTriggerRuntime {
	return &integrationTriggerRuntime{
		emitSignal: make(chan struct{}, 128),
	}
}

func (r *integrationTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	r.mu.Lock()
	r.emits = append(r.emits, event)
	r.mu.Unlock()
	select {
	case r.emitSignal <- struct{}{}:
	default:
	}
	return "exec-integration", nil
}

func (r *integrationTriggerRuntime) Dedup(_ context.Context, _ string, _ time.Duration) (bool, error) {
	// Always allow — no dedup in integration tests.
	return true, nil
}

func (r *integrationTriggerRuntime) TryLock(_ context.Context, _ string, _ time.Duration) (types.TriggerLock, bool, error) {
	return integrationTriggerLock{}, true, nil
}

func (r *integrationTriggerRuntime) State(_ context.Context, _ string) types.TriggerState { return nil }

func (r *integrationTriggerRuntime) waitForEmitCount(want int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		n := len(r.emits)
		r.mu.Unlock()
		if n >= want {
			return true
		}
		select {
		case <-r.emitSignal:
		case <-deadline.C:
			r.mu.Lock()
			ok := len(r.emits) >= want
			r.mu.Unlock()
			return ok
		}
	}
}

func (r *integrationTriggerRuntime) emitCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.emits)
}

type integrationTriggerLock struct{}

func (integrationTriggerLock) Release(context.Context) error { return nil }

// TestKafkaTriggerRealConsume verifies that KafkaTrigger activates against a
// real Kafka broker and delivers each produced message as a TriggerEvent via
// the runtime Emit callback.
func TestKafkaTriggerRealConsume(t *testing.T) {
	brokers := requireKafka(t)
	topic := uniqueTopic("xflow-kafka-trigger")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 2)

	messages := []kafka.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	writeKafkaMessages(t, brokers, topic, messages)

	tr := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest")

	rt := newIntegrationTriggerRuntime()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-kafka-real",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer sub.Close(context.Background())

	if !rt.waitForEmitCount(len(messages), 20*time.Second) {
		t.Fatalf("emit count = %d, want %d", rt.emitCount(), len(messages))
	}

	rt.mu.Lock()
	events := rt.emits
	rt.mu.Unlock()

	values := map[string]bool{}
	for _, ev := range events {
		if ev.Kind != "kafka" {
			t.Fatalf("event kind = %q, want kafka", ev.Kind)
		}
		if v, ok := ev.Data["value"].(string); ok {
			values[v] = true
		}
	}
	for _, want := range []string{"v1", "v2"} {
		if !values[want] {
			t.Fatalf("missing expected value %q in emitted events; got %v", want, values)
		}
	}
}

// TestKafkaTriggerRealAggregateByPartition verifies that AggregateByPartition
// correctly accumulates messages per partition before emitting batch events.
func TestKafkaTriggerRealAggregateByPartition(t *testing.T) {
	brokers := requireKafka(t)
	topic := uniqueTopic("xflow-kafka-agg")
	group := topic + "-group"
	// Use 1 partition so all messages land in the same aggregator window and
	// topic metadata propagates quickly before writing.
	newKafkaTopic(t, brokers, topic, 1)

	const totalMessages = 10
	msgs := make([]kafka.Message, 0, totalMessages)
	for i := 0; i < totalMessages; i++ {
		msgs = append(msgs, kafka.Message{Key: []byte("k"), Value: []byte("v")})
	}
	writeKafkaMessages(t, brokers, topic, msgs)

	tr := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(5, 50*time.Millisecond)

	rt := newIntegrationTriggerRuntime()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-kafka-agg",
		NodeName:   "kafka-agg",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer sub.Close(context.Background())

	// Wait until we've received a batch event with >= 2 aggregated messages
	// (demonstrates that AggregateByPartition actually batched, not just one message per event).
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var maxBatchSize int

	for {
		rt.mu.Lock()
		emits := rt.emits
		rt.mu.Unlock()

		for _, ev := range emits {
			if ev.Kind == "kafka.batch" {
				// Verify the batch has messages array.
				messagesRaw, ok := ev.Data["messages"]
				if !ok {
					t.Fatalf("kafka.batch event missing messages field: %+v", ev.Data)
				}

				// Extract messages list (type is []map[string]any from kafkaMessageDataList).
				messagesList, ok := messagesRaw.([]map[string]any)
				if !ok {
					// Try []any as a fallback in case of unexpected serialization.
					if anyList, ok := messagesRaw.([]any); ok {
						messagesList = make([]map[string]any, len(anyList))
						for i, m := range anyList {
							if mm, ok := m.(map[string]any); ok {
								messagesList[i] = mm
							}
						}
					} else {
						t.Fatalf("kafka.batch messages field has unexpected type: %T", messagesRaw)
					}
				}

				batchSize := len(messagesList)
				if batchSize > maxBatchSize {
					maxBatchSize = batchSize
				}

				if batchSize >= 2 {
					// Success: found a batch with >= 2 messages (real aggregation).
					return
				}
			}
		}

		select {
		case <-ticker.C:
		case <-deadline.C:
			rt.mu.Lock()
			count := len(rt.emits)
			rt.mu.Unlock()
			t.Fatalf("timed out waiting for kafka.batch with >= 2 messages; got %d emit(s), max batch size was %d", count, maxBatchSize)
		}
	}
}

// replicaKafkaRuntime records the raw Kafka records handled by one trigger
// subscription. In production each subscription is hosted by one activation
// replica on one runner; this test keeps the runtime minimal so it isolates the
// consumer-group partitioning contract from workflow execution cost.
type replicaKafkaRuntime struct {
	mu         sync.Mutex
	partitions map[int]int
	emitSignal chan struct{}
}

func newReplicaKafkaRuntime() *replicaKafkaRuntime {
	return &replicaKafkaRuntime{
		partitions: make(map[int]int),
		emitSignal: make(chan struct{}, 1),
	}
}

func (r *replicaKafkaRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	partition, ok := event.Data["partition"].(int)
	if !ok {
		return "", fmt.Errorf("partition has type %T, want int", event.Data["partition"])
	}
	count := 1
	if value, exists := event.Data["count"]; exists {
		var countOK bool
		count, countOK = value.(int)
		if !countOK {
			return "", fmt.Errorf("count has type %T, want int", value)
		}
	}
	r.mu.Lock()
	r.partitions[partition] += count
	r.mu.Unlock()
	select {
	case r.emitSignal <- struct{}{}:
	default:
	}
	return "exec-replica-integration", nil
}

func (*replicaKafkaRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (*replicaKafkaRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return integrationTriggerLock{}, true, nil
}

func (*replicaKafkaRuntime) State(context.Context, string) types.TriggerState { return nil }

func (r *replicaKafkaRuntime) snapshot() map[int]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[int]int, len(r.partitions))
	for partition, count := range r.partitions {
		out[partition] = count
	}
	return out
}

// TestKafkaTriggerRealReplicasSharePartitions proves the data-plane half of
// activation replicas against a real broker. Three production Kafka trigger
// subscriptions join the same group, Kafka assigns all 18 partitions exactly
// once, every subscription receives work, and every partition's committed
// frontier reaches the number of records actually emitted.
//
// TestReplicatedRemoteTriggerHosting_Memory separately proves that the control
// plane starts one sibling activation on each distinct real runner. Keeping the
// two concerns separate makes failures attributable: this test is Kafka group
// membership/offset behavior, not Redis, SQL, runner scheduling, or WASM speed.
func TestKafkaTriggerRealReplicasSharePartitions(t *testing.T) {
	brokers := requireKafka(t)
	const (
		partitionCount       = 18
		replicaCount         = 3
		messagesPerPartition = 20
		batchSize            = 10
	)

	topic := uniqueTopic("xflow-kafka-replicas")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, partitionCount)
	waitForKafkaPartitionLeaders(t, brokers, topic, partitionCount)

	tr := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		MaxInflight(64).
		AggregateByPartition(batchSize, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	runtimes := make([]*replicaKafkaRuntime, 0, replicaCount)
	subs := make([]types.TriggerSubscription, 0, replicaCount)
	for replica := 0; replica < replicaCount; replica++ {
		runtime := newReplicaKafkaRuntime()
		sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: types.WorkflowID(fmt.Sprintf("wf-kafka-replica-%d", replica)),
			NodeName:   "kafka",
			Params:     tr.RawParams().(map[string]any),
			Runtime:    runtime,
		})
		if err != nil {
			t.Fatalf("activate replica %d: %v", replica, err)
		}
		runtimes = append(runtimes, runtime)
		subs = append(subs, sub)
	}
	defer func() {
		for _, sub := range subs {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
			if err := sub.Close(closeCtx); err != nil {
				t.Errorf("close replica subscription: %v", err)
			}
			closeCancel()
		}
	}()

	assignments := waitForKafkaGroupAssignments(t, brokers, group, topic, replicaCount, partitionCount)
	assertExclusivePartitionCover(t, assignments, partitionCount)

	writeKafkaMessagesByPartition(t, brokers, topic, partitionCount, messagesPerPartition)
	waitForReplicaMessageCount(t, runtimes, partitionCount*messagesPerPartition, 30*time.Second)

	seenOwner := make(map[int]int, partitionCount)
	seenCount := make(map[int]int, partitionCount)
	runtimeAssignmentSizes := make([]int, 0, replicaCount)
	for replica, runtime := range runtimes {
		snapshot := runtime.snapshot()
		if len(snapshot) == 0 {
			t.Fatalf("replica %d received no partitions", replica)
		}
		runtimeAssignmentSizes = append(runtimeAssignmentSizes, len(snapshot))
		for partition, count := range snapshot {
			if owner, exists := seenOwner[partition]; exists {
				t.Fatalf("partition %d was handled by replicas %d and %d after the group became stable", partition, owner, replica)
			}
			seenOwner[partition] = replica
			seenCount[partition] = count
		}
	}
	if len(seenOwner) != partitionCount {
		t.Fatalf("runtime partition cover = %v, want all %d partitions", seenOwner, partitionCount)
	}
	for partition := 0; partition < partitionCount; partition++ {
		if got := seenCount[partition]; got != messagesPerPartition {
			t.Fatalf("partition %d emitted records = %d, want %d", partition, got, messagesPerPartition)
		}
	}

	brokerAssignmentSizes := make([]int, 0, len(assignments))
	for _, assignment := range assignments {
		brokerAssignmentSizes = append(brokerAssignmentSizes, len(assignment))
	}
	sort.Ints(brokerAssignmentSizes)
	sort.Ints(runtimeAssignmentSizes)
	if fmt.Sprint(runtimeAssignmentSizes) != fmt.Sprint(brokerAssignmentSizes) {
		t.Fatalf("runtime assignment sizes = %v, broker reported %v", runtimeAssignmentSizes, brokerAssignmentSizes)
	}

	waitForCommittedPartitionOffsets(t, brokers, group, topic, partitionCount, messagesPerPartition, 20*time.Second)
	t.Logf("three Kafka activation replicas covered %d partitions; broker assignment sizes=%v", partitionCount, brokerAssignmentSizes)
}

func writeKafkaMessagesByPartition(t *testing.T, brokers []string, topic string, partitionCount, messagesPerPartition int) {
	t.Helper()
	for partition := 0; partition < partitionCount; partition++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partition)
		cancel()
		if err != nil {
			t.Fatalf("dial partition %d leader: %v", partition, err)
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatalf("set partition %d write deadline: %v", partition, err)
		}
		messages := make([]kafka.Message, messagesPerPartition)
		for sequence := range messages {
			messages[sequence] = kafka.Message{Value: []byte(fmt.Sprintf("p%02d-m%02d", partition, sequence))}
		}
		if _, err := conn.WriteMessages(messages...); err != nil {
			_ = conn.Close()
			t.Fatalf("write partition %d: %v", partition, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close partition %d producer: %v", partition, err)
		}
	}
}

func waitForKafkaPartitionLeaders(t *testing.T, brokers []string, topic string, partitionCount int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ready := true
		for partition := 0; partition < partitionCount; partition++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partition)
			cancel()
			if err != nil {
				lastErr = err
				ready = false
				break
			}
			_ = conn.Close()
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s did not acquire leaders for all %d partitions: %v", topic, partitionCount, lastErr)
}

func waitForKafkaGroupAssignments(t *testing.T, brokers []string, group, topic string, memberCount, partitionCount int) [][]int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastState string
	var lastAssignments [][]int
	var lastErr error
	for time.Now().Before(deadline) {
		queryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		state, assignments, err := kafkaGroupAssignments(queryCtx, brokers, group, topic)
		cancel()
		lastState, lastAssignments, lastErr = state, assignments, err
		assigned := 0
		for _, partitions := range assignments {
			assigned += len(partitions)
		}
		if err == nil && state == "Stable" && len(assignments) == memberCount && assigned == partitionCount {
			return assignments
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("consumer group did not stabilize: state=%q assignments=%v err=%v", lastState, lastAssignments, lastErr)
	return nil
}

func kafkaGroupAssignments(ctx context.Context, brokers []string, group, topic string) (string, [][]int, error) {
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	coordinator, err := client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key:     group,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return "", nil, err
	}
	if coordinator.Error != nil {
		return "", nil, coordinator.Error
	}
	addr := kafka.TCP(net.JoinHostPort(coordinator.Coordinator.Host, fmt.Sprint(coordinator.Coordinator.Port)))
	response, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		Addr:     addr,
		GroupIDs: []string{group},
	})
	if err != nil {
		return "", nil, err
	}
	if len(response.Groups) != 1 {
		return "", nil, fmt.Errorf("describe groups returned %d groups", len(response.Groups))
	}
	groupState := response.Groups[0]
	if groupState.Error != nil {
		return groupState.GroupState, nil, groupState.Error
	}
	assignments := make([][]int, 0, len(groupState.Members))
	for _, member := range groupState.Members {
		var assigned []int
		for _, assignment := range member.MemberAssignments.Topics {
			if assignment.Topic == topic {
				assigned = append(assigned, assignment.Partitions...)
			}
		}
		sort.Ints(assigned)
		assignments = append(assignments, assigned)
	}
	return groupState.GroupState, assignments, nil
}

func assertExclusivePartitionCover(t *testing.T, assignments [][]int, partitionCount int) {
	t.Helper()
	seen := make(map[int]int, partitionCount)
	for member, partitions := range assignments {
		if len(partitions) == 0 {
			t.Fatalf("group member %d has no partition: %v", member, assignments)
		}
		for _, partition := range partitions {
			if owner, exists := seen[partition]; exists {
				t.Fatalf("partition %d assigned to members %d and %d", partition, owner, member)
			}
			seen[partition] = member
		}
	}
	if len(seen) != partitionCount {
		t.Fatalf("assigned partitions = %v, want exactly %d", seen, partitionCount)
	}
	for partition := 0; partition < partitionCount; partition++ {
		if _, ok := seen[partition]; !ok {
			t.Fatalf("partition %d is unassigned: %v", partition, assignments)
		}
	}
}

func waitForReplicaMessageCount(t *testing.T, runtimes []*replicaKafkaRuntime, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		total := 0
		for _, runtime := range runtimes {
			for _, count := range runtime.snapshot() {
				total += count
			}
		}
		if total >= want {
			if total != want {
				t.Fatalf("replicas emitted %d records, want exactly %d", total, want)
			}
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("replicas emitted %d records, want %d", total, want)
		}
	}
}

func waitForCommittedPartitionOffsets(t *testing.T, brokers []string, group, topic string, partitionCount, want int, timeout time.Duration) {
	t.Helper()
	partitions := make([]int, partitionCount)
	for partition := range partitions {
		partitions[partition] = partition
	}
	deadline := time.Now().Add(timeout)
	var last map[int]int64
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		client := &kafka.Client{Addr: kafka.TCP(brokers...)}
		response, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			GroupID: group,
			Topics:  map[string][]int{topic: partitions},
		})
		cancel()
		lastErr = err
		if err == nil && response.Error == nil {
			last = make(map[int]int64, partitionCount)
			complete := true
			for _, offset := range response.Topics[topic] {
				last[offset.Partition] = offset.CommittedOffset
				if offset.Error != nil || offset.CommittedOffset != int64(want) {
					complete = false
				}
			}
			if len(last) == partitionCount && complete {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("committed offsets did not reach %d on every partition: offsets=%v err=%v", want, last, lastErr)
}
