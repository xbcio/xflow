//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node"
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

	tr := node.KafkaTrigger().
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

	tr := node.KafkaTrigger().
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
