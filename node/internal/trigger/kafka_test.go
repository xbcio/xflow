package trigger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/types"
)

func TestKafkaTriggerDescriptor(t *testing.T) {
	n := KafkaTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.kafka" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestKafkaTriggerRequiresBrokersTopicAndGroup(t *testing.T) {
	_, err := KafkaTrigger().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     map[string]any{},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing brokers/topic/group error")
	}
}

func TestKafkaTriggerDefaultsStartOffsetLatest(t *testing.T) {
	params := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers").RawParams().(map[string]any)
	if got := params["start_offset"]; got != "latest" {
		t.Fatalf("start_offset = %v, want latest", got)
	}
}

func TestKafkaConsumerFactoryBuildsDefaultConsumer(t *testing.T) {
	consumer, err := newKafkaConsumer(KafkaConsumerConfig{
		Brokers:     []string{"127.0.0.1:1"},
		Topic:       "orders",
		Group:       "workers",
		StartOffset: "earliest",
		MaxInflight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if consumer == nil {
		t.Fatal("consumer is nil")
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaTriggerAggregateRawParams(t *testing.T) {
	params := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(50, 250*time.Millisecond).
		RawParams().(map[string]any)

	aggregate, ok := params["aggregate"].(map[string]any)
	if !ok {
		t.Fatalf("aggregate params = %#v, want map[string]any", params["aggregate"])
	}
	if got := aggregate["enabled"]; got != true {
		t.Fatalf("aggregate enabled = %v, want true", got)
	}
	if got := aggregate["by"]; got != "partition" {
		t.Fatalf("aggregate by = %v, want partition", got)
	}
	if got := aggregate["max_size"]; got != 50 {
		t.Fatalf("aggregate max_size = %v, want 50", got)
	}
	if got := aggregate["flush_interval"]; got != "250ms" {
		t.Fatalf("aggregate flush_interval = %v, want 250ms", got)
	}
}

func TestKafkaTriggerLegacyPathEmitsWithoutPreEmitDedup(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")}})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	// P0-1: the legacy single-message path must NOT run a pre-emit Dedup SETNX.
	// A dedup error must never suppress the emit — that was the data-loss bug.
	rt.dedupFunc = func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	}
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	// The message is emitted despite the dedup callback erroring, because the
	// legacy path no longer calls Dedup before Emit.
	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1 (emit must not depend on pre-emit dedup)", rt.emitCount())
	}
	// And Dedup must not have been consulted at all on the emit path.
	if rt.waitDedup(50 * time.Millisecond) {
		t.Fatal("legacy path called Dedup; pre-emit dedup SETNX must be removed (P0-1)")
	}
}

func TestKafkaTriggerContinuesAfterEmitError(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	var calls int
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	}
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers").MaxInflight(1)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.emitCount())
	}
}

func TestKafkaTriggerCommitsMessageAfterEmit(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newCommitRecordingKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(1, time.Second) {
		t.Fatalf("commit count = %d, want 1", consumer.commitCount())
	}
	if got := consumer.committedOffsets(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("committed offsets = %#v, want [1]", got)
	}
}

func TestKafkaTriggerDoesNotCommitMessageWhenEmitErrors(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newCommitRecordingKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("boom")
	}
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.emitCount())
	}
	time.Sleep(20 * time.Millisecond)
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0", got)
	}
}

func TestKafkaTriggerCommitsBatchAfterEmit(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newCommitRecordingKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !consumer.waitForCommitCount(2, time.Second) {
		t.Fatalf("commit count = %d, want 2", consumer.commitCount())
	}
	if got := consumer.committedOffsets(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("committed offsets = %#v, want [1 2]", got)
	}
}

func TestKafkaTriggerDoesNotCommitBatchWhenEmitErrors(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newCommitRecordingKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("boom")
	}
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.emitCount())
	}
	time.Sleep(20 * time.Millisecond)
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0", got)
	}
}

func TestKafkaTriggerConsumesRealKafka(t *testing.T) {
	brokers := kafkaIntegrationBrokers(t)
	topic := fmt.Sprintf("xflow-kafka-trigger-%d", time.Now().UnixNano())
	group := topic + "-group"
	createKafkaIntegrationTopic(t, brokers[0], topic, 2)
	writeKafkaIntegrationMessages(t, brokers, topic, []kafka.Message{
		{Key: []byte("order-1"), Value: []byte("one"), Headers: []kafka.Header{{Key: "source", Value: []byte("test")}}},
		{Key: []byte("order-1"), Value: []byte("two")},
	})

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().Brokers(brokers...).Topic(topic).Group(group).StartOffset("earliest")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(2, 10*time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.emitCount())
	}
	events := emittedKafkaEvents(rt)
	values := map[string]bool{}
	for _, event := range events {
		if event.Kind != "kafka" {
			t.Fatalf("event kind = %q, want kafka", event.Kind)
		}
		if got := event.Data["topic"]; got != topic {
			t.Fatalf("event topic = %v, want %s", got, topic)
		}
		values[event.Data["value"].(string)] = true
	}
	if !values["one"] || !values["two"] {
		t.Fatalf("event values = %#v, want one and two", values)
	}
}

func TestKafkaTriggerSingleEventIncludesMessagesArray(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 3, Offset: 1200, Key: []byte("k1"), Value: []byte("one")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.emitCount())
	}
	events := emittedKafkaEvents(rt)
	messages := kafkaMessagesFromEvent(t, events[0])
	if len(messages) != 1 {
		t.Fatalf("messages len = %d, want 1", len(messages))
	}
	if got := messages[0]["offset"]; got != int64(1200) {
		t.Fatalf("messages[0].offset = %v, want 1200", got)
	}
	if got := events[0].Data["value"]; got != "one" {
		t.Fatalf("event data value = %v, want one", got)
	}
}

func TestKafkaTriggerAggregatesMessagesByPartition(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("p0-1")},
		{Topic: "orders", Partition: 1, Offset: 10, Value: []byte("p1-10")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("p0-2")},
		{Topic: "orders", Partition: 1, Offset: 11, Value: []byte("p1-11")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.emitCount())
	}
	for _, event := range emittedKafkaEvents(rt) {
		if event.Kind != "kafka.batch" {
			t.Fatalf("event kind = %q, want kafka.batch", event.Kind)
		}
		messages := kafkaMessagesFromEvent(t, event)
		if len(messages) != 2 {
			t.Fatalf("messages len = %d, want 2", len(messages))
		}
		partition := messages[0]["partition"]
		for _, msg := range messages {
			if msg["partition"] != partition {
				t.Fatalf("batch mixed partitions: %#v", messages)
			}
		}
		if got := event.Data["partition"]; got != partition {
			t.Fatalf("event partition = %v, want %v", got, partition)
		}
	}
}

func TestKafkaTriggerAggregateFlushesRemainderOnClose(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
		{Topic: "orders", Partition: 0, Offset: 3, Value: []byte("three")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(2, time.Hour)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1 before close", rt.emitCount())
	}
	if err := sub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2 after close", rt.emitCount())
	}

	events := emittedKafkaEvents(rt)
	messages := kafkaMessagesFromEvent(t, events[1])
	if len(messages) != 1 {
		t.Fatalf("remainder messages len = %d, want 1", len(messages))
	}
	if got := messages[0]["offset"]; got != int64(3) {
		t.Fatalf("remainder offset = %v, want 3", got)
	}
}

func TestKafkaTriggerAggregateFlushesByInterval(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	tr := KafkaTrigger().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(100, 20*time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.emitCount())
	}
	events := emittedKafkaEvents(rt)
	if events[0].Kind != "kafka.batch" {
		t.Fatalf("event kind = %q, want kafka.batch", events[0].Kind)
	}
	messages := kafkaMessagesFromEvent(t, events[0])
	if len(messages) != 1 {
		t.Fatalf("messages len = %d, want 1", len(messages))
	}
}

func emittedKafkaEvents(rt *fakeTriggerRuntime) []*types.TriggerEvent {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]*types.TriggerEvent, len(rt.emits))
	copy(out, rt.emits)
	return out
}

func kafkaMessagesFromEvent(t *testing.T, event *types.TriggerEvent) []map[string]any {
	t.Helper()
	raw, ok := event.Data["messages"].([]map[string]any)
	if !ok {
		t.Fatalf("event data messages = %#v, want []map[string]any", event.Data["messages"])
	}
	return raw
}

func kafkaIntegrationBrokers(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("XFLOW_KAFKA_BROKERS")
	if raw == "" {
		t.Skip("set XFLOW_KAFKA_BROKERS to run real Kafka integration test")
	}
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("XFLOW_KAFKA_BROKERS did not contain any brokers")
	}
	return brokers
}

func createKafkaIntegrationTopic(t *testing.T, broker, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = controllerConn.Close() }()
	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func writeKafkaIntegrationMessages(t *testing.T, brokers []string, topic string, messages []kafka.Message) {
	t.Helper()
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireAll,
	}
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		t.Fatal(err)
	}
}

type scriptedKafkaConsumer struct {
	ch chan KafkaMessage
}

func newScriptedKafkaConsumer(messages []KafkaMessage) *scriptedKafkaConsumer {
	ch := make(chan KafkaMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &scriptedKafkaConsumer{ch: ch}
}

func (c *scriptedKafkaConsumer) Messages() <-chan KafkaMessage { return c.ch }

func (c *scriptedKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

type commitRecordingKafkaConsumer struct {
	ch chan KafkaMessage

	mu      sync.Mutex
	commits []KafkaMessage
	notify  chan struct{}
}

func newCommitRecordingKafkaConsumer(messages []KafkaMessage) *commitRecordingKafkaConsumer {
	ch := make(chan KafkaMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &commitRecordingKafkaConsumer{ch: ch, notify: make(chan struct{})}
}

func (c *commitRecordingKafkaConsumer) Messages() <-chan KafkaMessage { return c.ch }

func (c *commitRecordingKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

func (c *commitRecordingKafkaConsumer) CommitMessages(_ context.Context, messages ...KafkaMessage) error {
	c.mu.Lock()
	c.commits = append(c.commits, messages...)
	close(c.notify)
	c.notify = make(chan struct{})
	c.mu.Unlock()
	return nil
}

func (c *commitRecordingKafkaConsumer) waitForCommitCount(n int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		if len(c.commits) >= n {
			c.mu.Unlock()
			return true
		}
		notify := c.notify
		c.mu.Unlock()

		select {
		case <-notify:
		case <-deadline.C:
			return false
		}
	}
}

func (c *commitRecordingKafkaConsumer) commitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.commits)
}

func (c *commitRecordingKafkaConsumer) committedOffsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, 0, len(c.commits))
	for _, msg := range c.commits {
		out = append(out, msg.Offset)
	}
	return out
}

// ---------------------------------------------------------------------------
// Message schema validation tests
// ---------------------------------------------------------------------------

func TestValidateKafkaMessageSchema(t *testing.T) {
	schema := &KafkaMessageSchema{RequiredFields: []string{"user_id", "action"}}
	tests := []struct {
		name  string
		value []byte
		want  bool
	}{
		{"valid", []byte(`{"user_id":"u1","action":"click","extra":true}`), true},
		{"missing_field", []byte(`{"user_id":"u1"}`), false},
		{"empty_body", nil, false},
		{"not_json", []byte(`hello world`), false},
		{"json_array", []byte(`[1,2,3]`), false},
		{"null_value_counts", []byte(`{"user_id":"u1","action":null}`), true}, // key exists
		{"no_schema", []byte(`{}`), true}, // nil schema always passes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema
			if tt.name == "no_schema" {
				s = nil
			}
			got := validateKafkaMessageSchema(KafkaMessage{Value: tt.value}, s)
			if got != tt.want {
				t.Fatalf("validateKafkaMessageSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKafkaMessageSchemaFromParams(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]any
		want   *KafkaMessageSchema
	}{
		{"nil_params", map[string]any{}, nil},
		{"empty_schema", map[string]any{"message_schema": map[string]any{}}, nil},
		{"valid", map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"user_id", "ts"}},
		}, &KafkaMessageSchema{RequiredFields: []string{"user_id", "ts"}}},
		{"not_a_map", map[string]any{"message_schema": "invalid"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := kafkaMessageSchemaFromParams(tt.params)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("got %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("got nil, want %+v", tt.want)
			}
			if len(got.RequiredFields) != len(tt.want.RequiredFields) {
				t.Fatalf("RequiredFields = %v, want %v", got.RequiredFields, tt.want.RequiredFields)
			}
			for i := range got.RequiredFields {
				if got.RequiredFields[i] != tt.want.RequiredFields[i] {
					t.Fatalf("RequiredFields[%d] = %q, want %q", i, got.RequiredFields[i], tt.want.RequiredFields[i])
				}
			}
		})
	}
}

func TestKafkaTriggerMessageSchemaSkipsInvalidMessages(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newCommitRecordingKafkaConsumer([]KafkaMessage{
		{Topic: "events", Partition: 0, Offset: 1, Value: []byte(`{"user_id":"u1","action":"click"}`)},  // valid
		{Topic: "events", Partition: 0, Offset: 2, Value: []byte(`{"user_id":"u2"}`)},                   // missing "action"
		{Topic: "events", Partition: 0, Offset: 3, Value: []byte(`{"user_id":"u3","action":"scroll"}`)}, // valid
	})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
	params := map[string]any{
		"brokers":      []any{"localhost:9092"},
		"topic":        "events",
		"group":        "workers",
		"max_inflight": 1,
		"message_schema": map[string]any{
			"required_fields": []any{"user_id", "action"},
		},
	}
	sub, err := KafkaTrigger().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     params,
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	// All 3 messages should be committed (consumed from Kafka).
	if !consumer.waitForCommitCount(3, time.Second) {
		t.Fatalf("commit count = %d, want 3", consumer.commitCount())
	}
	// Only 2 messages should be emitted (offset 1 and 3; offset 2 skipped).
	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.emitCount())
	}
}
