package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func TestKafkaTriggerDescriptor(t *testing.T) {
	n := New()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.kafka" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestKafkaTriggerRequiresBrokersTopicAndGroup(t *testing.T) {
	_, err := New().Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     map[string]any{},
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing brokers/topic/group error")
	}
}

func TestKafkaTriggerDefaultsStartOffsetLatest(t *testing.T) {
	params := New().Brokers("localhost:9092").Topic("orders").Group("workers").RawParams().(map[string]any)
	if got := params["start_offset"]; got != "latest" {
		t.Fatalf("start_offset = %v, want latest", got)
	}
}

func TestKafkaConsumerFactoryBuildsDefaultConsumer(t *testing.T) {
	consumer, err := newConsumer(ConsumerConfig{
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
	params := New().
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
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")}})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	// P0-1: the legacy single-message path must NOT run a pre-emit Dedup SETNX.
	// A dedup error must never suppress the emit — that was the data-loss bug.
	rt.SetDedupFunc(func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	})
	tr := New().Brokers("localhost:9092").Topic("orders").Group("workers")
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
	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1 (emit must not depend on pre-emit dedup)", rt.EmitCount())
	}
	// And Dedup must not have been consulted at all on the emit path.
	if rt.WaitDedup(50 * time.Millisecond) {
		t.Fatal("legacy path called Dedup; pre-emit dedup SETNX must be removed (P0-1)")
	}
}

func TestKafkaTriggerContinuesAfterEmitError(t *testing.T) {
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	var calls int
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	})
	tr := New().Brokers("localhost:9092").Topic("orders").Group("workers").MaxInflight(1)
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

	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.EmitCount())
	}
}

func TestKafkaTriggerCommitsMessageAfterEmit(t *testing.T) {
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().Brokers("localhost:9092").Topic("orders").Group("workers")
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
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("boom")
	})
	tr := New().Brokers("localhost:9092").Topic("orders").Group("workers")
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

	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.EmitCount())
	}
	time.Sleep(20 * time.Millisecond)
	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0", got)
	}
}

func TestKafkaTriggerCommitsBatchAfterEmit(t *testing.T) {
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().
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
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", errors.New("boom")
	})
	tr := New().
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

	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.EmitCount())
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
	writeKafkaIntegrationMessages(t, brokers, topic, []kafkago.Message{
		{Key: []byte("order-1"), Value: []byte("one"), Headers: []kafkago.Header{{Key: "source", Value: []byte("test")}}},
		{Key: []byte("order-1"), Value: []byte("two")},
	})

	rt := triggertest.NewFakeRuntime()
	tr := New().Brokers(brokers...).Topic(topic).Group(group).StartOffset("earliest")
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

	if !rt.WaitForEmitCount(2, 10*time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.EmitCount())
	}
	events := rt.Events()
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
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{
		{Topic: "orders", Partition: 3, Offset: 1200, Key: []byte("k1"), Value: []byte("one")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().Brokers("localhost:9092").Topic("orders").Group("workers")
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

	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.EmitCount())
	}
	events := rt.Events()
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
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("p0-1")},
		{Topic: "orders", Partition: 1, Offset: 10, Value: []byte("p1-10")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("p0-2")},
		{Topic: "orders", Partition: 1, Offset: 11, Value: []byte("p1-11")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().
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

	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.EmitCount())
	}
	for _, event := range rt.Events() {
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
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("two")},
		{Topic: "orders", Partition: 0, Offset: 3, Value: []byte("three")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().
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

	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1 before close", rt.EmitCount())
	}
	if err := sub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2 after close", rt.EmitCount())
	}

	events := rt.Events()
	messages := kafkaMessagesFromEvent(t, events[1])
	if len(messages) != 1 {
		t.Fatalf("remainder messages len = %d, want 1", len(messages))
	}
	if got := messages[0]["offset"]; got != int64(3) {
		t.Fatalf("remainder offset = %v, want 3", got)
	}
}

func TestKafkaTriggerAggregateFlushesByInterval(t *testing.T) {
	orig := newConsumer
	consumer := newScriptedKafkaConsumer([]Message{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	tr := New().
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

	if !rt.WaitForEmitCount(1, time.Second) {
		t.Fatalf("emit count = %d, want 1", rt.EmitCount())
	}
	events := rt.Events()
	if events[0].Kind != "kafka.batch" {
		t.Fatalf("event kind = %q, want kafka.batch", events[0].Kind)
	}
	messages := kafkaMessagesFromEvent(t, events[0])
	if len(messages) != 1 {
		t.Fatalf("messages len = %d, want 1", len(messages))
	}
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
	// XFLOW_TEST_KAFKA_BROKERS, not XFLOW_KAFKA_BROKERS. This helper predates the
	// repo's convention by a week (e1ce28e added it; aef97c4 standardised the
	// name in the integration harness) and was never updated, so
	// TestKafkaTriggerConsumesRealKafka — the only test in this package that
	// drives a live broker end to end, all 18 others use fakes — has skipped on
	// every machine and every CI run since it was written.
	//
	// Renaming makes it runnable, not run: this package is in the plain
	// `make test` set, and CI only stands up Kafka for ./test/integration. Under
	// XFLOW_REQUIRE_KAFKA_INTEGRATION=1 the skip escalates, so a harness that
	// does provide a broker cannot mistake the skip for a pass.
	raw := os.Getenv("XFLOW_TEST_KAFKA_BROKERS")
	if raw == "" {
		if os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: XFLOW_TEST_KAFKA_BROKERS not set")
		}
		t.Skip("set XFLOW_TEST_KAFKA_BROKERS to run real Kafka integration test")
	}
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("XFLOW_TEST_KAFKA_BROKERS did not contain any brokers")
	}
	return brokers
}

func createKafkaIntegrationTopic(t *testing.T, broker, topic string, partitions int) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	controllerConn, err := kafkago.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = controllerConn.Close() }()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func writeKafkaIntegrationMessages(t *testing.T, brokers []string, topic string, messages []kafkago.Message) {
	t.Helper()
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafkago.RequireAll,
	}
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		t.Fatal(err)
	}
}

type scriptedKafkaConsumer struct {
	ch chan Message
}

func newScriptedKafkaConsumer(messages []Message) *scriptedKafkaConsumer {
	ch := make(chan Message, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &scriptedKafkaConsumer{ch: ch}
}

func (c *scriptedKafkaConsumer) Messages() <-chan Message { return c.ch }

func (c *scriptedKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

type commitRecordingKafkaConsumer struct {
	ch chan Message

	mu      sync.Mutex
	commits []Message
	notify  chan struct{}
}

func newCommitRecordingKafkaConsumer(messages []Message) *commitRecordingKafkaConsumer {
	ch := make(chan Message, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &commitRecordingKafkaConsumer{ch: ch, notify: make(chan struct{})}
}

func (c *commitRecordingKafkaConsumer) Messages() <-chan Message { return c.ch }

func (c *commitRecordingKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

func (c *commitRecordingKafkaConsumer) CommitMessages(_ context.Context, messages ...Message) error {
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
	schema := &MessageSchema{RequiredFields: []string{"user_id", "action"}}
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
		{"no_schema", []byte(`{}`), true},                                     // nil schema always passes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema
			if tt.name == "no_schema" {
				s = nil
			}
			got := validateMessageSchema(Message{Value: tt.value}, s)
			if got != tt.want {
				t.Fatalf("validateMessageSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKafkaMessageSchemaFromParams(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		want    *MessageSchema
		wantErr bool
	}{
		{name: "nil_params", params: map[string]any{}},
		{name: "empty_schema", params: map[string]any{"message_schema": map[string]any{}}},
		{name: "valid", params: map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"user_id", "ts"}},
		}, want: &MessageSchema{RequiredFields: []string{"user_id", "ts"}, OnInvalid: onInvalidDiscard}},
		{name: "not_a_map", params: map[string]any{"message_schema": "invalid"}},
		// An omitted on_invalid must resolve to discard, matching the behaviour
		// that shipped before the policy existed.
		{name: "defaults_to_discard", params: map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"a"}},
		}, want: &MessageSchema{RequiredFields: []string{"a"}, OnInvalid: onInvalidDiscard}},
		{name: "fail_policy", params: map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"a"}, "on_invalid": "fail"},
		}, want: &MessageSchema{RequiredFields: []string{"a"}, OnInvalid: onInvalidFail}},
		{name: "dead_letter_policy", params: map[string]any{
			"message_schema": map[string]any{
				"required_fields": []any{"a"}, "on_invalid": "DEAD_LETTER", "dead_letter_topic": "events-dlq",
			},
		}, want: &MessageSchema{
			RequiredFields: []string{"a"}, OnInvalid: onInvalidDeadLetter, DeadLetterTopic: "events-dlq",
		}},
		// The two negative cases are the ones that matter: a config that asked
		// not to lose messages must fail activation rather than silently fall
		// back to the policy that loses them.
		{name: "dead_letter_without_topic_errors", params: map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"a"}, "on_invalid": "dead_letter"},
		}, wantErr: true},
		{name: "unknown_policy_errors", params: map[string]any{
			"message_schema": map[string]any{"required_fields": []any{"a"}, "on_invalid": "ignore"},
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := messageSchemaFromParams(tt.params)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %+v, want an error", got)
				}
				if got != nil {
					t.Errorf("got schema %+v alongside the error; callers must not receive a usable schema", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("got %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("got nil, want %+v", tt.want)
			}
			if got.OnInvalid != tt.want.OnInvalid {
				t.Errorf("OnInvalid = %q, want %q", got.OnInvalid, tt.want.OnInvalid)
			}
			if got.DeadLetterTopic != tt.want.DeadLetterTopic {
				t.Errorf("DeadLetterTopic = %q, want %q", got.DeadLetterTopic, tt.want.DeadLetterTopic)
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
	orig := newConsumer
	consumer := newCommitRecordingKafkaConsumer([]Message{
		{Topic: "events", Partition: 0, Offset: 1, Value: []byte(`{"user_id":"u1","action":"click"}`)},  // valid
		{Topic: "events", Partition: 0, Offset: 2, Value: []byte(`{"user_id":"u2"}`)},                   // missing "action"
		{Topic: "events", Partition: 0, Offset: 3, Value: []byte(`{"user_id":"u3","action":"scroll"}`)}, // valid
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	params := map[string]any{
		"brokers":      []any{"localhost:9092"},
		"topic":        "events",
		"group":        "workers",
		"max_inflight": 1,
		"message_schema": map[string]any{
			"required_fields": []any{"user_id", "action"},
		},
	}
	sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
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
	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want 2", rt.EmitCount())
	}
}
