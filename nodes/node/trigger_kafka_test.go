package node

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestKafkaTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newScriptedKafkaConsumer([]KafkaMessage{{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("one")}})
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := newFakeTriggerRuntime()
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
	defer sub.Close(context.Background())

	if !rt.waitDedup(time.Second) {
		t.Fatal("kafka trigger did not attempt dedup")
	}
	if got := rt.emitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
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
	defer sub.Close(context.Background())

	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.emitCount())
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
