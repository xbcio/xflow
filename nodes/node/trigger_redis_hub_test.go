package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestRedisHubTriggerDescriptor(t *testing.T) {
	n := RedisHubTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.redis_hub" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestRedisHubTriggerStreamRequiresStreamAndGroup(t *testing.T) {
	_, err := RedisHubTrigger().Mode("stream").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "stream"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing stream/group error")
	}
}

func TestRedisHubTriggerPubSubRequiresChannel(t *testing.T) {
	_, err := RedisHubTrigger().Mode("pubsub").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing channel error")
	}
}

func TestRedisHubTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	orig := newRedisHubConsumer
	consumer := newScriptedRedisHubConsumer([]RedisHubMessage{{ID: "1", Stream: "orders", Payload: []byte("one")}})
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newRedisHubConsumer = orig })

	rt := newFakeTriggerRuntime()
	rt.dedupFunc = func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	}
	tr := RedisHubTrigger().Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(context.Background())

	if !rt.waitDedup(time.Second) {
		t.Fatal("redis hub trigger did not attempt dedup")
	}
	if got := rt.emitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestRedisHubTriggerContinuesAfterEmitError(t *testing.T) {
	orig := newRedisHubConsumer
	consumer := newScriptedRedisHubConsumer([]RedisHubMessage{
		{ID: "1", Stream: "orders", Payload: []byte("one")},
		{ID: "2", Stream: "orders", Payload: []byte("two")},
	})
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newRedisHubConsumer = orig })

	rt := newFakeTriggerRuntime()
	var calls int
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	}
	tr := RedisHubTrigger().Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
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

type scriptedRedisHubConsumer struct {
	ch chan RedisHubMessage
}

func newScriptedRedisHubConsumer(messages []RedisHubMessage) *scriptedRedisHubConsumer {
	ch := make(chan RedisHubMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &scriptedRedisHubConsumer{ch: ch}
}

func (c *scriptedRedisHubConsumer) Messages() <-chan RedisHubMessage { return c.ch }

func (c *scriptedRedisHubConsumer) Close() error {
	close(c.ch)
	return nil
}
