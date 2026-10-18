package redis

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestRedisSubscriptionHealthClosesWhenReaderExits pins the optional health
// surface for the Redis trigger. The reader loop exits when the consumer's
// message stream ends (or the renewable lock is lost), and without this signal
// the runner keeps reporting the activation as hosted, so the control plane
// never redelivers it.
func TestRedisSubscriptionHealthClosesWhenReaderExits(t *testing.T) {
	orig := newConsumer
	consumer := newBlockingConsumer()
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-health",
		NodeName:   "redis",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	health, ok := sub.(types.SubscriptionHealth)
	if !ok {
		t.Fatalf("redis subscription %T does not implement types.SubscriptionHealth", sub)
	}

	select {
	case <-health.Done():
		t.Fatal("Done fired while the reader was still running")
	case <-time.After(50 * time.Millisecond):
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("close consumer: %v", err)
	}

	select {
	case <-health.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not fire after the consumer's message stream closed")
	}
}
