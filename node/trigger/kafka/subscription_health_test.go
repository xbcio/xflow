package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// healthConsumer lets the test close the message stream from the outside,
// which is the shape of a real consumer whose reader has stopped: Messages()
// closes, the aggregate reader goroutine returns, and nothing else tells the
// runner that the subscription is gone.
type healthConsumer struct {
	ch        chan Message
	closeOnce sync.Once
}

func (c *healthConsumer) Messages() <-chan Message { return c.ch }

func (c *healthConsumer) Close() error {
	c.closeOnce.Do(func() { close(c.ch) })
	return nil
}

// TestAggregateSubscriptionHealthClosesWhenReaderExits pins the optional
// health surface the activation tracker relies on. Without it, a consumer
// whose stream closes leaves the tracker reporting the activation as hosted
// forever: the control plane then never redelivers it, and only a process
// restart changes that.
func TestAggregateSubscriptionHealthClosesWhenReaderExits(t *testing.T) {
	consumer := &healthConsumer{ch: make(chan Message)}
	in := &types.TriggerActivateInput{
		WorkflowID: "wf-health",
		NodeName:   "kafka",
		Runtime:    triggertest.NewFakeRuntime(),
	}
	cfg := ConsumerConfig{
		MaxInflight: 2,
		Aggregate: AggregateConfig{
			Enabled:       true,
			By:            aggregateByPartition,
			MaxSize:       10,
			FlushInterval: time.Hour,
			Dedup:         aggregateDedupMessage,
		},
	}

	sub := activateAggregate(context.Background(), in, cfg, consumer, nil)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	health, ok := sub.(types.SubscriptionHealth)
	if !ok {
		t.Fatalf("aggregate subscription %T does not implement types.SubscriptionHealth", sub)
	}

	select {
	case <-health.Done():
		t.Fatal("Done fired while the reader was still running")
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the consumer's stream ending (reader closed, assignment
	// revoked, transport failure). The subscription must report that exit.
	if err := consumer.Close(); err != nil {
		t.Fatalf("close consumer: %v", err)
	}

	select {
	case <-health.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not fire after the consumer's message stream closed")
	}
}

// TestPerMessageSubscriptionHealthClosesWhenReaderExits covers the other Kafka
// activation shape. The per-message reader has the same self-exit path, and
// without the health surface its activations would be reported as hosted
// forever after the consumer's stream ends.
func TestPerMessageSubscriptionHealthClosesWhenReaderExits(t *testing.T) {
	consumer := &healthConsumer{ch: make(chan Message)}
	in := &types.TriggerActivateInput{
		WorkflowID: "wf-health",
		NodeName:   "kafka",
		Runtime:    triggertest.NewFakeRuntime(),
	}

	sub := activatePerMessage(context.Background(), in, ConsumerConfig{MaxInflight: 2}, consumer, nil)
	t.Cleanup(func() { _ = sub.Close(context.Background()) })

	health, ok := sub.(types.SubscriptionHealth)
	if !ok {
		t.Fatalf("per-message subscription %T does not implement types.SubscriptionHealth", sub)
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
