package distributed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestQueueDepthOptions pins the two producer-backpressure options' state and
// their non-positive handling. Zero is the disabled default for the depth, and
// zero for the interval means "keep the transport default"; neither is a value
// an operator can usefully force, so both are ignored rather than stored.
func TestQueueDepthOptions(t *testing.T) {
	c := &config{}
	WithQueueMaxDepth(64)(c)
	WithQueueDepthSampleInterval(250 * time.Millisecond)(c)
	if c.queueMaxDepth != 64 {
		t.Fatalf("queueMaxDepth = %d, want 64", c.queueMaxDepth)
	}
	if c.queueDepthInterval != 250*time.Millisecond {
		t.Fatalf("queueDepthInterval = %v, want 250ms", c.queueDepthInterval)
	}

	WithQueueMaxDepth(0)(c)
	WithQueueMaxDepth(-1)(c)
	WithQueueDepthSampleInterval(0)(c)
	if c.queueMaxDepth != 64 {
		t.Fatalf("queueMaxDepth = %d after non-positive calls, want 64 to survive", c.queueMaxDepth)
	}
	if c.queueDepthInterval != 250*time.Millisecond {
		t.Fatalf("queueDepthInterval = %v after a zero call, want 250ms to survive", c.queueDepthInterval)
	}
}

// TestNewWiresQueueDepthBoundIntoDefaultTransport is the wiring assertion for
// the producer-backpressure path: WithQueueMaxDepth must reach the default
// asynq transport, and that transport must expose engine.NonBlockingTaskQueue
// so engine's FlushOutbox takes the non-blocking branch at all. A dropped
// transport option here would leave the backend compiling and enqueueing
// normally, with the flush back on its deadlocking path — visible only under
// the fan-out the bound exists for.
//
// The queue is filled through the blocking Enqueue; the poll tolerates the
// sampler not having taken its first post-enqueue sample yet. Each successful
// TryEnqueue before that adds another task, so the measured depth is monotonic
// and the poll cannot miss the bound.
func TestNewWiresQueueDepthBoundIntoDefaultTransport(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	b, err := New(rdb.Options().Addr, nil,
		WithQueueMaxDepth(1),
		WithQueueDepthSampleInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = b.transport.Close()
		_ = b.rdb.Close()
	})

	nb, ok := b.Queue().(engine.NonBlockingTaskQueue)
	if !ok {
		t.Fatalf("default transport %T does not implement engine.NonBlockingTaskQueue", b.Queue())
	}

	ctx := context.Background()
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-depth-wiring"),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := b.Queue().Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		err := nb.TryEnqueue(ctx, task)
		if errors.Is(err, engine.ErrQueueFull) {
			return
		}
		if err != nil {
			t.Fatalf("TryEnqueue() error = %v, want nil or engine.ErrQueueFull", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("TryEnqueue never reported engine.ErrQueueFull for a queue at the configured depth")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
