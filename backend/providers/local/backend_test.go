package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

func TestQueueCapacityOptionKeepsDefaultAndAllowsSmallerEmbeddedQueues(t *testing.T) {
	defaultBackend := New()
	if got := cap(defaultBackend.queue.ch); got != defaultMemoryQueueCapacity {
		t.Fatalf("default interactive queue capacity = %d, want %d", got, defaultMemoryQueueCapacity)
	}
	if got := cap(defaultBackend.queue.batchCh); got != defaultMemoryQueueCapacity {
		t.Fatalf("default batch queue capacity = %d, want %d", got, defaultMemoryQueueCapacity)
	}

	const capacity = 16
	embeddedBackend := New(WithQueueCapacity(capacity))
	if got := cap(embeddedBackend.queue.ch); got != capacity {
		t.Fatalf("configured interactive queue capacity = %d, want %d", got, capacity)
	}
	if got := cap(embeddedBackend.queue.batchCh); got != capacity {
		t.Fatalf("configured batch queue capacity = %d, want %d", got, capacity)
	}
}

func TestBindHandlerWithEngineDrainsAndStopsOutbox(t *testing.T) {
	backend := New(WithConcurrency(1))
	eng := engine.New(backend.State(), backend.Queue())
	delivered := make(chan *engine.Task, 2)
	stop := backend.bindHandlerWithOutbox(eng, func(_ context.Context, task *engine.Task) error {
		delivered <- task
		return nil
	}, 5*time.Millisecond)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()

	first := &engine.Task{ExecutionID: "exec-memory-outbox", NodeName: "first", NodeIdx: 0, Type: engine.TaskTypeNodeExec}
	backend.state.mu.Lock()
	backend.state.putOutboxLocked(first.ExecutionID, "first", *first, time.Time{})
	backend.state.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case task := <-delivered:
		if task.NodeName != first.NodeName {
			t.Fatalf("delivered node = %q, want %q", task.NodeName, first.NodeName)
		}
	case <-ctx.Done():
		t.Fatalf("durable outbox task was not delivered: %v", ctx.Err())
	}

	stop()
	stopped = true

	second := &engine.Task{ExecutionID: "exec-memory-outbox", NodeName: "second", NodeIdx: 1, Type: engine.TaskTypeNodeExec}
	backend.state.mu.Lock()
	backend.state.putOutboxLocked(second.ExecutionID, "second", *second, time.Time{})
	backend.state.mu.Unlock()

	noDeliveryCtx, noDeliveryCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer noDeliveryCancel()
	select {
	case task := <-delivered:
		t.Fatalf("outbox dispatcher delivered task after stop: %+v", task)
	case <-noDeliveryCtx.Done():
	}
}
