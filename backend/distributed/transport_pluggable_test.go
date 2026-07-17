package distributed

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/queue"
	"github.com/xbcio/xflow/engine"
)

// stubTransport is a non-Asynq queue.Transport used to prove the distributed
// backend is broker-agnostic: WithTransport injects it and the backend drives
// it through the same Producer/Consumer surface Asynq uses.
type stubTransport struct {
	enqueued       atomic.Int64
	consumerCfg    queue.ConsumerConfig
	consumerCalled atomic.Bool
	stopped        atomic.Bool
	closed         atomic.Bool
}

func (s *stubTransport) Enqueue(context.Context, *engine.Task) error {
	s.enqueued.Add(1)
	return nil
}

func (s *stubTransport) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	s.enqueued.Add(1)
	return nil
}

func (s *stubTransport) StartConsumer(cfg queue.ConsumerConfig, _ queue.TaskHandler) (func(), error) {
	s.consumerCfg = cfg
	s.consumerCalled.Store(true)
	return func() { s.stopped.Store(true) }, nil
}

func (s *stubTransport) Close() error {
	s.closed.Store(true)
	return nil
}

// TestWithTransportSwapsBroker verifies the phase-1 goal: a custom transport
// fully replaces Asynq. The backend must expose the injected transport as its
// Queue, start its consumer with the configured concurrency/transient flags,
// and close it on shutdown.
func TestWithTransportSwapsBroker(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	stub := &stubTransport{}

	b, err := New(rdb.Options().Addr, nil,
		WithConcurrency(7),
		WithTransport(stub),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Queue() must return the injected transport, not an Asynq queue.
	if b.Queue() != queue.Transport(stub) {
		t.Fatalf("Queue() did not return the injected transport")
	}

	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)

	if !stub.consumerCalled.Load() {
		t.Fatal("Bind did not start the injected transport's consumer")
	}
	if stub.consumerCfg.Concurrency != 7 {
		t.Fatalf("consumer concurrency = %d, want 7", stub.consumerCfg.Concurrency)
	}

	// Producer path routes through the injected transport.
	if err := b.Queue().Enqueue(context.Background(), &engine.Task{}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if got := stub.enqueued.Load(); got != 1 {
		t.Fatalf("enqueued = %d, want 1", got)
	}

	stop()
	if !stub.stopped.Load() {
		t.Fatal("stop did not stop the injected transport's consumer")
	}
	if !stub.closed.Load() {
		t.Fatal("stop did not close the injected transport")
	}
}
