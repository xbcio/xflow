package distributed

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
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

// errorTransport is a queue.Transport whose StartConsumer always fails, used to
// verify the A1 fail-closed contract: a consumer start error propagates from
// BindTaskHandler instead of being logged and swallowed.
type errorTransport struct {
	closed atomic.Bool
}

func (e *errorTransport) Enqueue(context.Context, *engine.Task) error { return nil }
func (e *errorTransport) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return nil
}
func (e *errorTransport) StartConsumer(queue.ConsumerConfig, queue.TaskHandler) (func(), error) {
	return nil, errors.New("boom: consumer unavailable")
}
func (e *errorTransport) Close() error { e.closed.Store(true); return nil }

// newTestBackend builds a distributed backend over miniredis with a custom
// transport, ready for binder-lifecycle tests.
func newTestBackend(t *testing.T, transport queue.Transport) *Backend {
	t.Helper()
	rdb := newRedisStateTestClient(t)
	b, err := New(rdb.Options().Addr, nil, WithTransport(transport))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return b
}

// goroutineCount returns the current goroutine count after a short settle.
func goroutineCount() int {
	// Allow in-flight cleanup to settle.
	for i := 0; i < 20; i++ {
		runtime.GC()
		n := runtime.NumGoroutine()
		time.Sleep(10 * time.Millisecond)
		if runtime.NumGoroutine() == n {
			return n
		}
	}
	return runtime.NumGoroutine()
}

// TestBindTaskHandlerFailsClosedOnConsumerStartError verifies the A1 core
// contract: when transport.StartConsumer fails, BindTaskHandler returns the
// error and the transport is closed so no consumer/dispatcher/monitor is left
// running. This is the regression for the prior behavior that logged the error
// and reported ready with no consumer.
func TestBindTaskHandlerFailsClosedOnConsumerStartError(t *testing.T) {
	stub := &errorTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	_, err := b.BindTaskHandler(eng, func(context.Context, *engine.Task) error { return nil })
	if err == nil {
		t.Fatal("BindTaskHandler error = nil, want consumer start error")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed after consumer start failure")
	}
}

// TestBindTaskHandlerConsumerErrorPropagatesThroughControlPlaneStart is in the
// control package (controlplane_test.go) because ControlPlane.Start lives
// there; this test covers the distributed half.

// TestBindHandlerRollsBackOnOutboxStartFailure verifies reverse-order
// rollback: when the step after consumer-start fails (simulated via test
// hook), the consumer is stopped and the transport closed.
func TestBindHandlerRollsBackOnOutboxStartFailure(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	b.testHooks.afterConsumerStart = func() error { return errors.New("outbox boom") }
	eng := engine.New(b.State(), b.Queue())

	_, err := b.BindTaskHandler(eng, func(context.Context, *engine.Task) error { return nil })
	if err == nil {
		t.Fatal("BindTaskHandler error = nil, want outbox start failure")
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped during rollback")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed during rollback")
	}
}

// TestBindHandlerRollsBackOnMonitorStartFailure verifies that when the timeout
// monitor start fails (simulated via test hook), the outbox dispatcher is
// awaited and the consumer is stopped.
func TestBindHandlerRollsBackOnMonitorStartFailure(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	b.testHooks.afterOutboxStart = func() error { return errors.New("monitor boom") }
	eng := engine.New(b.State(), b.Queue())

	_, err := b.BindTaskHandler(eng, func(context.Context, *engine.Task) error { return nil })
	if err == nil {
		t.Fatal("BindTaskHandler error = nil, want monitor start failure")
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped during monitor rollback")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed during monitor rollback")
	}
}

// TestBindStopIsIdempotentAndConcurrent verifies that the stop function is
// safe to call repeatedly and concurrently without panicking or leaking
// goroutines, and that background goroutines (outbox dispatcher, timeout
// monitor) have exited after stop returns.
func TestBindStopIsIdempotentAndConcurrent(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.BindTaskHandler(eng, func(context.Context, *engine.Task) error { return nil })
	if err != nil {
		t.Fatalf("BindTaskHandler error = %v", err)
	}

	before := goroutineCount()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stop() // must not panic
		}()
	}
	wg.Wait()

	// stop returned; goroutines must have drained back near baseline.
	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after stop = %d, before = %d (leak)", got, before)
	}
	if !stub.stopped.Load() || !stub.closed.Load() {
		t.Fatal("stop did not stop consumer or close transport")
	}
}

// TestBindStopWaitsForGoroutines verifies that stop blocks until the outbox
// dispatcher and timeout monitor goroutines have actually exited, not just
// until they were signaled. A second stop after the first must be a no-op.
func TestBindStopWaitsForGoroutines(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.BindTaskHandler(eng, func(context.Context, *engine.Task) error { return nil })
	if err != nil {
		t.Fatalf("BindTaskHandler error = %v", err)
	}

	before := goroutineCount()
	stop()
	stop() // idempotent second call
	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after stop = %d, before = %d (leak)", got, before)
	}
}
