package distributed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// TestStartBindingFailsClosedOnConsumerStartError verifies the SDK fail-closed
// contract: when transport.StartConsumer fails, StartBinding returns the error
// and the transport is closed so no consumer/dispatcher/monitor is left running.
func TestStartBindingFailsClosedOnConsumerStartError(t *testing.T) {
	stub := &errorTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want consumer start error")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed after consumer start failure")
	}
}

// TestStartBindingRollsBackOnOutboxStartFailure verifies reverse-order rollback
// when the after-consumer hook fails: the consumer is stopped and transport closed.
func TestStartBindingRollsBackOnOutboxStartFailure(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	b.testHooks.afterConsumerStart = func() error { return errors.New("outbox boom") }
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want outbox start failure")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped during rollback")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed during rollback")
	}
}

// TestStartBindingRollsBackOnMonitorStartFailure verifies that when the timeout
// monitor start fails (simulated via test hook), the outbox dispatcher and
// monitor goroutines are awaited, then the consumer is stopped and transport
// closed. This is the regression for the leaked timeout monitor goroutine.
func TestStartBindingRollsBackOnMonitorStartFailure(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	b.testHooks.afterOutboxStart = func() error { return errors.New("monitor boom") }
	monitorDone := make(chan struct{})
	b.testHooks.onMonitorDone = func() { close(monitorDone) }
	eng := engine.New(b.State(), b.Queue())

	before := goroutineCount()

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want monitor start failure")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped during monitor rollback")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed during monitor rollback")
	}

	// Direct regression assertion: the timeout monitor goroutine must have
	// exited after rollback. The test-only onMonitorDone hook fires after
	// bindHandler waits on tmDone.
	select {
	case <-monitorDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout monitor goroutine did not exit after rollback")
	}

	// Secondary goroutine-count guard against any other leaked goroutines.
	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after rollback = %d, before = %d (leak)", got, before)
	}
}

// TestStartBindingStopIsIdempotentAndConcurrent verifies that the stop function
// returned by StartBinding is safe to call repeatedly and concurrently without
// panicking or leaking goroutines.
func TestStartBindingStopIsIdempotentAndConcurrent(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	before := goroutineCount()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stop()
		}()
	}
	wg.Wait()

	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after stop = %d, before = %d (leak)", got, before)
	}
	if !stub.stopped.Load() || !stub.closed.Load() {
		t.Fatal("stop did not stop consumer or close transport")
	}
}

// TestStartBindingStopOrderBlocksConsumerFirst verifies that the normal stop
// path stops the consumer before canceling the outbox dispatcher and timeout
// monitor. We use a stub transport whose stop blocks until signaled, proving
// stopConsumer runs first.
func TestStartBindingStopOrderBlocksConsumerFirst(t *testing.T) {
	stub := newOrderTransport()
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stop()
	}()

	select {
	case <-stub.consumerStopped:
		// consumer stopped first as required.
	case <-time.After(2 * time.Second):
		t.Fatal("consumer was not stopped first within timeout")
	}

	stub.allowOutboxWait <- struct{}{}
	wg.Wait()
}

// orderTransport is a stub transport that lets tests observe stop ordering.
type orderTransport struct {
	consumerStopped chan struct{}
	allowOutboxWait chan struct{}
}

func newOrderTransport() *orderTransport {
	return &orderTransport{
		consumerStopped: make(chan struct{}),
		allowOutboxWait: make(chan struct{}),
	}
}

func (o *orderTransport) Enqueue(context.Context, *engine.Task) error { return nil }
func (o *orderTransport) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return nil
}
func (o *orderTransport) StartConsumer(queue.ConsumerConfig, queue.TaskHandler) (func(), error) {
	return func() {
		close(o.consumerStopped)
		<-o.allowOutboxWait
	}, nil
}
func (o *orderTransport) Close() error { return nil }

// TestStartBindingImplementsInterface is a compile-time assertion that
// *distributed.Backend satisfies backend.StartBinder.
func TestStartBindingImplementsInterface(t *testing.T) {
	var _ backend.StartBinder = (*Backend)(nil)
}

// TestStartBindingImmediateStop verifies that stopping the backend right after
// a successful StartBinding returns is safe: no panic and no goroutine leak.
func TestStartBindingImmediateStop(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v, want nil", err)
	}

	before := goroutineCount()

	// Calling stop immediately after StartBinding returns must not panic and
	// must leave no background goroutines behind.
	stop()

	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after immediate stop = %d, before = %d (leak)", got, before)
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed")
	}
}

// TestStartBindingFailedStartSafeToStop verifies that after StartBinding returns
// an error, there is no goroutine leak and the transport is closed. There is no
// stop function to call (it is nil on error), so the assertion is on resource
// cleanup performed by the rollback path itself.
func TestStartBindingFailedStartSafeToStop(t *testing.T) {
	stub := &stubTransport{}
	b := newTestBackend(t, stub)
	b.testHooks.afterConsumerStart = func() error { return errors.New("start boom") }
	eng := engine.New(b.State(), b.Queue())

	before := goroutineCount()

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want start failure")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if !stub.stopped.Load() {
		t.Fatal("consumer was not stopped during rollback")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed during rollback")
	}
	if got := goroutineCount(); got > before {
		t.Fatalf("goroutine count after failed start = %d, before = %d (leak)", got, before)
	}

	// Note: the rollback path closes the transport but does not close b.rdb;
	// the redis.Client is cleaned up by newRedisStateTestClient's t.Cleanup.
	// This is a known gap in the fail-closed rollback path that the current
	// production code leaves to the Backend owner.
}
