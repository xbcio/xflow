package distributed

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"google.golang.org/grpc"
)

// countingPool is a types.ResourcePool probe that records Close calls. It is
// used to assert that every failed-start rollback path in bindHandler releases
// the injected ResourcePool exactly once (regression for the 2026-07-20
// reacceptance finding that transport+rdb were closed but the pool leaked).
type countingPool struct {
	closes  atomic.Int64
	closeFn func(context.Context) error
}

func (c *countingPool) SQL(context.Context, string, string) (*sql.DB, error) {
	return nil, errors.New("countingPool: SQL not implemented")
}

func (c *countingPool) GRPC(context.Context, string, bool, ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, errors.New("countingPool: GRPC not implemented")
}

func (c *countingPool) Close(ctx context.Context) error {
	c.closes.Add(1)
	if c.closeFn != nil {
		return c.closeFn(ctx)
	}
	return nil
}

// newTestBackendWithPool builds a distributed backend over miniredis with a
// custom transport and (optional) injected ResourcePool, ready for rollback
// tests.
func newTestBackendWithPool(t *testing.T, transport queue.Transport, pool *countingPool) *Backend {
	t.Helper()
	rdb := newRedisStateTestClient(t)
	opts := []Option{WithTransport(transport)}
	if pool != nil {
		opts = append(opts, WithResourcePool(pool))
	}
	b, err := New(rdb.Options().Addr, nil, opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return b
}

// assertPoolClosedOnce fails the test if the pool was not closed exactly once.
func assertPoolClosedOnce(t *testing.T, p *countingPool) {
	t.Helper()
	if got := p.closes.Load(); got != 1 {
		t.Fatalf("ResourcePool.Close calls = %d, want 1", got)
	}
}

// assertErrorContains fails the test if the error message does not contain
// the expected substring. Used to verify that the startup error is preserved
// and not masked by a cleanup error.
func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want substring %q", err.Error(), want)
	}
}

// TestStartBindingRollbackClosesPool_OnConsumerStartError asserts that when
// transport.StartConsumer fails, the injected ResourcePool is closed exactly
// once and the startup error remains visible to the caller.
func TestStartBindingRollbackClosesPool_OnConsumerStartError(t *testing.T) {
	stub := &errorTransport{}
	pool := &countingPool{}
	b := newTestBackendWithPool(t, stub, pool)
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
	assertPoolClosedOnce(t, pool)
	assertErrorContains(t, err, "start consumer")
	assertErrorContains(t, err, "boom: consumer unavailable")
}

// TestStartBindingRollbackClosesPool_OnOutboxStartFailure asserts that when
// the after-consumer hook fails, the pool is closed exactly once and the
// startup error is preserved.
func TestStartBindingRollbackClosesPool_OnOutboxStartFailure(t *testing.T) {
	stub := &stubTransport{}
	pool := &countingPool{}
	b := newTestBackendWithPool(t, stub, pool)
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
	assertPoolClosedOnce(t, pool)
	assertErrorContains(t, err, "start outbox dispatcher")
	assertErrorContains(t, err, "outbox boom")
}

// TestStartBindingRollbackClosesPool_OnMonitorStartFailure asserts that when
// the timeout monitor start hook fails, the pool is closed exactly once and
// the startup error is preserved.
func TestStartBindingRollbackClosesPool_OnMonitorStartFailure(t *testing.T) {
	stub := &stubTransport{}
	pool := &countingPool{}
	b := newTestBackendWithPool(t, stub, pool)
	b.testHooks.afterOutboxStart = func() error { return errors.New("monitor boom") }
	eng := engine.New(b.State(), b.Queue())

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
	assertPoolClosedOnce(t, pool)
	assertErrorContains(t, err, "start timeout monitor")
	assertErrorContains(t, err, "monitor boom")
}

// TestStartBindingRollback_PoolCloseErrorDoesNotMaskStartup asserts that when
// the ResourcePool itself returns an error on Close, the other resources are
// still released and both the startup error and the cleanup error are
// visible in the joined error (errors.Join), so the caller can still observe
// the original startup failure.
func TestStartBindingRollback_PoolCloseErrorDoesNotMaskStartup(t *testing.T) {
	stub := &stubTransport{}
	poolErr := errors.New("pool close boom")
	pool := &countingPool{
		closeFn: func(context.Context) error { return poolErr },
	}
	b := newTestBackendWithPool(t, stub, pool)
	b.testHooks.afterConsumerStart = func() error { return errors.New("outbox boom") }
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want outbox start failure")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if got := pool.closes.Load(); got != 1 {
		t.Fatalf("ResourcePool.Close calls = %d, want 1 even when Close errors", got)
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed even when pool.Close errored")
	}
	// Both the startup error and the cleanup error must be visible.
	assertErrorContains(t, err, "start outbox dispatcher")
	assertErrorContains(t, err, "outbox boom")
	assertErrorContains(t, err, "close resource pool")
	assertErrorContains(t, err, "pool close boom")
}

// TestStartBindingRollback_NoPoolIsNoop asserts that when no ResourcePool is
// injected, the rollback path still works (no nil-pointer panic, startup error
// preserved). Guards the nil-check in closeOwnedResources.
func TestStartBindingRollback_NoPoolIsNoop(t *testing.T) {
	stub := &errorTransport{}
	b := newTestBackendWithPool(t, stub, nil)
	eng := engine.New(b.State(), b.Queue())

	stop, err := b.StartBinding(eng)
	if err == nil {
		t.Fatal("StartBinding error = nil, want consumer start error")
	}
	if stop != nil {
		t.Fatal("StartBinding stop = non-nil, want nil on error")
	}
	if !stub.closed.Load() {
		t.Fatal("transport was not closed when no pool was injected")
	}
	assertErrorContains(t, err, "start consumer")
}
