package distributed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// recordingShutdownObserver captures ShutdownReports for test assertions.
type recordingShutdownObserver struct {
	mu      sync.Mutex
	reports []ShutdownReport
}

func (r *recordingShutdownObserver) OnShutdown(report ShutdownReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report)
}

func (r *recordingShutdownObserver) Reports() []ShutdownReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ShutdownReport, len(r.reports))
	copy(out, r.reports)
	return out
}

func (r *recordingShutdownObserver) First() ShutdownReport {
	reports := r.Reports()
	if len(reports) == 0 {
		return ShutdownReport{}
	}
	return reports[0]
}

// closeErrAfterStartTransport is a queue.Transport whose StartConsumer succeeds
// but whose Close returns a configurable error. It is used to exercise the
// normal stop path (not the startup rollback path) with close failures.
type closeErrAfterStartTransport struct {
	stubTransport
	closeErr error
	closes   atomic.Int64
}

func (c *closeErrAfterStartTransport) StartConsumer(cfg queue.ConsumerConfig, h queue.TaskHandler) (func(), error) {
	c.consumerCfg = cfg
	c.consumerCalled.Store(true)
	return func() { c.stopped.Store(true) }, nil
}

func (c *closeErrAfterStartTransport) Close() error {
	c.closes.Add(1)
	c.closed.Store(true)
	return c.closeErr
}

// closeErrRedisClient wraps a redis.UniversalClient and returns a configurable
// error from Close, while delegating all other operations to the underlying
// client.
type closeErrRedisClient struct {
	redis.UniversalClient
	closeErr error
	closes   atomic.Int64
}

func (c *closeErrRedisClient) Close() error {
	c.closes.Add(1)
	if c.closeErr != nil {
		return c.closeErr
	}
	return c.UniversalClient.Close()
}

// TestNonConsumerStopReportsCloseErrors verifies that API-only backends report
// transport, Redis, and pool close errors through the configured observer.
func TestNonConsumerStopReportsCloseErrors(t *testing.T) {
	transportErr := errors.New("transport close boom")
	redisErr := errors.New("redis close boom")
	poolErr := errors.New("pool close boom")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return poolErr }}
	obs := &recordingShutdownObserver{}

	rdb := newRedisStateTestClient(t)
	b, err := New(rdb.Options().Addr, nil,
		WithConsumer(false),
		WithTransport(transport),
		WithResourcePool(pool),
		WithShutdownObserver(obs),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	b.rdb = &closeErrRedisClient{UniversalClient: b.rdb, closeErr: redisErr}

	stop := b.nonConsumerStop()
	stop()

	if got := transport.closes.Load(); got != 1 {
		t.Fatalf("transport.Close calls = %d, want 1", got)
	}
	if got := pool.closes.Load(); got != 1 {
		t.Fatalf("ResourcePool.Close calls = %d, want 1", got)
	}

	r := obs.First()
	if !errors.Is(r.TransportErr, transportErr) {
		t.Fatalf("TransportErr = %v, want %v", r.TransportErr, transportErr)
	}
	if !errors.Is(r.RedisErr, redisErr) {
		t.Fatalf("RedisErr = %v, want %v", r.RedisErr, redisErr)
	}
	if !errors.Is(r.PoolErr, poolErr) {
		t.Fatalf("PoolErr = %v, want %v", r.PoolErr, poolErr)
	}
}

// TestConsumerStopReportsCloseErrors verifies that the stop function returned
// by StartBinding reports transport and pool close errors via the observer.
func TestConsumerStopReportsCloseErrors(t *testing.T) {
	transportErr := errors.New("transport close boom")
	poolErr := errors.New("pool close boom")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return poolErr }}
	obs := &recordingShutdownObserver{}

	b := newTestBackendWithPool(t, transport, pool)
	b.shutdownObserver = obs

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	if got := transport.closes.Load(); got != 1 {
		t.Fatalf("transport.Close calls = %d, want 1", got)
	}
	if got := pool.closes.Load(); got != 1 {
		t.Fatalf("ResourcePool.Close calls = %d, want 1", got)
	}

	r := obs.First()
	if !errors.Is(r.TransportErr, transportErr) {
		t.Fatalf("TransportErr = %v, want %v", r.TransportErr, transportErr)
	}
	if !errors.Is(r.PoolErr, poolErr) {
		t.Fatalf("PoolErr = %v, want %v", r.PoolErr, poolErr)
	}
	if r.RedisErr != nil {
		t.Fatalf("RedisErr = %v, want nil", r.RedisErr)
	}
}

// TestShutdownObserverCalledOnce verifies that stop is idempotent: resources
// are closed exactly once and the observer receives exactly one report even
// when stop is called repeatedly and concurrently.
func TestShutdownObserverCalledOnce(t *testing.T) {
	transportErr := errors.New("transport close boom")
	poolErr := errors.New("pool close boom")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return poolErr }}
	obs := &recordingShutdownObserver{}

	b := newTestBackendWithPool(t, transport, pool)
	b.shutdownObserver = obs

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stop()
		}()
	}
	wg.Wait()

	if got := transport.closes.Load(); got != 1 {
		t.Fatalf("transport.Close calls = %d, want 1", got)
	}
	if got := pool.closes.Load(); got != 1 {
		t.Fatalf("ResourcePool.Close calls = %d, want 1", got)
	}
	if got := len(obs.Reports()); got != 1 {
		t.Fatalf("observer reports = %d, want 1", got)
	}
}

// TestNonConsumerStopObserverCalledOnce verifies idempotency for the API-only
// stop path.
func TestNonConsumerStopObserverCalledOnce(t *testing.T) {
	transport := &closeErrAfterStartTransport{}
	obs := &recordingShutdownObserver{}

	rdb := newRedisStateTestClient(t)
	b, err := New(rdb.Options().Addr, nil,
		WithConsumer(false),
		WithTransport(transport),
		WithShutdownObserver(obs),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	stop := b.nonConsumerStop()
	stop()
	stop()
	stop()

	if got := transport.closes.Load(); got != 1 {
		t.Fatalf("transport.Close calls = %d, want 1", got)
	}
	if got := len(obs.Reports()); got != 1 {
		t.Fatalf("observer reports = %d, want 1", got)
	}
}

// TestShutdownFallbackToLogger verifies that when no observer is configured
// but an engine.Logger is installed, close errors are logged through it.
func TestShutdownFallbackToLogger(t *testing.T) {
	transportErr := errors.New("transport close boom")
	poolErr := errors.New("pool close boom")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return poolErr }}
	logger := &recordingLogger{}

	b := newTestBackendWithPool(t, transport, pool)
	b.logger = logger

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	if !logger.hasError("transport close error", transportErr) {
		t.Fatal("logger did not record transport close error")
	}
	if !logger.hasError("resource pool close error", poolErr) {
		t.Fatal("logger did not record resource pool close error")
	}
}

// TestShutdownReportSuccessIsObserved verifies that a successful shutdown
// (no close errors) still emits a report with all-nil errors so observers can
// count clean shutdowns.
func TestShutdownReportSuccessIsObserved(t *testing.T) {
	transport := &closeErrAfterStartTransport{}
	obs := &recordingShutdownObserver{}

	b := newTestBackend(t, transport)
	b.shutdownObserver = obs

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	r := obs.First()
	if r.TransportErr != nil || r.RedisErr != nil || r.PoolErr != nil {
		t.Fatalf("ShutdownReport = %+v, want all nil errors", r)
	}
}

// recordingLogger is a minimal engine.Logger that records Error/Errorf calls.
type recordingLogger struct {
	mu     sync.Mutex
	errors []logEntry
}

type logEntry struct {
	msg  string
	args []any
}

func (r *recordingLogger) Debug(string, ...any)  {}
func (r *recordingLogger) Debugf(string, ...any) {}
func (r *recordingLogger) Info(string, ...any)   {}
func (r *recordingLogger) Infof(string, ...any)  {}
func (r *recordingLogger) Warn(string, ...any)   {}
func (r *recordingLogger) Warnf(string, ...any)  {}
func (r *recordingLogger) Panic(string, ...any)  {}
func (r *recordingLogger) Panicf(string, ...any) {}

func (r *recordingLogger) Error(msg string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, logEntry{msg: msg, args: args})
}

func (r *recordingLogger) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, logEntry{msg: format, args: args})
}

func (r *recordingLogger) hasError(wantMsg string, wantErr error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errors {
		if !contains(e.msg, wantMsg) {
			continue
		}
		for _, a := range e.args {
			if err, ok := a.(error); ok && errors.Is(err, wantErr) {
				return true
			}
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || containsInternal(s, substr))
}

func containsInternal(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
