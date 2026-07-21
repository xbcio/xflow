package distributed

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
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
	wantText := wantErr.Error()
	for _, e := range r.errors {
		if !strings.Contains(e.msg, wantMsg) {
			continue
		}
		for _, a := range e.args {
			switch v := a.(type) {
			case error:
				if errors.Is(v, wantErr) {
					return true
				}
			case string:
				if strings.Contains(v, wantText) {
					return true
				}
			}
		}
	}
	return false
}

// logTextContaining returns the concatenation of all string argument values
// from the first log entry whose message contains wantMsg, and ok=false if
// none is found.
func (r *recordingLogger) logTextContaining(wantMsg string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errors {
		if !strings.Contains(e.msg, wantMsg) {
			continue
		}
		var parts []string
		for _, a := range e.args {
			if s, ok := a.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, " "), true
	}
	return "", false
}

// panickingShutdownObserver simulates an observer that panics so the stop path
// can prove it does not crash shutdown.
type panickingShutdownObserver struct{}

func (panickingShutdownObserver) OnShutdown(ShutdownReport) {
	panic("observer boom")
}

// TestShutdownErrorDeliveredRawToObserver verifies that the observer receives
// the raw close error unchanged, even when it contains credential substrings.
func TestShutdownErrorDeliveredRawToObserver(t *testing.T) {
	transportErr := errors.New("transport close boom: postgres://user:secretPass@db.example.com:5432/xflow?password=dbpass&ak=AKIAIOS&sk=abcd")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return nil }}
	obs := &recordingShutdownObserver{}

	b := newTestBackendWithPool(t, transport, pool)
	b.shutdownObserver = obs

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	r := obs.First()
	if !errors.Is(r.TransportErr, transportErr) {
		t.Fatalf("observer received %v, want raw error %v", r.TransportErr, transportErr)
	}
}

// TestShutdownErrorRedactedInLogger verifies that credentials in close errors
// are masked before reaching the engine.Logger fallback path.
func TestShutdownErrorRedactedInLogger(t *testing.T) {
	transportErr := errors.New("transport close boom: postgres://user:secretPass@db.example.com:5432/xflow?password=dbpass&ak=AKIAIOS&sk=abcd")

	transport := &closeErrAfterStartTransport{closeErr: transportErr}
	pool := &countingPool{closeFn: func(context.Context) error { return nil }}
	logger := &recordingLogger{}

	b := newTestBackendWithPool(t, transport, pool)
	b.logger = logger

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	text, ok := logger.logTextContaining("transport close error")
	if !ok {
		t.Fatal("logger did not record transport close error")
	}
	if !strings.Contains(text, "postgres://***:***@") {
		t.Errorf("logger text did not redact URL credentials: %q", text)
	}
	for _, secret := range []string{"secretPass", "dbpass", "AKIAIOS", "abcd"} {
		if strings.Contains(text, secret) {
			t.Errorf("logger text leaked secret %q: %q", secret, text)
		}
	}
}

// TestShutdownErrorRedactedInStdlibLog verifies the stdlib log fallback also
// redacts credentials when no observer or engine.Logger is configured.
func TestShutdownErrorRedactedInStdlibLog(t *testing.T) {
	transportErr := errors.New("redis close boom: mysql://admin:hunter2@mysql.example.com/xflow?secret=shh")

	transport := &closeErrAfterStartTransport{}
	pool := &countingPool{closeFn: func(context.Context) error { return nil }}

	b := newTestBackendWithPool(t, transport, pool)

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	// Wrap the real Redis client so Close returns the credential-bearing error.
	b.rdb = &closeErrRedisClient{UniversalClient: b.rdb, closeErr: transportErr}

	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	stop()

	out := buf.String()
	if !strings.Contains(out, "mysql://***:***@") {
		t.Errorf("stdlib log did not redact URL credentials: %q", out)
	}
	for _, secret := range []string{"hunter2", "shh"} {
		if strings.Contains(out, secret) {
			t.Errorf("stdlib log leaked secret %q: %q", secret, out)
		}
	}
}

// TestShutdownObserverPanicDoesNotCrashShutdown verifies that a panicking
// observer is recovered and shutdown still releases resources.
func TestShutdownObserverPanicDoesNotCrashShutdown(t *testing.T) {
	transport := &closeErrAfterStartTransport{}
	pool := &countingPool{closeFn: func(context.Context) error { return nil }}
	logger := &recordingLogger{}

	b := newTestBackendWithPool(t, transport, pool)
	b.shutdownObserver = panickingShutdownObserver{}
	b.logger = logger

	eng := engine.New(b.State(), b.Queue())
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("StartBinding error = %v", err)
	}

	stop()

	if got := transport.closes.Load(); got != 1 {
		t.Fatalf("transport.Close calls = %d, want 1", got)
	}
	if !logger.hasError("shutdown observer panicked", errors.New("observer boom")) {
		t.Fatal("logger did not record observer panic")
	}
}
