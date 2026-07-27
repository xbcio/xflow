package distributed

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// TestTransientBackendDoesNotStartTimeoutMonitor verifies optimization 2: in
// transient (fire-and-forget) mode BindHandler must not construct or run the
// TimeoutMonitor, since there are no suspend/timeout semantics and its 5s poll
// + ZSET ops are pure overhead.
func TestTransientBackendDoesNotStartTimeoutMonitor(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	backend, err := New(rdb.Options().Addr, nil,
		WithConcurrency(1),
		WithTransientMode(time.Minute, 30*time.Second),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	eng := engine.New(backend.State(), backend.Queue())
	stop := backend.Bind(eng)
	defer stop()

	if backend.timeoutMonitor != nil {
		t.Fatalf("transient backend started TimeoutMonitor = %v, want nil", backend.timeoutMonitor)
	}
}

// TestDefaultBackendStartsTimeoutMonitor confirms default mode is unchanged:
// the monitor is constructed and runs.
func TestDefaultBackendStartsTimeoutMonitor(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	backend, err := New(rdb.Options().Addr, nil, WithConcurrency(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	eng := engine.New(backend.State(), backend.Queue())
	stop := backend.Bind(eng)
	defer stop()

	if backend.timeoutMonitor == nil {
		t.Fatal("default backend did not start TimeoutMonitor, want non-nil")
	}
}
