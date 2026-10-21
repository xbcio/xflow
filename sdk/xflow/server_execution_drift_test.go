package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// driftScanningStore adds the optional store.ExecutionDriftScanner capability
// to an in-memory store. Every other store behavior is memstore's.
type driftScanningStore struct {
	store.Store
	mu      sync.Mutex
	calls   int
	owned   []namespace.Namespace
	scanned chan struct{}
}

var _ store.ExecutionDriftScanner = (*driftScanningStore)(nil)

func (s *driftScanningStore) ListStaleRunningExecutions(_ context.Context, owned []namespace.Namespace, _ time.Time, _ int) ([]store.ExecutionDriftCandidate, error) {
	s.mu.Lock()
	s.calls++
	s.owned = append([]namespace.Namespace(nil), owned...)
	s.mu.Unlock()
	if s.scanned != nil {
		select {
		case s.scanned <- struct{}{}:
		default:
		}
	}
	return nil, nil
}

func (s *driftScanningStore) state() (int, []namespace.Namespace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, append([]namespace.Namespace(nil), s.owned...)
}

func newDriftSDKServer(t *testing.T, st store.Store, opts ...ServerOption) *Server {
	t.Helper()
	srv, err := NewServer(ServerConfig{Store: st}, append([]ServerOption{WithServerInsecureNoRunnerAuth()}, opts...)...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

// The repair worker writes to the durable projection, so an embedder that
// never asked for it must not get one — even when its store could scan.
func TestServerExecutionDriftRepairOffByDefault(t *testing.T) {
	st := &driftScanningStore{Store: memstore.New()}
	srv := newDriftSDKServer(t, st)
	if got := srv.ExecutionDriftRepair(); got != nil {
		t.Fatal("ExecutionDriftRepair() non-nil without WithServerExecutionDriftRepair: the worker writes and must be opt-in")
	}
}

// A store without the scanner capability leaves the option unbuilt rather than
// producing a worker that scans nothing.
func TestServerExecutionDriftRepairAbsentWithoutScanner(t *testing.T) {
	srv := newDriftSDKServer(t, memstore.New(), WithServerExecutionDriftRepair(time.Minute, 5*time.Minute, 64, false))
	if got := srv.ExecutionDriftRepair(); got != nil {
		t.Fatal("ExecutionDriftRepair() non-nil with a store that does not implement store.ExecutionDriftScanner")
	}
}

// The option plus a capable store builds a worker, and the worker scans under
// the server's owned namespace set (the durable table is shared).
func TestServerExecutionDriftRepairScopesToOwnedNamespaces(t *testing.T) {
	st := &driftScanningStore{Store: memstore.New()}
	srv := newDriftSDKServer(t, st,
		WithServerNamespaces([]namespace.Namespace{"local-jack"}),
		WithServerExecutionDriftRepair(time.Minute, 5*time.Minute, 64, false),
	)
	worker := srv.ExecutionDriftRepair()
	if worker == nil {
		t.Fatal("ExecutionDriftRepair() = nil with the option on and a scanning store")
	}
	if repaired := worker.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows with no candidates, want 0", repaired)
	}
	calls, owned := st.state()
	if calls != 1 {
		t.Fatalf("scanner calls = %d, want 1", calls)
	}
	if len(owned) != 1 || owned[0] != "local-jack" {
		t.Fatalf("scanner owned = %v, want [local-jack]", owned)
	}
}

// Start must drive the worker, not merely construct it. A worker that exists
// and is never run repairs nothing while reading as wired from every angle a
// caller can check.
func TestServerStartRunsTheExecutionDriftRepair(t *testing.T) {
	scanned := make(chan struct{}, 1)
	st := &driftScanningStore{Store: memstore.New(), scanned: scanned}
	srv := newDriftSDKServer(t, st, WithServerExecutionDriftRepair(time.Hour, 5*time.Minute, 64, false))
	if srv.ExecutionDriftRepair() == nil {
		t.Fatal("ExecutionDriftRepair() = nil with the option on and a scanning store")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-scanned:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not run the execution drift repair worker: the startup sweep never reached the scanner")
	}
}

// A caller that supplies no metrics still gets a working worker: the observer
// is optional, and a nil one must not panic the sweep.
func TestServerExecutionDriftRepairWithoutMetrics(t *testing.T) {
	st := &driftScanningStore{Store: memstore.New()}
	srv := newDriftSDKServer(t, st, WithServerExecutionDriftRepair(0, 0, 0, true))
	worker := srv.ExecutionDriftRepair()
	if worker == nil {
		t.Fatal("ExecutionDriftRepair() = nil with zero-valued policy knobs (they are documented as defaults)")
	}
	if repaired := worker.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("dry-run sweep repaired %d rows, want 0", repaired)
	}
}
