package xflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
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

func (s *driftScanningStore) ListStaleRunningExecutions(_ context.Context, owned []namespace.Namespace, _ time.Time, _ store.ExecutionDriftCursor, _ int) ([]store.ExecutionDriftCandidate, error) {
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

// stateStoreWithoutStatusReader hides the engine.ExecutionStatusReader
// capability the backend's StateStore actually has. Embedding the interface
// (not the concrete store) is what hides it: the optional method is not part of
// StateStore, so it cannot be promoted.
type stateStoreWithoutStatusReader struct {
	engine.StateStore
}

// providerWithoutStatusReader delegates everything to a real in-memory backend
// except State(), which returns the capability-stripped wrapper. It models a
// backend that supports the scan but cannot answer an authoritative status
// read.
type providerWithoutStatusReader struct {
	backend.Provider
}

func (p providerWithoutStatusReader) State() engine.StateStore {
	return stateStoreWithoutStatusReader{StateStore: p.Provider.State()}
}

// newAPIServerWithProvider builds a real *apiserver.APIServer over the given
// backend provider, so sdk code that asks api.Backend() sees that provider.
func newAPIServerWithProvider(t *testing.T, provider backend.Provider) *apiserver.APIServer {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: provider})
	if err != nil {
		t.Fatalf("control.NewControlPlane: %v", err)
	}
	api, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return api
}

// Both capabilities are required, and the status reader is asked of the
// backend's StateStore, not of the durable store: a store that can scan plus a
// backend that cannot report authoritative status is still no worker, because
// every candidate would be classified "not found" and never repaired.
func TestServerExecutionDriftRepairAbsentWithoutStatusReader(t *testing.T) {
	st := &driftScanningStore{Store: memstore.New()}
	sc := &serverConfig{executionDriftRepair: &executionDriftRepairConfig{
		period: time.Minute, staleAfter: 5 * time.Minute, batch: 64,
	}}

	nonReader := providerWithoutStatusReader{Provider: backendlocal.New(backendlocal.WithConcurrency(1))}
	if got := newExecutionDriftRepairWorker(st, newAPIServerWithProvider(t, nonReader), sc); got != nil {
		t.Fatal("worker built for a backend whose StateStore is not an engine.ExecutionStatusReader: every candidate would read as not-found")
	}

	// Positive control: the same store with a backend that does answer the
	// status read builds the worker, so the nil above is the capability gate and
	// not a broken fixture.
	reader := backendlocal.New(backendlocal.WithConcurrency(1))
	if got := newExecutionDriftRepairWorker(st, newAPIServerWithProvider(t, reader), sc); got == nil {
		t.Fatal("worker not built with both capabilities present")
	}
}

// Start and Run both drive the worker, and either can be called more than once.
// The worker must still have exactly one Run goroutine: two would scan twice per
// period and race on the sweep cursor.
func TestServerExecutionDriftRepairStartsOnlyOnce(t *testing.T) {
	scanned := make(chan struct{}, 4)
	st := &driftScanningStore{Store: memstore.New(), scanned: scanned}
	srv := newDriftSDKServer(t, st, WithServerExecutionDriftRepair(time.Hour, 5*time.Minute, 64, false))
	if srv.ExecutionDriftRepair() == nil {
		t.Fatal("ExecutionDriftRepair() = nil with the option on and a scanning store")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.startExecutionDriftRepair(ctx)
	srv.startExecutionDriftRepair(ctx)

	select {
	case <-scanned:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker's startup sweep never reached the scanner")
	}
	// Let a hypothetical second goroutine reach the scanner as well before
	// asserting the count. The period is an hour, so a duplicate worker's only
	// scan is its startup sweep.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls, _ := st.state(); calls != 1 {
		t.Fatalf("scanner calls = %d, want 1: the second start must not spawn a second Run goroutine", calls)
	}
}
