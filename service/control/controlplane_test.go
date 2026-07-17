package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/distributed"
	backendmemory "github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/observability/metrics"
)

func TestNewControlPlaneRequiresBackend(t *testing.T) {
	_, err := NewControlPlane(Config{})
	if err == nil {
		t.Fatal("NewControlPlane(Config{}) error = nil, want error for missing Backend")
	}
}

// TestControlPlaneHandlerServesRunnerProtocol verifies that Handler() wires
// the runner-protocol routes. Stage 3 narrowed control.Server.Handler to the
// runner protocol only; workflow/control routes now live in the apiserver
// workflow-control module.
func TestControlPlaneHandlerServesRunnerProtocol(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Shutdown(context.Background()) }()

	req := httptest.NewRequest(http.MethodPost, "/v1/runners/register", nil)
	rec := httptest.NewRecorder()
	cp.Handler().ServeHTTP(rec, req)

	// Empty body -> 400 (invalid JSON), but this proves the route is wired,
	// not 404.
	if rec.Code == http.StatusNotFound {
		t.Fatalf("Handler() did not route /v1/runners/register, got 404")
	}
}

func TestControlPlaneStartStopIsIdempotentSafe(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cp.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestControlPlaneStartReturnsErrorWhenAlreadyStarted(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Shutdown(context.Background()) }()

	if err := cp.Start(context.Background()); !errors.Is(err, ErrControlPlaneStarted) {
		t.Fatalf("second Start() error = %v, want ErrControlPlaneStarted", err)
	}
}

func TestControlPlaneStartReturnsErrorAfterShutdown(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cp.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := cp.Start(context.Background()); !errors.Is(err, ErrControlPlaneStopped) {
		t.Fatalf("Start() after Shutdown error = %v, want ErrControlPlaneStopped", err)
	}
}

func TestNewControlPlaneWiresMetricsIntoAuthAndSweeper(t *testing.T) {
	m := metrics.New()
	cp, err := NewControlPlane(Config{Backend: backendmemory.New(), Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	if cp.httpServer.core.authObserver == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the HTTP auth observer")
	}
	if cp.grpcServer.core.authObserver == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the gRPC auth observer")
	}
	if cp.sweeper.observer == nil {
		t.Fatal("NewControlPlane() did not wire Config.Metrics into the LeaseSweeper observer")
	}
}

func TestNewControlPlaneWiresPollWait(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New(), PollWait: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if cp.httpServer.core.pollWait != 5*time.Second {
		t.Fatalf("httpServer.core.pollWait = %v, want 5s", cp.httpServer.core.pollWait)
	}
	if cp.grpcServer.core.pollWait != 5*time.Second {
		t.Fatalf("grpcServer.core.pollWait = %v, want 5s", cp.grpcServer.core.pollWait)
	}
}

// TestNewControlPlaneActivatesRedisLeaderElection guards against a regression
// where *distributed.Backend only exposed leader election via a
// LeaderElector() getter rather than satisfying backend.LeaderElector itself.
// NewControlPlane detects leader election support via a type assertion on
// cfg.Backend directly (cfg.Backend.(backend.LeaderElector)); if *Backend
// doesn't implement the interface on its own, the assertion silently fails
// and every Redis-backed ControlPlane falls back to backend.AlwaysLeader,
// meaning RedisLeaderElector is never activated and every replica in a
// multi-replica deployment believes itself to be the leader.
func TestNewControlPlaneActivatesRedisLeaderElection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	b, err := distributed.New(mr.Addr(), nil)
	if err != nil {
		t.Fatal(err)
	}

	cp, err := NewControlPlane(Config{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cp.elector.(*distributed.Backend); !ok {
		t.Fatalf("cp.elector = %T, want *distributed.Backend (Redis leader election should activate, not fall back to AlwaysLeader)", cp.elector)
	}
}

func TestControlPlaneIsLeader(t *testing.T) {
	cp, err := NewControlPlane(Config{Backend: backendmemory.New()})
	if err != nil {
		t.Fatal(err)
	}
	if !cp.IsLeader() {
		t.Fatal("IsLeader() = false for memory backend, want true (AlwaysLeader)")
	}
}

type leaderBackend struct {
	*backendmemory.Backend
	elector *countingElector
}

func (b *leaderBackend) Campaign(ctx context.Context) error { return b.elector.Campaign(ctx) }
func (b *leaderBackend) IsLeader() bool                     { return b.elector.IsLeader() }
func (b *leaderBackend) Resign(ctx context.Context) error   { return b.elector.Resign(ctx) }
func (b *leaderBackend) Notify() <-chan bool                { return b.elector.Notify() }

type countingElector struct {
	campaigns atomic.Int64
	leader    atomic.Bool
	notifyCh  chan bool
	mu        sync.Mutex
	resigned  bool
}

func newCountingElector() *countingElector {
	return &countingElector{notifyCh: make(chan bool, 8)}
}

func (e *countingElector) Campaign(context.Context) error {
	e.campaigns.Add(1)
	e.leader.Store(true)
	e.notifyCh <- true
	return nil
}

func (e *countingElector) IsLeader() bool { return e.leader.Load() }

func (e *countingElector) Resign(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.resigned = true
	e.leader.Store(false)
	return nil
}

func (e *countingElector) Notify() <-chan bool { return e.notifyCh }

func TestControlPlaneRecampaignsAfterLeadershipLoss(t *testing.T) {
	elector := newCountingElector()
	cp, err := NewControlPlane(Config{Backend: &leaderBackend{Backend: backendmemory.New(), elector: elector}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Shutdown(context.Background()) }()

	waitForCampaigns(t, elector, 1)
	elector.leader.Store(false)
	elector.notifyCh <- false

	waitForCampaigns(t, elector, 2)
}

func waitForCampaigns(t *testing.T, elector *countingElector, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if elector.campaigns.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Campaign called %d times, want at least %d", elector.campaigns.Load(), want)
}

type blockingClaimReclaimerDirectory struct {
	*MemoryRunnerDirectory
	started chan struct{}
	stopped chan struct{}
}

func (d *blockingClaimReclaimerDirectory) ReclaimExpiredClaims(ctx context.Context) error {
	select {
	case d.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	select {
	case d.stopped <- struct{}{}:
	default:
	}
	return ctx.Err()
}

func TestControlPlaneStartsAndStopsClaimRecoveryLoop(t *testing.T) {
	directory := &blockingClaimReclaimerDirectory{
		MemoryRunnerDirectory: NewMemoryRunnerDirectory(),
		started:               make(chan struct{}, 1),
		stopped:               make(chan struct{}, 1),
	}
	cp, err := NewControlPlane(Config{
		Backend:         backendmemory.New(),
		RunnerDirectory: directory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-directory.started:
	case <-time.After(time.Second):
		t.Fatal("claim recovery did not run immediately after Start")
	}

	if err := cp.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-directory.stopped:
	case <-time.After(time.Second):
		t.Fatal("claim recovery context was not canceled during Shutdown")
	}
}
