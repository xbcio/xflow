//go:build soak

// Package soak provides the HA soak harness for the xflow control plane.
//
// It abstracts a multi-replica control-plane cluster (≥2 apiserver+ControlPlane
// instances sharing one Redis backend), multi-runner lifecycle, and a fault
// injector interface covering the ha-soak-plan §4 fault matrix
// (leader kill/restart, Redis failover, network partition, runner kill,
// report response loss, outbox flush failure).
//
// HONESTY / SCOPE: This file ships the harness scaffold only. The smoke test
// (harness_smoke_test.go) brings up two replicas over an in-process miniredis
// and verifies start/stop cleanliness and single-leader convergence. Real
// multi-replica fault injection, Redis HA failover, network partition, and SLO
// quantification are ENVIRONMENT-GATED — they require a real multi-host
// topology (≥2 xflow-server processes + sentinel/cluster Redis + iptables).
// Those concerns are implemented in Task 5.2 (faults.go) and Task 5.3 (slo.go)
// but their execution remains gated behind a real environment. This scaffold
// deliberately does not claim HA is verified.
//
// Build tag: `soak` is a standalone tag, separate from `integration`, so the
// soak harness does not run as part of the default integration suite. Run with:
//
//	go test -tags=soak -race -count=1 ./test/soak/...
package soak

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// DefaultReplicaCount is the minimum cluster size that exercises leader
// election contention (ha-soak-plan §1: ≥2 xflow-server instances).
const DefaultReplicaCount = 2

// defaultLeaderWait is the upper bound for the smoke test to observe one
// replica winning leadership. Real soak runs use TTL-scaled bounds (§6 SLO).
const defaultLeaderWait = 5 * time.Second

// Cluster is a multi-replica control-plane soak cluster: N replicas (each an
// apiserver+ControlPlane over its own distributed.Backend) sharing one Redis
// instance, plus optional runners and an SLO recorder.
//
// The cluster is intentionally self-contained: it owns its Redis (miniredis for
// smoke, or a real address supplied via Options.RedisAddr), its replicas, and
// their lifecycle. Real multi-host deployment is out of scope here — each
// replica is an in-process apiserver+httptest.Server pair, which is sufficient
// to exercise shared-Redis leader election and the harness contract.
type Cluster struct {
	t        testing.TB
	opts     Options
	mr       *miniredis.Miniredis // non-nil when running over an in-process miniredis
	redisAddr string              // resolved Redis address shared by all replicas

	mu       sync.Mutex
	replicas []*Replica
	runners  []*Runner

	slo SLORecorder
}

// Options configures NewCluster.
type Options struct {
	// RedisAddr, when non-empty, points every replica at a real Redis. When
	// empty, NewCluster starts an in-process miniredis (smoke only — miniredis
	// is not a Redis HA substitute; sentinel/cluster failover is ENV-GATED).
	RedisAddr string
	// ReplicaCount is the number of control-plane replicas to start. Defaults
	// to DefaultReplicaCount (2). Values <2 are rejected: a single replica
	// cannot exercise leader-election contention.
	ReplicaCount int
	// Concurrency per replica's task consumer (passed to distributed.WithConcurrency).
	// Defaults to 1.
	Concurrency int
	// SLORecorder receives lifecycle / fault observations. When nil a
	// no-op recorder is installed. The real recorder (counters + histograms)
	// is implemented in Task 5.3 (slo.go); the wire point is here so the
	// harness contract is stable from day one.
	SLORecorder SLORecorder
	// Logger injected into each ControlPlane. Optional.
	Logger engine.Logger
}

// Replica is a single control-plane replica: one distributed.Backend + one
// ControlPlane + one apiserver hosted by an httptest.Server. All replicas in a
// cluster share the same Redis address; leader election (RedisLeaderElector
// over the shared "xflow:leader:control-plane" key) ensures at most one replica
// holds leadership at a time.
type Replica struct {
	cluster *Cluster
	index   int
	backend *distributed.Backend
	cp      *control.ControlPlane
	srv     *apiserver.APIServer
	httpSrv *httptest.Server

	startCtx    context.Context
	startCancel context.CancelFunc

	mu      sync.Mutex
	stopped bool
}

// NewCluster constructs a cluster with the configured number of replicas
// sharing one Redis. It starts miniredis (or probes the supplied RedisAddr)
// but does NOT start the replicas themselves — call Start to bring them up and
// Stop (deferred via t.Cleanup) to tear them down.
//
// NewCluster does not gate on real Redis reachability: when RedisAddr is set
// the caller is responsible for ensuring it is up (mirroring the integration
// harness's requireRedis gate, which the soak caller should run before
// constructing the cluster for real-environment runs).
func NewCluster(t testing.TB, opts Options) (*Cluster, error) {
	t.Helper()
	if opts.ReplicaCount < 2 {
		opts.ReplicaCount = DefaultReplicaCount
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.SLORecorder == nil {
		opts.SLORecorder = noopSLORecorder{}
	}

	c := &Cluster{t: t, opts: opts, slo: opts.SLORecorder}

	if opts.RedisAddr != "" {
		c.redisAddr = opts.RedisAddr
	} else {
		mr, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("soak: start miniredis: %w", err)
		}
		c.mr = mr
		c.redisAddr = mr.Addr()
		t.Cleanup(func() {
			mr.Close()
		})
	}

	return c, nil
}

// RedisAddr returns the Redis address all replicas share. Exposed for tests
// that need to flush keys or assert on Redis state directly.
func (c *Cluster) RedisAddr() string { return c.redisAddr }

// Miniredis returns the in-process miniredis when running smoke, or nil when
// the cluster was constructed against a real RedisAddr. Callers MUST NOT use
// it to fake failover — miniredis has no HA semantics; real Redis failover is
// ENV-GATED.
func (c *Cluster) Miniredis() *miniredis.Miniredis { return c.mr }

// Start brings up all configured replicas and registers their shutdown via
// t.Cleanup. It returns once every replica's apiserver.Start has returned
// (leader election runs asynchronously in the background).
func (c *Cluster) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.replicas) > 0 {
		return errors.New("soak: cluster already started")
	}
	for i := 0; i < c.opts.ReplicaCount; i++ {
		r, err := c.newReplica(ctx, i)
		if err != nil {
			// Best-effort rollback of replicas already started.
			for _, prev := range c.replicas {
				_ = prev.Stop(context.Background())
			}
			c.replicas = nil
			return fmt.Errorf("soak: start replica %d: %w", i, err)
		}
		c.replicas = append(c.replicas, r)
	}
	c.t.Cleanup(func() {
		_ = c.Stop(context.Background())
	})
	return nil
}

// newReplica builds and starts one replica against the shared Redis address.
func (c *Cluster) newReplica(ctx context.Context, index int) (*Replica, error) {
	b, err := distributed.New(c.redisAddr, nil,
		distributed.WithConcurrency(c.opts.Concurrency),
		distributed.WithConsumer(true),
		distributed.WithStateLogger(c.opts.Logger),
	)
	if err != nil {
		return nil, fmt.Errorf("distributed.New: %w", err)
	}

	cp, err := control.NewControlPlane(control.Config{
		Backend: b,
		Logger:  c.opts.Logger,
	})
	if err != nil {
		closeBackendRdb(b)
		return nil, fmt.Errorf("NewControlPlane: %w", err)
	}

	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		closeBackendRdb(b)
		return nil, fmt.Errorf("apiserver.New: %w", err)
	}

	startCtx, startCancel := context.WithCancel(ctx)
	if err := srv.Start(startCtx); err != nil {
		startCancel()
		// On Start failure, cp.Start's bindDispatcher path already closed the
		// transport; the Redis client (b.rdb) was not closed by that path, so
		// close it here to avoid leaking a connection per failed replica.
		closeBackendRdb(b)
		return nil, fmt.Errorf("apiserver.Start: %w", err)
	}

	httpSrv := httptest.NewServer(srv.Handler())
	r := &Replica{
		cluster:      c,
		index:        index,
		backend:      b,
		cp:           cp,
		srv:          srv,
		httpSrv:      httpSrv,
		startCtx:      startCtx,
		startCancel:  startCancel,
	}
	return r, nil
}

// Replicas returns the started replicas. Callers must not mutate the slice.
// Returns an empty slice before Start.
func (c *Cluster) Replicas() []*Replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Replica, len(c.replicas))
	copy(out, c.replicas)
	return out
}

// Leader returns the replica currently holding leadership, or an error if no
// replica is leader within the timeout. It is a polling convenience for tests;
// real soak runs should drive leadership observation through the SLO recorder
// (Task 5.3) rather than this synchronous helper.
func (c *Cluster) Leader(ctx context.Context) (*Replica, error) {
	c.mu.Lock()
	replicas := make([]*Replica, len(c.replicas))
	copy(replicas, c.replicas)
	c.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		leaders := 0
		var leader *Replica
		for _, r := range replicas {
			if r.IsLeader() {
				leaders++
				leader = r
			}
		}
		if leaders == 1 {
			c.slo.LeaderElected(leader.index)
			return leader, nil
		}
		// 0 leaders: election still in progress. >1 leaders: invariant
		// violation (should never happen with RedisLeaderElector; surface it).
		if leaders > 1 {
			return nil, fmt.Errorf("soak: invariant violation — %d replicas claim leadership", leaders)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// Stop tears down all replicas and runners, bounded by ctx. It is idempotent
// and registered as a t.Cleanup by Start, so callers may invoke it explicitly
// for early teardown without risk of double-stop.
func (c *Cluster) Stop(ctx context.Context) error {
	c.mu.Lock()
	replicas := c.replicas
	runners := c.runners
	c.replicas = nil
	c.runners = nil
	c.mu.Unlock()

	var errs []error
	for _, r := range runners {
		if err := r.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("runner %d: %w", r.index, err))
		}
	}
	for _, r := range replicas {
		if err := r.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("replica %d: %w", r.index, err))
		}
	}
	return errors.Join(errs...)
}

// AddRunner attaches a runner to the cluster at replica index `replicaIndex`.
// The runner abstraction is a scaffold in this task: it records the runner
// ID and lifecycle but does not wire a real runnersvc.Runner (Task 5.2 wires
// the production runner client against the replica's HTTP URL). Start is a
// no-op that succeeds so harness callers can compose cleanly today.
func (c *Cluster) AddRunner(replicaIndex int, runnerID string) (*Runner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if replicaIndex < 0 || replicaIndex >= len(c.replicas) {
		return nil, fmt.Errorf("soak: replica index %d out of range (have %d replicas)", replicaIndex, len(c.replicas))
	}
	r := &Runner{
		cluster:     c,
		index:       len(c.runners),
		replica:     c.replicas[replicaIndex],
		runnerID:    runnerID,
	}
	c.runners = append(c.runners, r)
	return r, nil
}

// Runners returns the attached runners. Callers must not mutate the slice.
func (c *Cluster) Runners() []*Runner {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Runner, len(c.runners))
	copy(out, c.runners)
	return out
}

// SLO returns the cluster's SLO recorder.
func (c *Cluster) SLO() SLORecorder { return c.slo }

// --- Replica ---

// Index returns the replica's 0-based index within the cluster.
func (r *Replica) Index() int { return r.index }

// HTTPURL returns the replica's HTTP base URL (workflow control + runner
// protocol). Real multi-host soak would use real addresses; in-process this is
// a httptest.Server URL.
func (r *Replica) HTTPURL() string { return r.httpSrv.URL }

// IsLeader reports whether this replica currently holds leadership. Transparent
// passthrough to ControlPlane.IsLeader (which forwards to RedisLeaderElector).
func (r *Replica) IsLeader() bool { return r.cp.IsLeader() }

// Backend returns the replica's distributed backend. Exposed for fault
// injectors (Task 5.2) that need to manipulate the transport/state directly.
func (r *Replica) Backend() *distributed.Backend { return r.backend }

// ControlPlane returns the replica's ControlPlane. Exposed for fault injectors
// (Task 5.2) and SLO observation hooks.
func (r *Replica) ControlPlane() *control.ControlPlane { return r.cp }

// Stop tears down this replica: cancels its start context, shuts down the
// apiserver (which resigns leadership + unbinds the queue consumer), and closes
// the httptest.Server. Idempotent.
func (r *Replica) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	r.mu.Unlock()

	var errs []error
	if r.startCancel != nil {
		r.startCancel()
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("apiserver.Shutdown: %w", err))
	}
	if r.httpSrv != nil {
		r.httpSrv.Close()
	}
	// r.srv.Shutdown already resigned leadership and unbound the consumer via
	// ControlPlane.Shutdown. The backend's Redis client is closed there too.
	r.cluster.slo.ReplicaStopped(r.index)
	return errors.Join(errs...)
}

// --- Runner ---

// Runner is a (currently stubbed) runner attached to a replica. The real
// runner process wiring (runnersvc.New + protocol.NewClient against the
// replica's HTTP URL) is implemented in Task 5.2; this scaffold captures the
// topology so harness callers can be written against a stable interface today.
type Runner struct {
	cluster  *Cluster
	index    int
	replica  *Replica
	runnerID string

	mu      sync.Mutex
	started bool
	stopped bool
}

// ID returns the runner's configured ID.
func (r *Runner) ID() string { return r.runnerID }

// Replica returns the replica this runner is attached to.
func (r *Runner) Replica() *Replica { return r.replica }

// Start is a stub. Real runner lifecycle wiring is Task 5.2.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("soak: runner already started")
	}
	r.started = true
	r.cluster.slo.RunnerStarted(r.index)
	return nil
}

// Stop is a stub. Real runner lifecycle wiring is Task 5.2.
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil
	}
	r.stopped = true
	r.cluster.slo.RunnerStopped(r.index)
	return nil
}

// --- Fault injector ---

// FaultInjector drives the ha-soak-plan §4 fault matrix. Each method injects
// one fault class and is expected to return after the fault has been induced
// (not after recovery); recovery is observed via the SLO recorder and cluster
// state queries.
//
// HONESTY: implementations live in Task 5.2 (faults.go). The stubInjector
// returned by NewStubInjector satisfies the interface so the harness compiles
// and so callers can wire a no-op injector during scaffold development. It
// returns ErrFaultNotImplemented for every method — never call it expecting a
// real fault to be induced.
type FaultInjector interface {
	// LeaderKill terminates the replica currently holding leadership
	// (ha-soak-plan §4 row 1). Expected invariant: a non-leader takes over
	// within ≤ TTL (15s); at most one maintenance leader at a time.
	LeaderKill(ctx context.Context) error
	// LeaderRestart kills then restarts the same replica (§4 row 2). Expected
	// invariant: the restarted replica re-campaigns; in-flight
	// assignments/leases/outbox entries are not lost.
	LeaderRestart(ctx context.Context) error
	// RedisFailover triggers a sentinel/cluster Redis failover (§4 row 3).
	// ENV-GATED: requires a real sentinel/cluster topology; miniredis has no
	// HA semantics. Implementations may document a manual runbook step in
	// lieu of an automated trigger.
	RedisFailover(ctx context.Context) error
	// NetworkPartition blocks traffic between target and Redis (§4 row 4).
	// target names a side: "server" or "runner". Real partition injection uses
	// iptables / network namespaces (ENV-GATED); miniredis cannot simulate it.
	NetworkPartition(ctx context.Context, target string) error
	// RunnerKill terminates a runner process (§4 row 5). Expected invariant:
	// the sweeper reclaims the expired lease and re-dispatches; another runner
	// converges the workflow to a terminal state.
	RunnerKill(ctx context.Context) error
	// ReportResponseLoss drops runner→server report responses (§4 row 6).
	// Implemented at the transport layer (intercept the runner protocol
	// response) rather than at the process level.
	ReportResponseLoss(ctx context.Context) error
	// OutboxFlushFail kills the server after a commit succeeds but before the
	// outbox flush (§4 row 7). Expected invariant: on restart the outbox
	// dispatcher replays the pending downstream dispatch intent.
	OutboxFlushFail(ctx context.Context) error
}

// ErrFaultNotImplemented is returned by stubInjector for every fault method.
// It signals that the harness scaffold is wired but the real injection is
// Task 5.2; callers must not interpret a nil error from a stub injector as
// "fault induced".
var ErrFaultNotImplemented = errors.New("soak: fault injection not implemented (see Task 5.2 faults.go)")

// stubInjector is a no-op FaultInjector used to make the harness compile before
// Task 5.2 lands the real implementations. Every method returns
// ErrFaultNotImplemented so no caller can mistake the stub for working fault
// injection.
type stubInjector struct {
	cluster *Cluster
	calls   atomic.Int64
}

// NewStubInjector returns a FaultInjector whose methods all return
// ErrFaultNotImplemented. It records the call count so tests can assert the
// harness reached the injector at all.
func NewStubInjector(c *Cluster) FaultInjector {
	return &stubInjector{cluster: c}
}

// Calls returns the number of fault methods invoked on this stub.
func (s *stubInjector) Calls() int64 { return s.calls.Load() }

func (s *stubInjector) LeaderKill(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) LeaderRestart(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) RedisFailover(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) NetworkPartition(context.Context, string) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) RunnerKill(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) ReportResponseLoss(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}
func (s *stubInjector) OutboxFlushFail(context.Context) error {
	s.calls.Add(1)
	return ErrFaultNotImplemented
}

// --- SLO recorder ---

// SLORecorder captures the ha-soak-plan §6 SLO measurements: leader switch
// time, recovery time, duplicate invocation count, and error rate. The wire
// points are defined here so the harness contract is stable; the real
// histogram/counter implementation lands in Task 5.3 (slo.go).
//
// HONESTY: the methods on this interface are observation hooks only. They do
// not compute the SLO; they record the raw events from which an SLO report is
// derived. Real SLO quantification is ENV-GATED.
type SLORecorder interface {
	// LeaderElected is called when a replica wins leadership.
	LeaderElected(replicaIndex int)
	// LeaderLost is called when a replica loses leadership (lease expiry,
	// resign, or kill).
	LeaderLost(replicaIndex int)
	// LeaderSwitchTime records the elapsed time between a leader loss and the
	// next leader acquisition (ha-soak-plan §6: ≤ 3×TTL target).
	LeaderSwitchTime(d time.Duration)
	// RecoveryTime records the elapsed time for outbox/lease replay convergence
	// after a fault (§6: ≤ 30s target).
	RecoveryTime(d time.Duration)
	// DuplicateInvocation records one duplicate handler invocation observed
	// during recovery (§6: recorded, not bounded; at-least-once delivery).
	DuplicateInvocation()
	// ReplicaStopped / RunnerStarted / RunnerStopped are lifecycle events for
	// topology accounting.
	ReplicaStopped(replicaIndex int)
	RunnerStarted(runnerIndex int)
	RunnerStopped(runnerIndex int)
}

// noopSLORecorder is the default recorder when Options.SLORecorder is nil.
type noopSLORecorder struct{}

func (noopSLORecorder) LeaderElected(int)             {}
func (noopSLORecorder) LeaderLost(int)                {}
func (noopSLORecorder) LeaderSwitchTime(time.Duration) {}
func (noopSLORecorder) RecoveryTime(time.Duration)    {}
func (noopSLORecorder) DuplicateInvocation()          {}
func (noopSLORecorder) ReplicaStopped(int)            {}
func (noopSLORecorder) RunnerStarted(int)            {}
func (noopSLORecorder) RunnerStopped(int)            {}

// --- helpers shared with test/integration/harness.go (re-implemented here
// because soak is a standalone package and cannot import the integration
// package's unexported helpers). ---

// RequireRedis returns the Redis address, skipping the test when Redis is
// unreachable. It mirrors test/integration/harness.go:requireRedis so a real
// soak run (XFLOW_TEST_REDIS_ADDR set) gates itself the same way as the
// integration suite. Under XFLOW_REQUIRE_REDIS_INTEGRATION=1 (CI gating) it
// fails the test instead, so a missing dependency cannot be mistaken for a
// passing gate (per 2026-07-18 remediation §6.3).
//
// Smoke tests do NOT call this — they use in-process miniredis. This helper
// exists for the real-environment soak entry points (Task 5.2).
func RequireRedis(t testing.TB) string {
	t.Helper()
	addr := redisAddr()
	if err := pingRedis(addr, 2*time.Second); err != nil {
		if envOr("XFLOW_REQUIRE_REDIS_INTEGRATION", "") == "1" {
			t.Fatalf("XFLOW_REQUIRE_REDIS_INTEGRATION=1: redis unavailable at %s: %v (run `make env-up`)", addr, err)
		}
		t.Skipf("redis unavailable at %s: %v (run `make env-up`)", addr, err)
	}
	return addr
}
