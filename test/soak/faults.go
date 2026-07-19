//go:build soak

package soak

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// ErrEnvGated is returned by fault injectors whose injection cannot be
// truthfully induced in the in-process miniredis harness. Each such fault
// requires a real multi-host topology, a real Redis HA deployment, real OS
// process kill/restart, or a real runner-protocol transport — none of which the
// in-process soak harness provides. Returning ErrEnvGated (rather than nil) is
// deliberate: callers must not mistake "not implemented in-process" for "fault
// induced and recovered".
//
// See docs/references/ha-soak-plan.md §4 (fault matrix) and §5 (invariants)
// for the real-environment runbook each ENV-GATED injector points at.
var ErrEnvGated = errors.New("soak: fault injection is ENV-GATED in the in-process harness (requires real multi-host topology / Redis HA / OS process kill / runner protocol; see docs/references/ha-soak-plan.md §4)")

// InjectorOptions configures an Injector.
type InjectorOptions struct {
	// LeaderWaitTimeout bounds the wait for a new leader to be observed after
	// a leader-losing fault. Defaults to one lease TTL (15s): the
	// RedisLeaderElector Campaign loop retries SetNX every ttl/3, so after a
	// graceful Resign a remaining replica wins within ~ttl/3 in the common
	// case and within one TTL in the worst case (ha-soak-plan §4 row 1:
	// "non-leader ≤ TTL takes over"). Real-environment soak should scale this
	// to 3×TTL (§6: ≤ 45s) and gate execution behind RequireRedis.
	LeaderWaitTimeout time.Duration
}

// Injector is the real FaultInjector implementation for the soak harness. It
// implements each ha-soak-plan §4 fault row with an HONEST capability split:
//
//   - LeaderKill / LeaderRestart: REAL in-process. The in-process harness can
//     genuinely exercise RedisLeaderElector failover over the shared miniredis:
//     stopping the leader replica triggers graceful Resign (leaderReleaseScript
//     deletes the Redis key), and a remaining replica's Campaign loop wins
//     within ~ttl/3. Single-leader and "new leader != killed leader" invariants
//     are asserted, and leader-switch / recovery times are recorded on the
//     SLORecorder. NOTE: this is GRACEFUL leadership transfer, not a SIGKILL
//     crash. A real crash kill (no Resign) waits for the lease TTL to expire
//     before failover — that TTL-based crash-recovery path is ENV-GATED (it
//     requires an independent OS process whose renewal goroutine cannot be
//     stopped without graceful Shutdown, which the in-process harness cannot
//     truthfully reproduce).
//   - RedisFailover / NetworkPartition: ENV-GATED. miniredis is a single-node
//     in-memory emulator with no sentinel/cluster failover semantics and no
//     real network to partition. Returning ErrEnvGated rather than simulating
//     a fake failover that proves nothing.
//   - RunnerKill / ReportResponseLoss / OutboxFlushFail: ENV-GATED. These
//     require the real runner-protocol client wired against the replica's HTTP
//     URL (the harness's Runner is currently a lifecycle stub) and/or precise
//     timing control between commit and outbox flush. They are honestly marked
//     ENV-GATED until the runner wiring lands; faking them in-process would
//     assert against a stub, not the real report/commit path.
//
// HONESTY FIRST: an ENV-GATED injector never pretends to induce its fault. It
// returns ErrEnvGated so a caller cannot mistake "skipped" for "verified".
type Injector struct {
	cluster *Cluster
	opts    InjectorOptions
}

// defaultInjectorLeaderWait is the default LeaderWaitTimeout: one lease TTL.
// The leader-elect retry cadence is ttl/3, so this comfortably bounds the
// worst-case in-process failover after a graceful Resign.
const defaultInjectorLeaderWait = 15 * time.Second

// NewInjector returns a FaultInjector backed by real in-process implementations
// for the leader-kill family and honest ENV-GATED stubs for the rest.
func NewInjector(c *Cluster, opts InjectorOptions) *Injector {
	if opts.LeaderWaitTimeout <= 0 {
		opts.LeaderWaitTimeout = defaultInjectorLeaderWait
	}
	return &Injector{cluster: c, opts: opts}
}

// Compile-time check: *Injector satisfies the FaultInjector interface declared
// in harness.go.
var _ FaultInjector = (*Injector)(nil)

// LeaderKill implements ha-soak-plan §4 row 1 (leader kill). In-process it
// performs a GRACEFUL leadership transfer: stop the leader replica (which
// calls ControlPlane.Shutdown → RedisLeaderElector.Resign → leaderReleaseScript
// deletes the Redis lease key), then wait for a remaining replica to campaign
// and win within LeaderWaitTimeout.
//
// Invariants asserted (ha-soak-plan §5):
//   - §5.3: at most one maintenance leader after failover (pollSingleLeader
//     returns only when exactly one replica reports IsLeader, and errors on >1).
//   - The new leader is a DIFFERENT replica than the killed one (the killed
//     replica resigned and its apiserver is shut down, so it cannot lead).
//
// Metrics recorded on the cluster's SLORecorder:
//   - LeaderLost(killedIndex)
//   - LeaderSwitchTime(elapsed from kill to new leader observed)
//   - RecoveryTime(elapsed)
//
// HONESTY: this verifies the graceful failover path. Real crash-kill recovery
// (SIGKILL, no Resign, TTL-based expiry) is ENV-GATED — see package doc.
func (inj *Injector) LeaderKill(ctx context.Context) error {
	start := time.Now()

	leader, err := inj.pollSingleLeader(ctx)
	if err != nil {
		return fmt.Errorf("soak LeaderKill: locate leader: %w", err)
	}
	killedIdx := leader.Index()

	if err := leader.Stop(ctx); err != nil {
		return fmt.Errorf("soak LeaderKill: stop leader replica %d: %w", killedIdx, err)
	}
	inj.cluster.slo.LeaderLost(killedIdx)

	waitCtx, cancel := context.WithTimeout(ctx, inj.opts.LeaderWaitTimeout)
	defer cancel()
	newLeader, err := inj.pollSingleLeader(waitCtx)
	if err != nil {
		return fmt.Errorf("soak LeaderKill: no failover observed within %s: %w", inj.opts.LeaderWaitTimeout, err)
	}
	if newLeader.Index() == killedIdx {
		return fmt.Errorf("soak LeaderKill: invariant violation — killed replica %d still reports leader", killedIdx)
	}

	elapsed := time.Since(start)
	inj.cluster.slo.LeaderSwitchTime(elapsed)
	inj.cluster.slo.RecoveryTime(elapsed)
	inj.cluster.slo.LeaderElected(newLeader.Index())
	return nil
}

// LeaderRestart implements ha-soak-plan §4 row 2 (leader restart). In-process
// it performs LeaderKill (graceful leadership transfer), then rebuilds a fresh
// replica at the same index and waits for the cluster to re-converge to a
// single leader.
//
// Invariants asserted (ha-soak-plan §5):
//   - §5.3: exactly one leader after the killed replica is rebuilt and
//     re-campaigns.
//   - §5.1 (durability): the rebuilt replica re-campaigns against the SAME
//     shared Redis, so durable state (ready intent / assignment / lease /
//     terminal result) is not lost across the restart. NOTE: without a
//     workflow actively running through the restart, this asserts leader
//     re-convergence only; in-flight assignment durability across a real
//     crash-restart is ENV-GATED.
//
// Metrics recorded: LeaderLost(killedIndex), RecoveryTime(elapsed).
func (inj *Injector) LeaderRestart(ctx context.Context) error {
	start := time.Now()

	leader, err := inj.pollSingleLeader(ctx)
	if err != nil {
		return fmt.Errorf("soak LeaderRestart: locate leader: %w", err)
	}
	killedIdx := leader.Index()

	if err := leader.Stop(ctx); err != nil {
		return fmt.Errorf("soak LeaderRestart: stop leader replica %d: %w", killedIdx, err)
	}
	inj.cluster.slo.LeaderLost(killedIdx)

	// Kill phase: a remaining replica must take over (§4 row 2).
	killWaitCtx, killCancel := context.WithTimeout(ctx, inj.opts.LeaderWaitTimeout)
	defer killCancel()
	if _, err := inj.pollSingleLeader(killWaitCtx); err != nil {
		return fmt.Errorf("soak LeaderRestart: no failover after kill: %w", err)
	}

	// Restart phase: rebuild a fresh replica at the same index so it
	// re-campaigns against the shared Redis.
	if err := inj.cluster.rebuildReplica(ctx, killedIdx); err != nil {
		return fmt.Errorf("soak LeaderRestart: rebuild replica %d: %w", killedIdx, err)
	}

	// Re-convergence: exactly one leader after the rebuilt replica campaigns.
	restartWaitCtx, restartCancel := context.WithTimeout(ctx, inj.opts.LeaderWaitTimeout)
	defer restartCancel()
	if _, err := inj.pollSingleLeader(restartWaitCtx); err != nil {
		return fmt.Errorf("soak LeaderRestart: no convergence after restart: %w", err)
	}

	inj.cluster.slo.RecoveryTime(time.Since(start))
	return nil
}

// RedisFailover implements ha-soak-plan §4 row 3 (Redis 主从切换). ENV-GATED:
// miniredis is a single-node in-memory emulator with no sentinel/cluster
// failover semantics; it cannot reproduce a real master/replica switch.
//
// Real-environment runbook (ha-soak-plan §4 row 3):
//  1. Deploy Redis as sentinel-managed master/replica (or Redis Cluster).
//  2. Wire the control plane to a sentinel/cluster client (ha-soak-plan §3 —
//     currently backend/distributed/backend.go uses redis.NewClient only).
//  3. Trigger failover via `redis-cli -p <sentinel> SENTINEL FAILOVER <master>`
//     or by stopping the master process.
//  4. Assert: during the switch, leader election / commit calls retry and
//     recover; after the switch, leader re-acquires; ready intent / assignment
//     / lease / terminal result are intact (§5.1).
//
// Returns ErrEnvGated so no caller mistakes "skipped in-process" for "failover
// verified".
func (inj *Injector) RedisFailover(context.Context) error {
	return ErrEnvGated
}

// NetworkPartition implements ha-soak-plan §4 row 4 (网络分区 server↔Redis).
// ENV-GATED: the in-process harness shares a single miniredis over an in-memory
// dial; there is no real network to partition and no OS-level isolation between
// replicas (they are goroutines in one process).
//
// Real-environment runbook (ha-soak-plan §4 row 4):
//  1. Deploy each replica as a separate xflow-server process on its own host
//     (or network namespace).
//  2. Block server↔Redis traffic with iptables:
//       iptables -A OUTPUT -p tcp --dport <redis-port> -j DROP
//  3. Assert: the partitioned replica loses leadership (lease expires, no
//     renewal); no dual leader (§5.3); after partition heals the replica
//     re-campaigns (§5.4).
//
// Returns ErrEnvGated.
func (inj *Injector) NetworkPartition(context.Context, string) error {
	return ErrEnvGated
}

// RunnerKill implements ha-soak-plan §4 row 5 (runner kill). ENV-GATED: the
// harness's Runner is a lifecycle stub (Start/Stop are no-ops); it does not
// wire a real runnersvc.Runner + protocol.Client against the replica's HTTP
// URL, so there is no real runner process/goroutine whose lease expiry and
// sweeper reclamation can be observed.
//
// Pre-condition for a real in-process implementation: wire Runner.Start to
// construct a real service/runner.Runner (protocol.NewClient against
// replica.HTTPURL(), a handler registry, runner.New) so a kill cancels a real
// poll/report loop and the sweeper reclaims its lease. Until that wiring lands,
// returning ErrEnvGated honestly rather than asserting against a stub.
//
// Real-environment runbook (ha-soak-plan §4 row 5): SIGKILL a runner process;
// assert the sweeper reclaims the expired lease and re-dispatches; another
// runner converges the workflow to a terminal state (§5.4, §5.5).
func (inj *Injector) RunnerKill(context.Context) error {
	return ErrEnvGated
}

// ReportResponseLoss implements ha-soak-plan §4 row 6 (响应丢失 runner→server).
// ENV-GATED: requires a real runner driving a real workflow so a report
// response can be dropped at the transport layer (protocol.Client.post wraps an
// injectable *http.Client whose Transport can be wrapped to drop
// ReportResultPath responses). The harness's Runner stub does not wire a real
// protocol.Client, so there is no genuine report response to drop.
//
// When runner wiring lands, the in-process implementation is straightforward
// and SHOULD be promoted to real (it is the most tractable in-process fault in
// the matrix):
//  1. Build the runner's protocol.Client with an *http.Client whose Transport
//     is a lossyTransport{inner, dropOnPath: protocol.ReportResultPath}.
//  2. Submit a workflow; the runner executes and reports; the lossy transport
//     drops the report response.
//  3. Assert: the runner reconnects and replays the report on the same lease;
//     the fenced commit (lease token as fencing token) does not advance the DAG
//     twice — CommitTaskResultWithOutcome returns DuplicateTerminal on the
//     duplicate (§5.2); the business side effect occurs exactly once (§5.5).
//
// Returns ErrEnvGated until the runner wiring exists.
func (inj *Injector) ReportResponseLoss(context.Context) error {
	return ErrEnvGated
}

// OutboxFlushFail implements ha-soak-plan §4 row 7 (outbox flush 前故障).
// ENV-GATED: requires precise timing control between a successful commit and
// the outbox dispatcher's flush, plus a real runner driving the commit. The
// in-process harness has neither (no runner wired; no commit/flush seam hook
// exposed without prod test instrumentation, which the task brief forbids).
//
// Real-environment runbook (ha-soak-plan §4 row 7): after a commit succeeds,
// kill the server before the outbox dispatcher flushes; on restart the
// dispatcher replays the pending downstream dispatch intent (§5.1, §5.4).
//
// Returns ErrEnvGated.
func (inj *Injector) OutboxFlushFail(context.Context) error {
	return ErrEnvGated
}

// pollSingleLeader polls the cluster until exactly one replica reports
// IsLeader, or ctx is cancelled. It mirrors Cluster.Leader but does NOT record
// SLO events (the injector records them explicitly at fault boundaries). It
// returns an error if >1 replicas simultaneously claim leadership (ha-soak-plan
// §5.3 invariant violation).
func (inj *Injector) pollSingleLeader(ctx context.Context) (*Replica, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		replicas := inj.cluster.Replicas()
		var leader *Replica
		leaders := 0
		for _, r := range replicas {
			if r.IsLeader() {
				leaders++
				leader = r
			}
		}
		if leaders == 1 {
			return leader, nil
		}
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

// rebuildReplica constructs a fresh replica at the given index (re-campaigning
// against the shared Redis) and swaps it into the cluster's replica slice in
// place of the previously stopped one. Used by LeaderRestart to simulate a
// process restart. The previously stopped replica has already been shut down
// (apiserver.Shutdown + httpSrv.Close via Replica.Stop); it is dropped from the
// slice, and Cluster.Stop will tear down the rebuilt replica at cleanup.
func (c *Cluster) rebuildReplica(ctx context.Context, index int) error {
	c.mu.Lock()
	if index < 0 || index >= len(c.replicas) {
		c.mu.Unlock()
		return fmt.Errorf("soak: rebuild replica index %d out of range (have %d replicas)", index, len(c.replicas))
	}
	c.mu.Unlock()

	r, err := c.newReplica(context.Background(), index)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.replicas[index] = r
	c.mu.Unlock()
	return nil
}

// WaitForCompletion polls the engine StateStore until the execution reaches a
// terminal status, then returns its result. It mirrors
// test/integration/harness.go:waitForCompletion so the soak package can drive a
// submitted workflow to terminal state in future real-env fault runs (e.g. to
// assert "submitted workflow continues to terminal state after LeaderRestart").
//
// Not currently invoked by the in-process leader faults (which assert leader
// convergence, not workflow completion, since no runner is wired). Retained here
// so the helper is available when runner wiring lands; tagged
// //nolint:unused to silence the unused linter once `make lint` enables the
// soak build tag.
//
//nolint:unused
func WaitForCompletion(ctx context.Context, t testing.TB, state engine.StateStore, id types.ExecutionID, outputNodes ...string) types.Result {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		if err == nil && types.IsTerminalExecutionStatus(snap.Status) {
			out := map[string]any{}
			for _, n := range outputNodes {
				if v, e := state.GetOutput(ctx, id, n); e == nil && v != nil {
					out[n] = v
				}
			}
			return types.Result{ExecutionID: id, Status: snap.Status, Output: out}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("soak: timeout waiting for execution %s: %v", id, ctx.Err())
		case <-ticker.C:
		}
	}
}
