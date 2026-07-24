//go:build soak

package soak

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// countingRecorder is a test SLORecorder that captures every event so fault
// tests can assert the injector recorded the expected lifecycle / SLO
// observations (leader switch time, recovery time, leader lost/elected).
type countingRecorder struct {
	mu             sync.Mutex
	leaderElected  []int
	leaderLost     []int
	switchTimes    []time.Duration
	recoveryTimes  []time.Duration
	duplicates     int
	replicaStopped []int
	runnerStarted  []int
	runnerStopped  []int
}

func (r *countingRecorder) LeaderElected(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaderElected = append(r.leaderElected, i)
}
func (r *countingRecorder) LeaderLost(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaderLost = append(r.leaderLost, i)
}
func (r *countingRecorder) LeaderSwitchTime(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.switchTimes = append(r.switchTimes, d)
}
func (r *countingRecorder) RecoveryTime(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recoveryTimes = append(r.recoveryTimes, d)
}
func (r *countingRecorder) DuplicateInvocation() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.duplicates++
}
func (r *countingRecorder) ReplicaStopped(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicaStopped = append(r.replicaStopped, i)
}
func (r *countingRecorder) RunnerStarted(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runnerStarted = append(r.runnerStarted, i)
}
func (r *countingRecorder) RunnerStopped(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runnerStopped = append(r.runnerStopped, i)
}

func (r *countingRecorder) leaderLostCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.leaderLost)
}
func (r *countingRecorder) switchTimeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.switchTimes)
}
func (r *countingRecorder) recoveryTimeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recoveryTimes)
}

// startCluster brings up a 2-replica in-process miniredis cluster with a
// counting SLO recorder and waits for initial single-leader convergence.
//
// NOTE: c.Start is given context.Background() (not a short-lived timeout ctx)
// because Start's ctx becomes the parent of every replica's startCtx; a ctx
// cancelled at helper return would tear all replicas down before the fault
// test runs. Replica lifetime is bounded by Cluster.Stop (registered via
// t.Cleanup by Start).
func startCluster(t *testing.T, rec *countingRecorder) *Cluster {
	t.Helper()
	c, err := NewCluster(t, Options{ReplicaCount: 2, SLORecorder: rec})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Cluster.Start: %v", err)
	}
	// Wait for the initial single leader so LeaderKill has a leader to kill.
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), defaultLeaderWait)
	defer leaderCancel()
	if _, err := c.Leader(leaderCtx); err != nil {
		t.Fatalf("initial Leader: %v", err)
	}
	return c
}

// TestInjectorLeaderKillInProcess verifies the in-process LeaderKill fault:
// stopping the leader replica triggers graceful Resign, a remaining replica
// campaigns and wins within the bound, exactly one leader remains, and the new
// leader is a different replica than the killed one.
//
// HONESTY: this is a graceful leadership transfer over miniredis, not a real
// SIGKILL crash-kill (TTL-based failover). See faults.go Injector.LeaderKill
// doc for the ENV-GATED crash-kill boundary.
func TestInjectorLeaderKillInProcess(t *testing.T) {
	rec := &countingRecorder{}
	c := startCluster(t, rec)
	inj := NewInjector(c, InjectorOptions{})

	replicas := c.Replicas()
	origLeaderIdx := -1
	for _, r := range replicas {
		if r.IsLeader() {
			origLeaderIdx = r.Index()
		}
	}
	if origLeaderIdx < 0 {
		t.Fatalf("no initial leader found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := inj.LeaderKill(ctx); err != nil {
		t.Fatalf("LeaderKill: %v", err)
	}

	// Invariant §5.3: exactly one leader, and it is NOT the killed replica.
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatalf("post-kill Leader: %v", err)
	}
	if leader.Index() == origLeaderIdx {
		t.Fatalf("post-kill leader = replica %d (killed); expected failover to a different replica", origLeaderIdx)
	}
	if rec.leaderLostCount() < 1 {
		t.Fatalf("expected ≥1 LeaderLost event, got %d", rec.leaderLostCount())
	}
	if rec.switchTimeCount() != 1 {
		t.Fatalf("expected 1 LeaderSwitchTime event, got %d", rec.switchTimeCount())
	}
	if rec.recoveryTimeCount() != 1 {
		t.Fatalf("expected 1 RecoveryTime event, got %d", rec.recoveryTimeCount())
	}
	if rec.duplicates != 0 {
		t.Fatalf("expected 0 duplicate invocations on graceful transfer, got %d", rec.duplicates)
	}
}

// TestInjectorLeaderRestartInProcess verifies the in-process LeaderRestart
// fault: kill the leader, rebuild a fresh replica at the same index, and assert
// the cluster re-converges to exactly one leader (the restarted replica
// re-campaigns against the shared Redis).
//
// HONESTY: asserts leader re-convergence only; in-flight assignment durability
// across a real crash-restart is ENV-GATED (no workflow driven here).
func TestInjectorLeaderRestartInProcess(t *testing.T) {
	rec := &countingRecorder{}
	c := startCluster(t, rec)
	inj := NewInjector(c, InjectorOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := inj.LeaderRestart(ctx); err != nil {
		t.Fatalf("LeaderRestart: %v", err)
	}

	// Invariant §5.3: exactly one leader after restart.
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatalf("post-restart Leader: %v", err)
	}
	// The rebuilt replica must exist in the cluster and the cluster must have
	// exactly one leader (asserted by c.Leader returning without error).
	replicas := c.Replicas()
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas after restart, got %d", len(replicas))
	}
	if !replicas[0].IsLeader() && !replicas[1].IsLeader() {
		t.Fatalf("no leader after restart")
	}
	t.Logf("post-restart leader = replica %d", leader.Index())
	if rec.leaderLostCount() < 1 {
		t.Fatalf("expected ≥1 LeaderLost event, got %d", rec.leaderLostCount())
	}
	if rec.recoveryTimeCount() != 1 {
		t.Fatalf("expected 1 RecoveryTime event, got %d", rec.recoveryTimeCount())
	}
	if rec.duplicates != 0 {
		t.Fatalf("expected 0 duplicate invocations on graceful transfer, got %d", rec.duplicates)
	}
}

// TestInjectorEnvGatedStubs asserts that the ENV-GATED injectors honestly
// return ErrEnvGated rather than pretending to induce their fault. Each of
// these faults requires a real multi-host topology / Redis HA / OS process
// kill / runner-protocol wiring that the in-process miniredis harness does not
// provide; a nil error here would be a dishonest "verified" claim.
func TestInjectorEnvGatedStubs(t *testing.T) {
	rec := &countingRecorder{}
	c := startCluster(t, rec)
	inj := NewInjector(c, InjectorOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checks := []struct {
		name string
		fn   func() error
	}{
		{"RedisFailover", func() error { return inj.RedisFailover(ctx) }},
		{"NetworkPartition", func() error { return inj.NetworkPartition(ctx, "server") }},
		{"RunnerKill", func() error { return inj.RunnerKill(ctx) }},
		{"ReportResponseLoss", func() error { return inj.ReportResponseLoss(ctx) }},
		{"OutboxFlushFail", func() error { return inj.OutboxFlushFail(ctx) }},
	}
	for _, ck := range checks {
		if err := ck.fn(); !errors.Is(err, ErrEnvGated) {
			t.Errorf("%s = %v, want ErrEnvGated", ck.name, err)
		}
	}
}
