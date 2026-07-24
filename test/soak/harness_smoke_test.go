//go:build soak

package soak

import (
	"context"
	"testing"
	"time"
)

// TestClusterSmokeMiniredis is the single-node smoke for the HA soak harness.
// It brings up a 2-replica cluster over an in-process miniredis (no real
// Redis, no fault injection) and asserts:
//
//  1. Both replicas start (apiserver + ControlPlane + asynq consumer bound to
//     the shared miniredis) without error.
//  2. Exactly one replica wins leadership via the shared RedisLeaderElector
//     key within defaultLeaderWait — the core HA invariant ("at most one
//     maintenance leader").
//  3. Cluster.Stop tears every replica down without blocking (no goroutine
//     leak / shutdown hang).
//
// HONESTY: this is a START/STOP + LEADER-CONVERGENCE smoke only. It does NOT
// exercise fault injection (Task 5.2), runner lifecycle, or Redis HA failover.
// Real multi-replica fault injection and Redis failover are ENVIRONMENT-GATED:
// miniredis is a single-node Redis emulator with no HA semantics, and the
// replicas here share a single process rather than separate hosts. Passing
// this smoke does not constitute HA verification (see ha-soak-plan §6, §9).
func TestClusterSmokeMiniredis(t *testing.T) {
	c, err := NewCluster(t, Options{ReplicaCount: 2})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if c.Miniredis() == nil {
		t.Fatal("expected in-process miniredis for smoke")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Cluster.Start: %v", err)
	}

	replicas := c.Replicas()
	if got := len(replicas); got != 2 {
		t.Fatalf("len(Replicas) = %d, want 2", got)
	}
	for i, r := range replicas {
		if r.HTTPURL() == "" {
			t.Fatalf("replica %d HTTPURL empty", i)
		}
	}

	// Core HA invariant: exactly one leader among the replicas, observed
	// within the smoke timeout. Real soak uses TTL-scaled bounds (§6).
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), defaultLeaderWait)
	defer leaderCancel()
	leader, err := c.Leader(leaderCtx)
	if err != nil {
		t.Fatalf("Leader: %v", err)
	}
	t.Logf("leader = replica %d", leader.Index())

	// Stub injector wires up and reports unimplemented for every fault, so
	// the harness contract compiles without committing to a real fault
	// implementation (Task 5.2).
	inj := NewStubInjector(c)
	if err := inj.LeaderKill(ctx); err != ErrFaultNotImplemented {
		t.Fatalf("LeaderKill = %v, want ErrFaultNotImplemented", err)
	}
	if got := inj.(*stubInjector).Calls(); got != 1 {
		t.Fatalf("stub injector Calls = %d, want 1", got)
	}

	// Stop must not block / hang; bound it well under the test timeout.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("Cluster.Stop: %v", err)
	}

	// Stop is idempotent: a second Stop (also registered via t.Cleanup) must
	// be a no-op.
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("second Cluster.Stop: %v", err)
	}
}
