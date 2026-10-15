package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// TestBuildTaskLeaseStampsTheEnginesNamespace pins the first hop of the
// namespace's cross-boundary wiring for embedded engines: a lease built by an
// engine constructed with WithNamespace carries that namespace, because
// nothing else can put one there — the local queue's worker starts from
// context.Background(), so the submitting request's namespace never reaches
// BuildTaskLease, and a local engine has no claim record to read it from.
// execution/runner.go reads lease.Namespace back to bind the input, which is
// how $supplies and the artifact-reference check pick their tenant.
//
// Mutation: make WithNamespace a no-op — lease.Namespace stays empty and this
// test fails.
func TestBuildTaskLeaseStampsTheEnginesNamespace(t *testing.T) {
	g, state, queue := artifactUseWiringDef(t)
	eng := New(state, queue, WithNamespace("tenant-b"))
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}
	if lease.Namespace != "tenant-b" {
		t.Fatalf("lease.Namespace = %q, want tenant-b", lease.Namespace)
	}
}

// TestBuildTaskLeaseWithoutNamespaceLeavesItEmpty pins the multi-namespace
// invariant: an engine with no WithNamespace must NOT invent one. The
// deployed control plane names each lease per claim after BuildTaskLease
// returns (dispatchSubgraphLease and friends), and a whole-engine default
// stamped here would either be that same value in a coincidence or paper
// over a missing per-claim name in a real multi-tenant deployment.
//
// Mutation: default the field to namespace.Default — lease.Namespace becomes
// "default" instead of empty and this test fails.
func TestBuildTaskLeaseWithoutNamespaceLeavesItEmpty(t *testing.T) {
	g, state, queue := artifactUseWiringDef(t)
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}
	if lease.Namespace != "" {
		t.Fatalf("lease.Namespace = %q, want empty (unstamped)", lease.Namespace)
	}
}

// TestRecoverTaskLeaseStampsTheEnginesNamespace is the RecoverTaskLease
// counterpart: a control-plane crash between lease issue and delivery must
// not drop the namespace on replay. For the distributed control plane the
// service layer overwrites this with the claim's namespace afterwards; an
// embedded engine has no such caller.
//
// Mutation: drop the stamping in RecoverTaskLease — recovered.Namespace stays
// empty and this test fails.
func TestRecoverTaskLeaseStampsTheEnginesNamespace(t *testing.T) {
	g, state, queue := artifactUseWiringDef(t)
	eng := New(state, queue, WithNamespace(namespace.Namespace("tenant-b")))
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	task := queue.Drain()[0]
	if _, err := eng.BuildTaskLease(ctx, task); err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}
	recovered, err := eng.RecoverTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("RecoverTaskLease: %v", err)
	}
	if recovered.Namespace != "tenant-b" {
		t.Fatalf("recovered.Namespace = %q, want tenant-b", recovered.Namespace)
	}
}
