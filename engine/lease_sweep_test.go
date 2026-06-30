package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestEngine_BuildTaskLease_PopulatesIssuedAtAndTTL(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "lease-ttl",
		Nodes: []types.NodeDef{{Name: "n", Type: "test.echo"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithDefaultLeaseTTL(2*time.Second))
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if lease.TTL != 2*time.Second {
		t.Fatalf("TTL = %v, want 2s", lease.TTL)
	}
	if lease.IssuedAt.IsZero() {
		t.Fatal("IssuedAt is zero")
	}
	if got, want := lease.Deadline(), lease.IssuedAt.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("Deadline = %v, want %v", got, want)
	}
}

func TestEngine_ReclaimLease_RequeuesAndClearsToken(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "reclaim",
		Nodes: []types.NodeDef{{Name: "n", Type: "test.echo"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue, WithDefaultLeaseTTL(50*time.Millisecond))
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatal(err)
	}

	expired, err := state.ListExpiredLeases(ctx, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 0 {
		t.Fatalf("expired before TTL: %+v", expired)
	}

	expired, err = state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].NodeName != "n" {
		t.Fatalf("expected one expired lease, got %+v", expired)
	}

	ok, err := eng.ReclaimLease(ctx, expired[0])
	if err != nil || !ok {
		t.Fatalf("ReclaimLease ok=%v err=%v", ok, err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "n" {
		t.Fatalf("expected one re-enqueued task, got %v", taskNames(tasks))
	}

	ns, _ := state.GetNode(ctx, id, "n")
	if ns == nil || ns.Status != types.NodeStatusPending || ns.LeaseToken != "" {
		t.Fatalf("snapshot after reclaim = %+v, want pending+empty token", ns)
	}

	if ok, err := eng.ReclaimLease(ctx, expired[0]); err != nil || ok {
		t.Fatalf("second reclaim ok=%v err=%v, want token-mismatch race", ok, err)
	}

	// Stale commit should be rejected — the lease token no longer matches.
	if err := eng.CommitTaskResult(ctx, lease, TaskResult{Output: &types.Output{Data: map[string]any{"k": 1}}}); err == nil {
		t.Fatal("expected stale commit after reclaim to fail")
	}
}
