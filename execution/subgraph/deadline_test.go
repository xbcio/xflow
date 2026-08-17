package subgraph

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// sleepHandler blocks until the context fires or its internal timer elapses,
// whichever comes first. It increments a counter so tests can observe whether
// the handler was cancelled or ran to completion.
type sleepHandler struct {
	duration  time.Duration
	started   atomic.Int64
	completed atomic.Int64
	cancelled atomic.Int64
}

func (h *sleepHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.sleep"}
}

func (h *sleepHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.started.Add(1)
	select {
	case <-time.After(h.duration):
		h.completed.Add(1)
		return &types.Output{Data: map[string]any{"done": true}}, nil
	case <-ctx.Done():
		h.cancelled.Add(1)
		return nil, ctx.Err()
	}
}

// buildSingleMemberPackage builds a sub-graph package with one member node.
// The member's Timeout field controls its own declared timeout (0 = unset,
// positive = bounded, negative = opt-out).
func buildSingleMemberPackage(t *testing.T, memberTimeout time.Duration) *graph.SubgraphPackage {
	t.Helper()
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "member",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "member", Type: "test.sleep", Version: 1, Timeout: memberTimeout},
				{Name: "__collector_member_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"member": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_member_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_member_main", SrcNode: "member", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.sleep", NodeVersion: 1},
		},
	}
}

func deadlineTestExecutor(t *testing.T, handler types.ActionHandler) *Executor {
	t.Helper()
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.sleep", handler)
	return NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
}

// TestGroupDeadlineClampsMembers: a group with a 200ms deadline whose member
// declares 10s must see the member terminated at ~200ms, not 10s.
func TestGroupDeadlineClampsMembers(t *testing.T) {
	handler := &sleepHandler{duration: 5 * time.Second}
	ex := deadlineTestExecutor(t, handler)

	// Member declares 10s timeout, but the group deadline is 200ms from now.
	pkg := buildSingleMemberPackage(t, 10*time.Second)
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	groupDeadline := time.Now().Add(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
		Deadline:    groupDeadline,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// The member must be terminated by the group deadline (~200ms), not by its
	// own 10s timeout. Allow generous slack for CI but reject anything over 2s.
	if elapsed > 2*time.Second {
		t.Fatalf("execution took %v, want ~200ms (group deadline should clamp member)", elapsed)
	}

	// The outcome must reflect the timeout.
	if res.Outcome != OutcomeTimeout && res.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %v, want timeout or failed (member was killed by group deadline)", res.Outcome)
	}

	// The handler must have started but not completed.
	if handler.started.Load() == 0 {
		t.Fatal("handler was never started")
	}
	if handler.completed.Load() > 0 {
		t.Fatal("handler completed -- group deadline did not terminate it")
	}
}

// TestMemberDeadlineWinsWhenTighter: a group with a 10s deadline whose member
// declares 100ms must see the member terminated at ~100ms.
func TestMemberDeadlineWinsWhenTighter(t *testing.T) {
	handler := &sleepHandler{duration: 5 * time.Second}
	ex := deadlineTestExecutor(t, handler)

	// Member declares 100ms timeout; group deadline is 10s from now.
	pkg := buildSingleMemberPackage(t, 100*time.Millisecond)
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	groupDeadline := time.Now().Add(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
		Deadline:    groupDeadline,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// The member's own 100ms timeout is tighter than the group's 10s.
	if elapsed > 2*time.Second {
		t.Fatalf("execution took %v, want ~100ms (member's own deadline should win)", elapsed)
	}

	// The member timed out.
	if res.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %v, want failed (member was killed by its own deadline)", res.Outcome)
	}
}

// TestGroupTimeoutCancelsRunningMembers is the regression guard for the
// pre-existing bug: when the group's deadline fires, a member handler that
// respects ctx must observe cancellation. Before this fix, the member handler
// ran on context.Background() with no deadline at all, so a group timeout left
// every running member uncancelled -- WaitDone returned DeadlineExceeded while
// the members kept going.
func TestGroupTimeoutCancelsRunningMembers(t *testing.T) {
	handler := &sleepHandler{duration: 30 * time.Second}
	ex := deadlineTestExecutor(t, handler)

	// Member declares no timeout (will inherit the default 30min); the group
	// deadline is 200ms from now.
	pkg := buildSingleMemberPackage(t, -1) // -1 = opt out of any own timeout
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	groupDeadline := time.Now().Add(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
		Deadline:    groupDeadline,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if elapsed > 2*time.Second {
		t.Fatalf("execution took %v, want ~200ms (group deadline must reach member)", elapsed)
	}

	if res.Outcome != OutcomeTimeout {
		t.Fatalf("outcome = %v, want timeout (group deadline fired)", res.Outcome)
	}

	// The member MUST have observed cancellation -- the fix propagates the
	// group's absolute deadline into the member's lease, which the runner
	// enforces via context.WithDeadline.
	if handler.started.Load() == 0 {
		t.Fatal("handler was never started")
	}
	if handler.completed.Load() > 0 {
		t.Fatal("handler completed (30s sleep!) -- group deadline did not reach it")
	}
}

// TestZeroGroupDeadlineLeavesMemberBoundsIntact: a group with no timeout still
// bounds its members by their own (or the global default).
func TestZeroGroupDeadlineLeavesMemberBoundsIntact(t *testing.T) {
	handler := &sleepHandler{duration: 5 * time.Second}
	ex := deadlineTestExecutor(t, handler)

	// Member declares 100ms timeout; group has NO deadline (zero).
	pkg := buildSingleMemberPackage(t, 100*time.Millisecond)
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
		// Deadline is zero -- no group-level deadline.
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// The member's own 100ms timeout still fires.
	if elapsed > 2*time.Second {
		t.Fatalf("execution took %v, want ~100ms (member's own deadline applies)", elapsed)
	}
	if res.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %v, want failed (member timed out on its own)", res.Outcome)
	}
}
