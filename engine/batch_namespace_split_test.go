package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file pins the two follow-ups on the namespace chain introduced by
// "carry the namespace across the in-process execution boundaries":
//
//   - the ExecuteBatch re-anchor is scoped to the BODY runtime only; the state
//     operations (graph load, lease fences, completion barrier) stay on the
//     task's own ctx, whose namespace is where the execution's records live. A
//     cluster engine whose SDK namespace differs from its submission namespace
//     would otherwise read state under a namespace the execution does not
//     exist in, fail the batch as ErrExecutionInactive, and strand the map
//     node;
//   - the parent's namespace actually travels on the batch payload, so the
//     lease rebuilt by expansionBatchLease carries it as data across the
//     queue boundary instead of relying on the engine-namespace fallback.

// nsRecordingBodyExecutor wraps a body executor and records the namespace of
// the ctx it was invoked with, then delegates so the batch flow is otherwise
// untouched.
type nsRecordingBodyExecutor struct {
	inner *echoBodyExecutor
	mu    sync.Mutex
	seen  []namespace.Namespace
}

func (x *nsRecordingBodyExecutor) ExecuteBatchBody(ctx context.Context, req BatchBodyRequest) ([]BatchItemResult, error) {
	x.mu.Lock()
	x.seen = append(x.seen, namespace.FromContext(ctx))
	x.mu.Unlock()
	return x.inner.ExecuteBatchBody(ctx, req)
}

func (x *nsRecordingBodyExecutor) namespaces() []namespace.Namespace {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]namespace.Namespace(nil), x.seen...)
}

// nsRecordingState records the ctx namespace of the state reads ExecuteBatch's
// bookkeeping performs (active check, graph load, lease fence) and delegates
// everything else to the fake.
type nsRecordingState struct {
	*fakeState
	mu   sync.Mutex
	seen []namespace.Namespace
}

func (s *nsRecordingState) record(ctx context.Context) {
	s.mu.Lock()
	s.seen = append(s.seen, namespace.FromContext(ctx))
	s.mu.Unlock()
}

func (s *nsRecordingState) reset() {
	s.mu.Lock()
	s.seen = nil
	s.mu.Unlock()
}

func (s *nsRecordingState) namespaces() []namespace.Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]namespace.Namespace(nil), s.seen...)
}

func (s *nsRecordingState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.record(ctx)
	return s.fakeState.GetExecution(ctx, id)
}

func (s *nsRecordingState) LoadGraph(ctx context.Context, id types.ExecutionID) (*graph.Graph, error) {
	s.record(ctx)
	return s.fakeState.LoadGraph(ctx, id)
}

func (s *nsRecordingState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.record(ctx)
	return s.fakeState.GetNode(ctx, id, name)
}

// TestExecuteBatchKeepsStateOpsOnTheTaskContext drives a real map expansion
// (submit -> map node -> two batch tasks -> ExecuteBatch) on an engine pinned
// to tenant-b, with the task ctx coming from context.Background() — exactly
// the shape a queue worker hands the dispatcher. It asserts the two halves of
// the split at once:
//
//   - every state read ExecuteBatch performs ran with the TASK ctx
//     (Background -> Default), because that is the namespace the execution's
//     records live under;
//   - the body runtime was invoked with the LEASE namespace (tenant-b), which
//     is what seeds the inner engine and its $supplies resolution.
//
// Mutation: move the re-anchor back onto ctx itself in ExecuteBatch (the
// pre-split shape) — the state reads record "tenant-b" and the first assertion
// fails. Dropping the re-anchor from runBatchBody — the body records
// "default" and the second one fails.
func TestExecuteBatchKeepsStateOpsOnTheTaskContext(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "ns-split",
		Nodes: []types.NodeDef{
			mapNodeWithEchoBody(5),
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	body := &nsRecordingBodyExecutor{inner: newEchoBodyExecutor()}
	state := &nsRecordingState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &sizedMapHandler{batchSize: 5},
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, WithNamespace("tenant-b"), WithBatchBodyExecutor(body))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })

	ctx := context.Background()
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])

	batchTasks := queue.Drain()
	if len(batchTasks) != 2 {
		t.Fatalf("batch tasks = %d, want 2 (10 items at batch_size 5)", len(batchTasks))
	}

	state.reset()
	for _, bt := range batchTasks {
		if err := eng.ExecuteBatch(ctx, bt); err != nil {
			t.Fatalf("ExecuteBatch: %v", err)
		}
	}

	// The first state read in ExecuteBatch is the active check; if a refactor
	// ever skips all of them, the assertions below would pass vacuously.
	stateSeen := state.namespaces()
	if len(stateSeen) == 0 {
		t.Fatal("no state reads recorded during ExecuteBatch — the test observed nothing")
	}
	for _, ns := range stateSeen {
		if ns != namespace.Default {
			t.Fatalf("state operation ran with ctx namespace %q, want %q (the task ctx). "+
				"The lease namespace names the tenant the BODY resolves against; state records "+
				"live under the namespace the execution was submitted with, and re-anchoring "+
				"state reads points them at a namespace where the execution does not exist",
				ns, namespace.Default)
		}
	}

	bodySeen := body.namespaces()
	if len(bodySeen) == 0 {
		t.Fatal("body runtime was never invoked")
	}
	for _, ns := range bodySeen {
		if ns != namespace.Namespace("tenant-b") {
			t.Fatalf("body runtime ran with ctx namespace %q, want tenant-b (the lease namespace). "+
				"The inner engine and its $supplies resolution seed from this ctx", ns)
		}
	}

	// The expansion still completed: the map node produced the flattened
	// output, proving the split did not break the batch barrier.
	out, err := state.GetOutput(ctx, execID, "m")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	if count, _ := out["count"].(int); count != 10 {
		t.Fatalf("map count = %v, want 10", out["count"])
	}
}

// TestExpansionBatchTaskCarriesTheParentNamespace pins the batch payload as
// the carrier of the parent lease's namespace: ExecuteBatch rebuilds the lease
// from this payload, and without the field that rebuild always produced an
// empty Namespace — leaving the lease-namespace branch of the body re-anchor
// dead and the engine-namespace fallback as the only live path.
//
// Mutation: drop the payload["parent_namespace"] write in expansionBatchTask —
// the stamped sub-test fails. Default the rebuild to namespace.Default — the
// unstamped and pre-field sub-tests fail: a non-empty value here would
// override the engine-namespace fallback in runBatchBody.
func TestExpansionBatchTaskCarriesTheParentNamespace(t *testing.T) {
	newParentLease := func(ns namespace.Namespace) *TaskLease {
		return &TaskLease{
			LeaseID:    "l1",
			LeaseToken: "t1",
			Namespace:  ns,
			Task: Task{
				ExecutionID: "e1",
				NodeName:    "m",
				NodeIdx:     0,
			},
		}
	}

	t.Run("stamped parent round-trips", func(t *testing.T) {
		task := expansionBatchTask(newParentLease("tenant-b"), "e1/sub/0", 0, []any{"a"}, nil)
		if got := task.Payload.Data["parent_namespace"]; got != "tenant-b" {
			t.Fatalf("payload parent_namespace = %v, want tenant-b", got)
		}
		rebuilt, _, _, err := expansionBatchLease(&task)
		if err != nil {
			t.Fatalf("expansionBatchLease: %v", err)
		}
		if rebuilt.Namespace != "tenant-b" {
			t.Fatalf("rebuilt lease namespace = %q, want tenant-b", rebuilt.Namespace)
		}
	})

	t.Run("unstamped parent stays empty and absent", func(t *testing.T) {
		task := expansionBatchTask(newParentLease(""), "e1/sub/0", 0, []any{"a"}, nil)
		if _, present := task.Payload.Data["parent_namespace"]; present {
			t.Fatal("unstamped parent wrote a parent_namespace field; it must stay absent so the engine-namespace fallback decides")
		}
		rebuilt, _, _, err := expansionBatchLease(&task)
		if err != nil {
			t.Fatalf("expansionBatchLease: %v", err)
		}
		if rebuilt.Namespace != "" {
			t.Fatalf("rebuilt lease namespace = %q, want empty (not %q): a non-empty value would override the engine-namespace fallback in runBatchBody",
				rebuilt.Namespace, namespace.Default)
		}
	})

	t.Run("pre-field payload rebuilds empty", func(t *testing.T) {
		// A batch queued by a build that predates the field: the payload has
		// no parent_namespace key at all.
		task := expansionBatchTask(newParentLease("tenant-b"), "e1/sub/0", 0, []any{"a"}, nil)
		delete(task.Payload.Data, "parent_namespace")
		rebuilt, _, _, err := expansionBatchLease(&task)
		if err != nil {
			t.Fatalf("expansionBatchLease: %v", err)
		}
		if rebuilt.Namespace != "" {
			t.Fatalf("rebuilt lease namespace = %q, want empty for a pre-field payload", rebuilt.Namespace)
		}
	})
}
