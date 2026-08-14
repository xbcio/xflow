package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// fakeAuditStore is a minimal store.Store that counts CreateExecution calls.
type fakeAuditStore struct {
	store.Store
	createCount int
}

func (f *fakeAuditStore) CreateExecution(_ context.Context, _ *store.ExecutionRecord) error {
	f.createCount++
	return nil
}

func testTransientGraph() *graph.Graph {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "transient-workflow",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Options: &types.WorkflowOptions{
			Transient:              true,
			TransientTTL:           2 * time.Minute,
			TransientCompletionTTL: 30 * time.Second,
		},
	})
	if err != nil {
		panic(err)
	}
	return g
}

func testDurableGraph() *graph.Graph {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "durable-workflow",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		panic(err)
	}
	return g
}

// TestPerWorkflowTransient_GraphCompilesTransientOptions verifies that
// Transient/TransientTTL/TransientCompletionTTL are extracted from
// WorkflowOptions and accessible on the compiled Graph.
func TestPerWorkflowTransient_GraphCompilesTransientOptions(t *testing.T) {
	g := testTransientGraph()
	if !g.Transient() {
		t.Fatal("expected Transient()=true on compiled graph")
	}
	if g.TransientTTL() != 2*time.Minute {
		t.Fatalf("TransientTTL() = %v, want 2m", g.TransientTTL())
	}
	if g.TransientCompletionTTL() != 30*time.Second {
		t.Fatalf("TransientCompletionTTL() = %v, want 30s", g.TransientCompletionTTL())
	}

	dg := testDurableGraph()
	if dg.Transient() {
		t.Fatal("expected Transient()=false on durable graph")
	}
}

// TestPerWorkflowTransient_SkipsSQLAudit verifies that a per-workflow
// transient execution skips SQL audit while a durable execution in the same
// store does not.
func TestPerWorkflowTransient_SkipsSQLAudit(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	// Use a fake store.Store that records calls.
	fakeDB := &fakeAuditStore{}
	state := New(rdb, fakeDB, time.Hour)
	// Global transient is OFF.
	state.transient = false

	ctx := context.Background()

	// Submit a transient workflow — should skip SQL.
	transientID := types.ExecutionID("exec-per-wf-transient")
	tg := testTransientGraph()
	ctx1 := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(ctx1, &engine.ExecutionSnapshot{
		ID:     transientID,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution (transient) error = %v", err)
	}

	// Submit a durable workflow — should hit SQL.
	durableID := types.ExecutionID("exec-per-wf-durable")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     durableID,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution (durable) error = %v", err)
	}

	if fakeDB.createCount != 1 {
		t.Fatalf("expected 1 SQL CreateExecution call (durable only), got %d", fakeDB.createCount)
	}
}

// TestPerWorkflowTransient_UsesWorkflowTTL verifies that per-workflow transient
// executions use the workflow's declared TTL rather than the default exec TTL.
func TestPerWorkflowTransient_UsesWorkflowTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour) // default TTL = 1h
	state.transient = false            // global transient OFF

	ctx := context.Background()
	id := types.ExecutionID("exec-per-wf-ttl")
	tg := testTransientGraph() // TTL = 2 min

	ctx1 := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(ctx1, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
		Params: map[string]any{"k": "v"},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Structural keys should have the workflow's transient TTL (2m), not 1h.
	key := execKey(namespace.Default, id, "status")
	ttl := mr.TTL(key)
	// Allow a small tolerance for test execution time.
	if ttl > 2*time.Minute || ttl < time.Minute {
		t.Fatalf("structural key TTL = %v, want ~2m", ttl)
	}
}

// TestPerWorkflowTransient_ShortenCompletionTTL verifies that per-workflow
// transient executions get their completion TTL shortened correctly.
func TestPerWorkflowTransient_ShortenCompletionTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-per-wf-completion-ttl")
	tg := testTransientGraph() // completionTTL = 30s

	ctx1 := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(ctx1, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Complete the execution.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	// After completion, structural keys should have been shortened to ~30s.
	key := execKey(namespace.Default, id, "status")
	ttl := mr.TTL(key)
	if ttl > 30*time.Second || ttl <= 0 {
		t.Fatalf("completed key TTL = %v, want ~30s", ttl)
	}
}

// TestPerWorkflowTransient_GlobalTransientOffPerWorkflowStillWorks confirms
// that per-workflow transient works even when the global transient is off.
func TestPerWorkflowTransient_GlobalTransientOffPerWorkflowStillWorks(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = false // explicitly OFF

	ctx := context.Background()
	id := types.ExecutionID("exec-per-wf-override")

	// isTransient should be false before the execution is admitted.
	if state.isTransient(ctx, id) {
		t.Fatal("expected isTransient=false before marking")
	}

	// Admit through the real submission path -- the marker is written by
	// createExecution from the context hint, not by a setter a test can call.
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           5 * time.Minute,
		CompletionTTL: time.Minute,
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	if !state.isTransient(ctx, id) {
		t.Fatal("expected isTransient=true after marking")
	}
	if state.getTransientTTL(ctx, id) != 5*time.Minute {
		t.Fatalf("getTransientTTL = %v, want 5m", state.getTransientTTL(ctx, id))
	}
	if state.getTransientCompletionTTL(ctx, id) != time.Minute {
		t.Fatalf("getTransientCompletionTTL = %v, want 1m", state.getTransientCompletionTTL(ctx, id))
	}
}

// TestPerWorkflowTransient_ContextPropagation verifies the engine's
// WithExecutionTransient/ExecutionTransientFromContext round-trip.
func TestPerWorkflowTransient_ContextPropagation(t *testing.T) {
	ctx := context.Background()

	// No hint attached.
	if _, ok := engine.ExecutionTransientFromContext(ctx); ok {
		t.Fatal("expected no transient hint on bare context")
	}

	// Attach hint.
	ctx = engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           3 * time.Minute,
		CompletionTTL: 45 * time.Second,
	})
	hint, ok := engine.ExecutionTransientFromContext(ctx)
	if !ok {
		t.Fatal("expected transient hint from context")
	}
	if hint.TTL != 3*time.Minute {
		t.Fatalf("hint.TTL = %v, want 3m", hint.TTL)
	}
	if hint.CompletionTTL != 45*time.Second {
		t.Fatalf("hint.CompletionTTL = %v, want 45s", hint.CompletionTTL)
	}
}

// TestPerWorkflowTransient_GraphJSONRoundTrip verifies that transient fields
// survive a JSON marshal/unmarshal round-trip (as happens via Redis persistence).
func TestPerWorkflowTransient_GraphJSONRoundTrip(t *testing.T) {
	g := testTransientGraph()
	data, err := g.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error = %v", err)
	}
	var g2 graph.Graph
	if err := g2.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON error = %v", err)
	}
	if !g2.Transient() {
		t.Fatal("expected Transient()=true after round-trip")
	}
	if g2.TransientTTL() != 2*time.Minute {
		t.Fatalf("TransientTTL() = %v after round-trip, want 2m", g2.TransientTTL())
	}
	if g2.TransientCompletionTTL() != 30*time.Second {
		t.Fatalf("TransientCompletionTTL() = %v after round-trip, want 30s", g2.TransientCompletionTTL())
	}
}
