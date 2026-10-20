package statestoretest

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ExecutionInputBackend is one fresh backend under test for the narrow
// execution-read contracts.
//
// Namespaced reports whether the backend partitions executions by the namespace
// carried in ctx. The in-memory backend has no namespace dimension, so its
// caller passes false and the isolation subtest does not run there; the
// execution-ID dimension is covered on every backend. The flag is explicit
// rather than probed from behavior for the same reason BatchOutputBackend's is:
// probing would turn a backend whose namespace isolation REGRESSED into a
// backend that merely stops running the subtest.
type ExecutionInputBackend struct {
	Store      engine.StateStore
	Namespaced bool
}

const executionInputContractNamespace namespace.Namespace = "tenant-execution-input-contract"

// RunExecutionInputContextContract exercises engine.ExecutionInputContextReader:
// the narrow read must reproduce the same status, params, runtime, scope and
// trace identity as GetExecution, report an absent execution as found=false, and
// keep executions in different namespaces apart.
//
// The equivalence assertions are the whole point. buildInput picks this read by
// capability, so a backend whose narrow answer differs from its snapshot would
// make a node's input depend on which backend it happened to run on — the exact
// class of silent divergence the shared contract exists to catch.
func RunExecutionInputContextContract(t *testing.T, manifest *Manifest, newBackend func(*testing.T) ExecutionInputBackend) {
	t.Helper()
	manifest.Report(t)
	if !manifest.SkipUnlessDeclared(t, CapabilityExecutionInputContextReader) {
		return
	}
	backend := newBackend(t)
	reader, ok := backend.Store.(engine.ExecutionInputContextReader)
	if !ok {
		t.Fatalf("backend %q DECLARES capability %s (engine.ExecutionInputContextReader) but %T does not implement it",
			manifest.Backend(), CapabilityExecutionInputContextReader, backend.Store)
	}
	ctx := namespace.WithNamespace(context.Background(), executionInputContractNamespace)

	// create seeds one execution and fails the subtest on error. Each case uses
	// its own execution ID so a case cannot read another case's leftovers.
	create := func(t *testing.T, ctx context.Context, snap *engine.ExecutionSnapshot) {
		t.Helper()
		if snap.Graph == nil {
			snap.Graph = ContractGraph()
		}
		if err := backend.Store.CreateExecution(ctx, snap); err != nil {
			t.Fatalf("CreateExecution(%q) error = %v", snap.ID, err)
		}
	}

	t.Run("MatchesGetExecution", func(t *testing.T) {
		id := types.ExecutionID("exec-input-context-match")
		create(t, ctx, &engine.ExecutionSnapshot{
			ID:      id,
			Status:  types.ExecutionStatusRunning,
			Params:  map[string]any{"claim_id": "c-1", "retry_budget": float64(2)},
			Runtime: &types.Runtime{Vars: map[string]any{"tenant": "acme", "limit": float64(3)}},
			Scope:   map[string]any{"$index": float64(7), "$item": map[string]any{"sku": "s-1"}},
			TraceID: "trace-input-context",
			SpanID:  "span-input-context",
		})

		snap, err := backend.Store.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		if snap == nil {
			t.Fatal("GetExecution() = nil for a created execution")
		}
		narrow, found, err := reader.GetExecutionInputContext(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext() error = %v", err)
		}
		if !found {
			t.Fatal("GetExecutionInputContext() found=false for a created execution")
		}
		if narrow == nil {
			t.Fatal("GetExecutionInputContext() returned a nil context with found=true")
		}
		if narrow.Status != snap.Status {
			t.Errorf("GetExecutionInputContext().Status = %q, GetExecution().Status = %q; the narrow and full reads must agree",
				narrow.Status, snap.Status)
		}
		if !reflect.DeepEqual(narrow.Params, snap.Params) {
			t.Errorf("GetExecutionInputContext().Params = %#v, GetExecution().Params = %#v; every root node's input.Data comes from this field",
				narrow.Params, snap.Params)
		}
		if !reflect.DeepEqual(narrow.Runtime, snap.Runtime) {
			t.Errorf("GetExecutionInputContext().Runtime = %#v, GetExecution().Runtime = %#v; $vars is built from this field",
				narrow.Runtime, snap.Runtime)
		}
		if !reflect.DeepEqual(narrow.Scope, snap.Scope) {
			t.Errorf("GetExecutionInputContext().Scope = %#v, GetExecution().Scope = %#v; execution-level expression roots come from this field",
				narrow.Scope, snap.Scope)
		}
		if narrow.TraceID != snap.TraceID || narrow.SpanID != snap.SpanID {
			t.Errorf("GetExecutionInputContext() trace identity = (%q, %q), GetExecution() = (%q, %q); a sub-execution re-submits with this pair",
				narrow.TraceID, narrow.SpanID, snap.TraceID, snap.SpanID)
		}
	})

	t.Run("StatusFollowsUpdates", func(t *testing.T) {
		id := types.ExecutionID("exec-input-context-status")
		create(t, ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning})
		if err := backend.Store.UpdateExecutionStatus(ctx, id, types.ExecutionStatusPaused, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus() error = %v", err)
		}

		snap, err := backend.Store.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		narrow, found, err := reader.GetExecutionInputContext(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext() error = %v", err)
		}
		if !found {
			t.Fatal("GetExecutionInputContext() found=false after a status update")
		}
		if snap == nil {
			t.Fatal("GetExecution() = nil after a status update")
		}
		if narrow.Status != types.ExecutionStatusPaused || narrow.Status != snap.Status {
			t.Fatalf("GetExecutionInputContext().Status = %q (GetExecution = %q), want %q: the narrow read must not serve a stale or cached status",
				narrow.Status, snap.Status, types.ExecutionStatusPaused)
		}
	})

	t.Run("AbsentExecution", func(t *testing.T) {
		id := types.ExecutionID("exec-input-context-absent")
		narrow, found, err := reader.GetExecutionInputContext(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext(absent) error = %v", err)
		}
		if found {
			t.Fatalf("GetExecutionInputContext(absent) found=true with %#v, want found=false", narrow)
		}
		if narrow != nil {
			t.Errorf("GetExecutionInputContext(absent) = %#v, want nil; found=false carries no context", narrow)
		}
		if snap, err := backend.Store.GetExecution(ctx, id); err != nil {
			t.Fatalf("GetExecution(absent) error = %v", err)
		} else if snap != nil {
			t.Errorf("GetExecution(absent) = %+v, want nil: the two methods must agree on absence", snap)
		}
	})

	t.Run("NilOptionalFieldsStayNil", func(t *testing.T) {
		id := types.ExecutionID("exec-input-context-minimal")
		create(t, ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning})

		snap, err := backend.Store.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		narrow, found, err := reader.GetExecutionInputContext(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext() error = %v", err)
		}
		if !found {
			t.Fatal("GetExecutionInputContext() found=false for a created execution with no params")
		}
		if narrow.Params != nil || narrow.Runtime != nil || narrow.Scope != nil {
			t.Errorf("GetExecutionInputContext() = (%#v, %#v, %#v), want nil params/runtime/scope that were never written",
				narrow.Params, narrow.Runtime, narrow.Scope)
		}
		if snap != nil {
			if (snap.Params != nil) != (narrow.Params != nil) ||
				(snap.Runtime != nil) != (narrow.Runtime != nil) ||
				(snap.Scope != nil) != (narrow.Scope != nil) {
				t.Errorf("narrow read nil-ness disagrees with GetExecution: narrow = (%#v, %#v, %#v), snapshot = (%#v, %#v, %#v)",
					narrow.Params, narrow.Runtime, narrow.Scope, snap.Params, snap.Runtime, snap.Scope)
			}
		}
	})

	t.Run("ScopesToExecution", func(t *testing.T) {
		mine := types.ExecutionID("exec-input-context-mine")
		other := types.ExecutionID("exec-input-context-other")
		create(t, ctx, &engine.ExecutionSnapshot{
			ID: mine, Status: types.ExecutionStatusRunning, Params: map[string]any{"owner": "self"},
		})
		create(t, ctx, &engine.ExecutionSnapshot{
			ID: other, Status: types.ExecutionStatusRunning, Params: map[string]any{"owner": "other"},
		})

		narrow, found, err := reader.GetExecutionInputContext(ctx, mine)
		if err != nil {
			t.Fatalf("GetExecutionInputContext(mine) error = %v", err)
		}
		if !found {
			t.Fatal("GetExecutionInputContext(mine) found=false")
		}
		if narrow.Params["owner"] != "self" {
			t.Fatalf("GetExecutionInputContext(mine).Params = %#v, want owner=self", narrow.Params)
		}
	})

	t.Run("ScopesToNamespace", func(t *testing.T) {
		if !backend.Namespaced {
			t.Skip("backend has no namespace dimension; execution-ID scoping is covered by ScopesToExecution")
		}
		id := types.ExecutionID("exec-input-context-ns")
		ctxA := namespace.WithNamespace(ctx, "tenant-input-a")
		ctxB := namespace.WithNamespace(ctx, "tenant-input-b")
		create(t, ctxA, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Params: map[string]any{"who": "a"}, TraceID: "trace-a",
		})
		create(t, ctxB, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Params: map[string]any{"who": "b"}, TraceID: "trace-b",
		})

		narrowA, foundA, err := reader.GetExecutionInputContext(ctxA, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext(ns a) error = %v", err)
		}
		if !foundA || narrowA.Params["who"] != "a" || narrowA.TraceID != "trace-a" {
			t.Fatalf("GetExecutionInputContext(ns a) = (%#v, %v), want who=a trace-a", narrowA, foundA)
		}
		narrowB, foundB, err := reader.GetExecutionInputContext(ctxB, id)
		if err != nil {
			t.Fatalf("GetExecutionInputContext(ns b) error = %v", err)
		}
		if !foundB || narrowB.Params["who"] != "b" || narrowB.TraceID != "trace-b" {
			t.Fatalf("GetExecutionInputContext(ns b) = (%#v, %v), want who=b trace-b", narrowB, foundB)
		}
	})
}

// RunExecutionTraceCarrierContract exercises engine.ExecutionTraceCarrierReader:
// a carrier written at creation is read back unchanged and agrees with the full
// snapshot, while an absent carrier and an absent execution both report
// found=false (the engine treats both as nil), and namespaces stay apart.
func RunExecutionTraceCarrierContract(t *testing.T, manifest *Manifest, newBackend func(*testing.T) ExecutionInputBackend) {
	t.Helper()
	manifest.Report(t)
	if !manifest.SkipUnlessDeclared(t, CapabilityExecutionTraceCarrierReader) {
		return
	}
	backend := newBackend(t)
	reader, ok := backend.Store.(engine.ExecutionTraceCarrierReader)
	if !ok {
		t.Fatalf("backend %q DECLARES capability %s (engine.ExecutionTraceCarrierReader) but %T does not implement it",
			manifest.Backend(), CapabilityExecutionTraceCarrierReader, backend.Store)
	}
	ctx := namespace.WithNamespace(context.Background(), executionInputContractNamespace)

	create := func(t *testing.T, ctx context.Context, snap *engine.ExecutionSnapshot) {
		t.Helper()
		if snap.Graph == nil {
			snap.Graph = ContractGraph()
		}
		if err := backend.Store.CreateExecution(ctx, snap); err != nil {
			t.Fatalf("CreateExecution(%q) error = %v", snap.ID, err)
		}
	}

	t.Run("MatchesGetExecution", func(t *testing.T) {
		id := types.ExecutionID("exec-carrier-match")
		want := map[string]string{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"tracestate":  "vendor=opaque",
		}
		create(t, ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, TraceCarrier: want,
		})

		snap, err := backend.Store.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		if snap == nil || len(snap.TraceCarrier) == 0 {
			t.Fatalf("GetExecution() = %+v, want the carrier written at creation", snap)
		}
		got, found, err := reader.GetExecutionTraceCarrier(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionTraceCarrier() error = %v", err)
		}
		if !found {
			t.Fatal("GetExecutionTraceCarrier() found=false for an execution with a carrier")
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetExecutionTraceCarrier() = %#v, want %#v", got, want)
		}
		if !reflect.DeepEqual(got, snap.TraceCarrier) {
			t.Errorf("GetExecutionTraceCarrier() = %#v, GetExecution().TraceCarrier = %#v; the narrow and full reads must agree",
				got, snap.TraceCarrier)
		}
	})

	t.Run("AbsentCarrier", func(t *testing.T) {
		id := types.ExecutionID("exec-carrier-absent")
		create(t, ctx, &engine.ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning})

		got, found, err := reader.GetExecutionTraceCarrier(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionTraceCarrier(no carrier) error = %v", err)
		}
		if found {
			t.Fatalf("GetExecutionTraceCarrier(no carrier) found=true with %#v, want false: "+
				"the carrier key is written only when tracing was enabled at submission", got)
		}
		if got != nil {
			t.Errorf("GetExecutionTraceCarrier(no carrier) = %#v, want nil", got)
		}
		if snap, err := backend.Store.GetExecution(ctx, id); err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		} else if snap == nil || len(snap.TraceCarrier) != 0 {
			t.Errorf("GetExecution() carrier = %#v, want none for the same execution", snap)
		}
	})

	t.Run("AbsentExecution", func(t *testing.T) {
		id := types.ExecutionID("exec-carrier-gone")
		got, found, err := reader.GetExecutionTraceCarrier(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionTraceCarrier(absent) error = %v", err)
		}
		if found || got != nil {
			t.Fatalf("GetExecutionTraceCarrier(absent) = (%#v, %v), want (nil, false)", got, found)
		}
	})

	t.Run("ScopesToNamespace", func(t *testing.T) {
		if !backend.Namespaced {
			t.Skip("backend has no namespace dimension; per-execution scoping is covered by MatchesGetExecution")
		}
		id := types.ExecutionID("exec-carrier-ns")
		ctxA := namespace.WithNamespace(ctx, "tenant-carrier-a")
		ctxB := namespace.WithNamespace(ctx, "tenant-carrier-b")
		create(t, ctxA, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
			TraceCarrier: map[string]string{"traceparent": "00-11111111111111111111111111111111-1111111111111111-01"},
		})
		create(t, ctxB, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning,
			TraceCarrier: map[string]string{"traceparent": "00-22222222222222222222222222222222-2222222222222222-01"},
		})

		gotA, foundA, err := reader.GetExecutionTraceCarrier(ctxA, id)
		if err != nil {
			t.Fatalf("GetExecutionTraceCarrier(ns a) error = %v", err)
		}
		if !foundA || gotA["traceparent"] != "00-11111111111111111111111111111111-1111111111111111-01" {
			t.Fatalf("GetExecutionTraceCarrier(ns a) = (%#v, %v), want namespace a's carrier", gotA, foundA)
		}
		gotB, foundB, err := reader.GetExecutionTraceCarrier(ctxB, id)
		if err != nil {
			t.Fatalf("GetExecutionTraceCarrier(ns b) error = %v", err)
		}
		if !foundB || gotB["traceparent"] != "00-22222222222222222222222222222222-2222222222222222-01" {
			t.Fatalf("GetExecutionTraceCarrier(ns b) = (%#v, %v), want namespace b's carrier", gotB, foundB)
		}
	})
}
