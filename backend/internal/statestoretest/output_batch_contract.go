package statestoretest

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// BatchOutputBackend is one fresh backend under test.
//
// Namespaced reports whether the backend partitions outputs by the namespace
// carried in ctx. The in-memory backend has no namespace dimension, so its
// caller passes false and the isolation subtest does not run there; the
// execution-ID dimension is covered on every backend. The flag is explicit
// rather than probed from behavior on purpose: probing would turn a backend
// whose namespace isolation REGRESSED into a backend that merely stops running
// the subtest, which is the one failure this contract exists to catch.
type BatchOutputBackend struct {
	Store      engine.StateStore
	Namespaced bool
}

const batchOutputContractNamespace namespace.Namespace = "tenant-batch-output-contract"

// RunBatchOutputContract exercises engine.BatchOutputReader against a backend:
// one call must return every stored output it was asked for, omit the names
// that have none, tolerate duplicate and empty name lists, and agree name by
// name with GetOutput. The batch form exists to collapse the engine's fan-in
// and $nodes reads from one round trip per name to one, so a backend that
// silently drops a name would starve a node's input rather than fail.
func RunBatchOutputContract(t *testing.T, manifest *Manifest, newBackend func(*testing.T) BatchOutputBackend) {
	t.Helper()
	manifest.Report(t)
	if !manifest.SkipUnlessDeclared(t, CapabilityBatchOutputReader) {
		return
	}
	backend := newBackend(t)
	reader, ok := backend.Store.(engine.BatchOutputReader)
	if !ok {
		t.Fatalf("backend %q DECLARES capability %s (engine.BatchOutputReader) but %T does not implement it",
			manifest.Backend(), CapabilityBatchOutputReader, backend.Store)
	}
	ctx := namespace.WithNamespace(context.Background(), batchOutputContractNamespace)

	// put seeds one output and fails the subtest on error. Each case uses its
	// own execution ID so a case cannot read another case's leftovers.
	put := func(t *testing.T, ctx context.Context, id types.ExecutionID, name string, data map[string]any) {
		t.Helper()
		if err := backend.Store.PutOutput(ctx, id, name, data); err != nil {
			t.Fatalf("PutOutput(%q/%q) error = %v", id, name, err)
		}
	}

	t.Run("ReadsEveryStoredOutput", func(t *testing.T) {
		id := types.ExecutionID("exec-batch-hit")
		put(t, ctx, id, "alpha", map[string]any{"value": "a"})
		put(t, ctx, id, "beta", map[string]any{"value": "b"})

		got, err := reader.GetOutputs(ctx, id, []string{"alpha", "beta"})
		if err != nil {
			t.Fatalf("GetOutputs() error = %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("GetOutputs() returned %d entries, want 2: %#v", len(got), got)
		}
		for _, name := range []string{"alpha", "beta"} {
			want := map[string]any{"value": name[:1]}
			if !reflect.DeepEqual(got[name], want) {
				t.Errorf("GetOutputs()[%q] = %#v, want %#v", name, got[name], want)
			}
			// The batch form must agree with the per-name read; the engine
			// picks between them based on the backend, not on the call site.
			single, err := backend.Store.GetOutput(ctx, id, name)
			if err != nil {
				t.Fatalf("GetOutput(%q) error = %v", name, err)
			}
			if !reflect.DeepEqual(got[name], single) {
				t.Errorf("GetOutputs()[%q] = %#v but GetOutput = %#v; the batch and single reads must agree",
					name, got[name], single)
			}
		}
	})

	t.Run("MissingNamesAreAbsent", func(t *testing.T) {
		id := types.ExecutionID("exec-batch-miss")
		put(t, ctx, id, "present", map[string]any{"ok": true})

		got, err := reader.GetOutputs(ctx, id, []string{"present", "never-executed"})
		if err != nil {
			t.Fatalf("GetOutputs() error = %v", err)
		}
		if _, ok := got["never-executed"]; ok {
			t.Fatalf("GetOutputs() contains %q with value %#v; a name with no stored output must be "+
				"ABSENT, the same absence GetOutput reports as nil, nil",
				"never-executed", got["never-executed"])
		}
		if _, ok := got["present"]; !ok {
			t.Fatal("GetOutputs() dropped the stored output alongside the missing one")
		}
	})

	t.Run("EmptyNamesReturnsEmptyResult", func(t *testing.T) {
		for _, names := range [][]string{nil, {}} {
			got, err := reader.GetOutputs(ctx, types.ExecutionID("exec-batch-empty"), names)
			if err != nil {
				t.Fatalf("GetOutputs(%v) error = %v", names, err)
			}
			if len(got) != 0 {
				t.Fatalf("GetOutputs(%v) = %#v, want an empty result", names, got)
			}
		}
	})

	t.Run("DuplicateNamesAreReadOnce", func(t *testing.T) {
		id := types.ExecutionID("exec-batch-dup")
		put(t, ctx, id, "alpha", map[string]any{"value": "a"})

		got, err := reader.GetOutputs(ctx, id, []string{"alpha", "alpha", "alpha"})
		if err != nil {
			t.Fatalf("GetOutputs() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("GetOutputs() returned %d entries for one distinct name: %#v", len(got), got)
		}
		if !reflect.DeepEqual(got["alpha"], map[string]any{"value": "a"}) {
			t.Fatalf("GetOutputs()[alpha] = %#v, want value a", got["alpha"])
		}
	})

	t.Run("ScopesOutputsToTheirExecution", func(t *testing.T) {
		id := types.ExecutionID("exec-batch-scope")
		other := types.ExecutionID("exec-batch-scope-other")
		put(t, ctx, id, "mine", map[string]any{"owner": "self"})
		put(t, ctx, other, "theirs", map[string]any{"owner": "other"})

		got, err := reader.GetOutputs(ctx, id, []string{"mine", "theirs"})
		if err != nil {
			t.Fatalf("GetOutputs() error = %v", err)
		}
		if _, ok := got["theirs"]; ok {
			t.Fatal("GetOutputs() returned another execution's output under the same name")
		}
		if _, ok := got["mine"]; !ok {
			t.Fatal("GetOutputs() dropped this execution's output")
		}
	})

	t.Run("PreservesNonStringJSONValues", func(t *testing.T) {
		id := types.ExecutionID("exec-batch-values")
		// Every JSON value shape, all in float64 for numbers so a JSON backend
		// and the in-memory backend decode to the same Go values. The control
		// bytes exercise the one payload shape a node-level capture produces
		// that is not printable text (and, on the Redis backend, the path that
		// may hand back a binary compressed frame for decodeOutputValue).
		payload := map[string]any{
			"string":  "plain",
			"number":  float64(42),
			"boolean": true,
			"null":    nil,
			"nested":  map[string]any{"list": []any{float64(1), "two", false}},
			"binary":  "\x00\x01\x02\x1f\x7f",
		}
		put(t, ctx, id, "shapes", payload)

		got, err := reader.GetOutputs(ctx, id, []string{"shapes"})
		if err != nil {
			t.Fatalf("GetOutputs() error = %v", err)
		}
		if !reflect.DeepEqual(got["shapes"], payload) {
			t.Fatalf("GetOutputs()[shapes] = %#v, want %#v", got["shapes"], payload)
		}
	})

	t.Run("ScopesOutputsByNamespace", func(t *testing.T) {
		if !backend.Namespaced {
			t.Skip("backend has no namespace dimension; execution-ID scoping is covered by ScopesOutputsToTheirExecution")
		}
		id := types.ExecutionID("exec-batch-ns")
		ctxA := namespace.WithNamespace(ctx, "tenant-batch-a")
		ctxB := namespace.WithNamespace(ctx, "tenant-batch-b")
		put(t, ctxA, id, "shared", map[string]any{"who": "a"})
		put(t, ctxA, id, "only-a", map[string]any{"who": "a"})
		put(t, ctxB, id, "shared", map[string]any{"who": "b"})

		gotA, err := reader.GetOutputs(ctxA, id, []string{"shared", "only-a"})
		if err != nil {
			t.Fatalf("GetOutputs(ns a) error = %v", err)
		}
		if gotA["shared"]["who"] != "a" {
			t.Fatalf("GetOutputs(ns a)[shared] = %#v, want who a", gotA["shared"])
		}
		gotB, err := reader.GetOutputs(ctxB, id, []string{"shared", "only-a"})
		if err != nil {
			t.Fatalf("GetOutputs(ns b) error = %v", err)
		}
		if gotB["shared"]["who"] != "b" {
			t.Fatalf("GetOutputs(ns b)[shared] = %#v, want who b", gotB["shared"])
		}
		if _, ok := gotB["only-a"]; ok {
			t.Fatal("GetOutputs(ns b) returned an output stored only under another namespace")
		}
	})
}
