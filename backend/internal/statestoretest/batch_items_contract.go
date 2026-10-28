package statestoretest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// BatchItemsBackend is one fresh backend under test.
//
// InlineMax is the largest encoded items array, in bytes, the backend keeps
// inline: the contract builds one array at or under it and one well over it, so
// the threshold is the backend's configuration, not a guess baked in here.
//
// Namespaced reports whether stored items are partitioned by the namespace on
// ctx. It is explicit for the same reason as BatchOutputBackend.Namespaced:
// probing would turn a backend whose isolation regressed into one that merely
// stops running the isolation case.
type BatchItemsBackend struct {
	Store      engine.StateStore
	InlineMax  int
	Namespaced bool
}

const (
	batchItemsContractNamespace namespace.Namespace = "tenant-batch-items-contract"
	batchItemsForeignNamespace  namespace.Namespace = "tenant-batch-items-foreign"
)

// RunBatchItemsContract exercises engine.BatchItemsStore against a backend.
//
// The engine trusts two answers from the capability without checking them. An
// externalized=false is taken as "keep the batch inline", so a backend that
// stored the array anyway leaks it, and one that answered false for a large
// array silently disables the feature. A resolved array is sliced by the
// batch's [start, end) window, so a backend that returned a reordered or
// truncated array hands each batch the wrong items with no error anywhere. And
// a ResolveItems failure that is not marked permanent is retried forever,
// because the engine must not import the blob layer and can only ask
// types.IsPermanent.
func RunBatchItemsContract(t *testing.T, manifest *Manifest, newBackend func(*testing.T) BatchItemsBackend) {
	t.Helper()
	manifest.Report(t)
	if !manifest.SkipUnlessDeclared(t, CapabilityBatchItemsStore) {
		return
	}
	backend := newBackend(t)
	store, ok := backend.Store.(engine.BatchItemsStore)
	if !ok {
		t.Fatalf("backend %q DECLARES capability %s (engine.BatchItemsStore) but %T does not implement it",
			manifest.Backend(), CapabilityBatchItemsStore, backend.Store)
	}
	ctx := namespace.WithNamespace(context.Background(), batchItemsContractNamespace)

	externalize := func(t *testing.T, ctx context.Context, execID string, items []any) json.RawMessage {
		t.Helper()
		ref, externalized, err := store.ExternalizeItems(ctx, execID, items)
		if err != nil {
			t.Fatalf("ExternalizeItems(%q) error = %v", execID, err)
		}
		if !externalized || len(ref) == 0 {
			t.Fatalf("ExternalizeItems(%q) = (%s, %v), want a reference for an array over InlineMax %d",
				execID, ref, externalized, backend.InlineMax)
		}
		return ref
	}

	t.Run("SmallArrayStaysInline", func(t *testing.T) {
		items := batchItemsWithin(t, backend.InlineMax)
		ref, externalized, err := store.ExternalizeItems(ctx, "exec-batch-items-small", items)
		if err != nil || externalized || ref != nil {
			t.Fatalf("ExternalizeItems(small) = (%s, %v, %v), want (nil, false, nil) -- "+
				"the engine keeps the batch inline on false, so anything stored here is never read",
				ref, externalized, err)
		}
	})

	t.Run("LargeArrayRoundTrips", func(t *testing.T) {
		items := batchItemsOver(backend.InlineMax, "round-trip")
		ref := externalize(t, ctx, "exec-batch-items-round-trip", items)
		got, err := store.ResolveItems(ctx, ref)
		if err != nil {
			t.Fatalf("ResolveItems() error = %v", err)
		}
		if !reflect.DeepEqual(got, items) {
			t.Fatalf("ResolveItems() returned %d items, want the %d stored in order -- "+
				"each batch slices this array by index, so any change hands it the wrong items",
				len(got), len(items))
		}
	})

	t.Run("ResolveIsRepeatable", func(t *testing.T) {
		// A retried batch, and every batch after an engine cache miss, resolves
		// the same reference again.
		items := batchItemsOver(backend.InlineMax, "repeat")
		ref := externalize(t, ctx, "exec-batch-items-repeat", items)
		for i := range 3 {
			got, err := store.ResolveItems(ctx, ref)
			if err != nil || !reflect.DeepEqual(got, items) {
				t.Fatalf("ResolveItems() read %d = (%d items, %v), want the stored array", i, len(got), err)
			}
		}
	})

	t.Run("DistinctArraysKeepDistinctContents", func(t *testing.T) {
		// Two executions mapping different arrays must not read each other's,
		// however the backend chooses to key or deduplicate them.
		first := batchItemsOver(backend.InlineMax, "first")
		second := batchItemsOver(backend.InlineMax, "second")
		firstRef := externalize(t, ctx, "exec-batch-items-first", first)
		secondRef := externalize(t, ctx, "exec-batch-items-second", second)
		for _, tc := range []struct {
			ref  json.RawMessage
			want []any
		}{{firstRef, first}, {secondRef, second}} {
			got, err := store.ResolveItems(ctx, tc.ref)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ResolveItems(%s) = (%d items, %v), want its own array", tc.ref, len(got), err)
			}
		}
	})

	t.Run("UnreadableReferenceIsPermanent", func(t *testing.T) {
		for _, ref := range []json.RawMessage{
			json.RawMessage(`"not a reference"`),
			json.RawMessage(`{"unexpected":true}`),
			json.RawMessage(`[1,2,3]`),
		} {
			_, err := store.ResolveItems(ctx, ref)
			if err == nil {
				t.Fatalf("ResolveItems(%s) error = nil, want a failure", ref)
			}
			if !types.IsPermanent(err) {
				t.Fatalf("ResolveItems(%s) error = %v, want types.IsPermanent -- "+
					"re-reading the same reference cannot succeed, and an unmarked error is retried forever", ref, err)
			}
		}
	})

	t.Run("ForeignNamespaceIsRefused", func(t *testing.T) {
		if !backend.Namespaced {
			t.Skipf("backend %q does not partition batch items by namespace", manifest.Backend())
		}
		items := batchItemsOver(backend.InlineMax, "isolated")
		ref := externalize(t, ctx, "exec-batch-items-isolated", items)
		foreign := namespace.WithNamespace(context.Background(), batchItemsForeignNamespace)
		got, err := store.ResolveItems(foreign, ref)
		if err == nil {
			t.Fatalf("ResolveItems() from namespace %q returned %d items of namespace %q, want a refusal",
				batchItemsForeignNamespace, len(got), batchItemsContractNamespace)
		}
		if !types.IsPermanent(err) {
			t.Fatalf("ResolveItems() from a foreign namespace error = %v, want types.IsPermanent", err)
		}
	})
}

// batchItemsWithin returns a non-empty array whose JSON encoding fits in
// inlineMax bytes, failing the test if the backend's threshold is too small to
// hold any array at all.
func batchItemsWithin(t *testing.T, inlineMax int) []any {
	t.Helper()
	items := []any{"a"}
	if data, _ := json.Marshal(items); len(data) > inlineMax {
		t.Fatalf("InlineMax %d is smaller than the smallest array (%d bytes); give the contract a larger threshold",
			inlineMax, len(data))
	}
	return items
}

// batchItemsOver returns an array whose JSON encoding exceeds inlineMax by a
// wide margin. Its elements mix the shapes map items carry -- objects, strings,
// numbers, booleans, nulls -- and the numbers are float64, which is what any
// JSON round trip decodes them as. label makes the contents unique per case.
func batchItemsOver(inlineMax int, label string) []any {
	pad := strings.Repeat("x", 64)
	var items []any
	for size := 0; size <= 2*inlineMax+256; {
		i := len(items)
		item := map[string]any{
			"id":    fmt.Sprintf("%s-%d", label, i),
			"n":     float64(i),
			"even":  i%2 == 0,
			"none":  nil,
			"pad":   pad,
			"inner": []any{float64(i), label},
		}
		items = append(items, item)
		data, _ := json.Marshal(item)
		size += len(data) + 1
	}
	return items
}
