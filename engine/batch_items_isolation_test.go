package engine

// Isolation of the stored items array. A script body is handed $items, $item
// and the item's own maps and slices without a copy, and goja writes through
// them, so whatever the engine hands a batch must be that batch's own copy: the
// process cache behind resolveBatchView serves every batch of a map node and
// every other execution that stores the same array.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"

	_ "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// objectItems returns n distinct items that carry nested maps and slices, the
// shapes a body can write into.
func objectItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = map[string]any{
			"id":     float64(i),
			"tag":    fmt.Sprintf("t%d", i),
			"nested": map[string]any{"v": float64(i)},
			"list":   []any{float64(i), "a"},
		}
	}
	return items
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// viewParts returns the items window and all_items of a batch view.
func viewParts(t *testing.T, view *Task) (window, all []any) {
	t.Helper()
	window, ok := view.Payload.Data["items"].([]any)
	if !ok {
		t.Fatalf("view items = %T, want []any", view.Payload.Data["items"])
	}
	all, ok = view.Payload.Data["all_items"].([]any)
	if !ok {
		t.Fatalf("view all_items = %T, want []any", view.Payload.Data["all_items"])
	}
	return window, all
}

// scribble writes into everything a script body can reach in a view: it
// reverses all_items, rewrites one item's fields, a nested map and a nested
// list, and overwrites a field of the first element of the batch window.
func scribble(t *testing.T, view *Task) {
	t.Helper()
	window, all := viewParts(t, view)
	slices.Reverse(all)
	for _, item := range all {
		m := item.(map[string]any)
		m["tag"] = "scribbled"
		m["nested"].(map[string]any)["v"] = float64(-1)
		m["list"].([]any)[0] = float64(-1)
	}
	window[0].(map[string]any)["id"] = float64(99)
}

// TestBatchItemsIsolationWindowAcrossBatches: batch 0's body rewrites its view in
// place -- sorting or reversing $items, editing $item and the maps inside it.
// Batch 1 slices its window out of the same stored array, so it must still see
// the array exactly as it was stored.
func TestBatchItemsIsolationWindowAcrossBatches(t *testing.T) {
	items := objectItems(6)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2), batchSize: 2})
	ctx := context.Background()
	wantAll := mustJSON(t, items)

	first, err := run.eng.resolveBatchView(ctx, run.batches[0])
	if err != nil {
		t.Fatalf("resolveBatchView(batch 0): %v", err)
	}
	scribble(t, first)

	second, err := run.eng.resolveBatchView(ctx, run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView(batch 1): %v", err)
	}
	window, all := viewParts(t, second)
	if got, want := mustJSON(t, window), mustJSON(t, items[2:4]); got != want {
		t.Errorf("batch 1 items after batch 0 rewrote its view = %s, want %s", got, want)
	}
	if got := mustJSON(t, all); got != wantAll {
		t.Errorf("batch 1 all_items after batch 0 rewrote its view = %s, want %s", got, wantAll)
	}
}

// TestBatchItemsIsolationAcrossViewsOfOneBatch: two views of the same batch do
// not share the array or the maps in it, and rewriting one leaves the cached
// original alone, which the third resolution proves by being served from the
// cache with the stored content.
func TestBatchItemsIsolationAcrossViewsOfOneBatch(t *testing.T) {
	items := objectItems(4)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2})
	ctx := context.Background()
	wantAll := mustJSON(t, items)

	a, err := run.eng.resolveBatchView(ctx, run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView(first): %v", err)
	}
	b, err := run.eng.resolveBatchView(ctx, run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView(second): %v", err)
	}
	_, allA := viewParts(t, a)
	allA[0].(map[string]any)["nested"].(map[string]any)["v"] = float64(-1)
	scribble(t, a)

	windowB, allB := viewParts(t, b)
	if got := mustJSON(t, allB); got != wantAll {
		t.Errorf("second view all_items after the first was rewritten = %s, want %s", got, wantAll)
	}
	if got, want := mustJSON(t, windowB), mustJSON(t, items[2:4]); got != want {
		t.Errorf("second view items after the first was rewritten = %s, want %s", got, want)
	}

	c, err := run.eng.resolveBatchView(ctx, run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView(third): %v", err)
	}
	windowC, allC := viewParts(t, c)
	if state.resolveCalls != 1 {
		t.Errorf("ResolveItems calls = %d, want 1: later views must come from the cache", state.resolveCalls)
	}
	if got := mustJSON(t, allC); got != wantAll {
		t.Errorf("cached array after two views were rewritten = %s, want %s", got, wantAll)
	}
	if got, want := mustJSON(t, windowC), mustJSON(t, items[2:4]); got != want {
		t.Errorf("cached window after two views were rewritten = %s, want %s", got, want)
	}
}

// TestBatchItemsIsolationConcurrentRewrites: two bodies of the same map node
// resolve their views and write into the maps in them at the same moment. With
// views that share the cached array those are concurrent writes to one Go map,
// which the race detector flags and the runtime can turn into a fatal error.
func TestBatchItemsIsolationConcurrentRewrites(t *testing.T) {
	items := objectItems(4)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2})
	wantAll := mustJSON(t, items)

	start := make(chan struct{})
	var ready, done sync.WaitGroup
	for range 2 {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			view, err := run.eng.resolveBatchView(context.Background(), run.batches[0])
			ready.Done()
			if err != nil {
				t.Errorf("resolveBatchView: %v", err)
				return
			}
			window, all := viewParts(t, view)
			<-start
			for i := range 2000 {
				all[1].(map[string]any)["tag"] = i
				all[1].(map[string]any)["nested"].(map[string]any)["v"] = float64(i)
				window[0].(map[string]any)["id"] = float64(i)
				// Yield so the two writers interleave instead of one finishing
				// before the other starts.
				runtime.Gosched()
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	view, err := run.eng.resolveBatchView(context.Background(), run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView after the rewrites: %v", err)
	}
	if _, all := viewParts(t, view); mustJSON(t, all) != wantAll {
		t.Errorf("all_items after concurrent rewrites = %s, want %s", mustJSON(t, all), wantAll)
	}
}

// scriptBodyExecutor stands in for the map body runtime with the part that
// matters here: each item runs a real xflow.script node (goja) whose globals are
// $item, $index and $items, bound the way the production map body binds them --
// $items is the request's AllItems, unchanged. The engine under test supplies
// the request; only the body's sub-graph machinery is replaced.
type scriptBodyExecutor struct{ code string }

func (x scriptBodyExecutor) ExecuteBatchBody(ctx context.Context, req BatchBodyRequest) ([]BatchItemResult, error) {
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		return nil, errors.New("xflow.script is not registered")
	}
	out := make([]BatchItemResult, 0, len(req.Items))
	for i, item := range req.Items {
		idx := globalItemIndex(req.BatchIndex, req.BatchSize, i)
		res, err := h.Execute(ctx, &types.Input{
			Params: map[string]any{"language": "js", "runtime": "goja", "code": x.code},
			Data:   map[string]any{"$item": item, "$index": idx, "$items": req.AllItems},
		})
		if err != nil {
			return nil, err
		}
		if res.Port == "error" {
			out = append(out, BatchItemResult{Index: idx, Err: fmt.Errorf("script: %v", res.Data["error"])})
			continue
		}
		out = append(out, BatchItemResult{Index: idx, Data: res.Data})
	}
	return out, nil
}

const (
	// readScript reports what the body sees of its own item and of the array.
	readScript = `var r = {id: $item.id, first: $items[0].id, tag2: $items[2].tag, n: $items[3].nested.v, len: $items.length};`
	// rewriteScript is readScript plus, for item 0 only, every kind of write a
	// script can make through goja into the Go data behind $items and $item.
	rewriteScript = readScript + `
if ($index === 0) {
  $items.sort(function (a, b) { return b.id - a.id; });
  $item.id = 99;
  $items[2].tag = "x";
  $items[3].nested.v = -1;
}`
)

// TestBatchItemsIsolationScriptBody: a map whose body is a goja script that
// sorts $items and edits $item and the maps inside $items produces the same
// results as the same map whose script only reads. Each batch holds one item, so
// batch 0's script rewrites its data after its own result is built and every
// later batch is the one that would see the damage.
func TestBatchItemsIsolationScriptBody(t *testing.T) {
	if _, ok := registry.Lookup("xflow.script"); !ok {
		t.Fatal("xflow.script is not registered")
	}
	run := func(code string) string {
		items := objectItems(4)
		r := expandShaped(t, newItemsState(), &shapedMapHandler{items: items, batches: chunked(items, 1, 1, 1, 1), batchSize: 1},
			WithBatchBodyExecutor(scriptBodyExecutor{code: code + "\nr;"}))
		out := runAllBatches(t, r, []int{0, 1, 2, 3})
		return mustJSON(t, out["results"])
	}
	want := run(readScript)
	if got := run(rewriteScript); got != want {
		t.Errorf("results with a rewriting script = %s\nwant (read-only script)       = %s", got, want)
	}
}
