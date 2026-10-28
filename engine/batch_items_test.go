package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// itemsStore is an in-memory BatchItemsStore. It keeps the encoded array by
// digest, hands back a reference shaped like a blob reference, and counts calls
// so a test can tell a cache hit from a store read.
type itemsStore struct {
	guard            sync.Mutex
	blobs            map[string][]byte
	externalizeCalls int
	resolveCalls     int
	// decline makes ExternalizeItems answer externalized=false.
	decline        bool
	externalizeErr error
	resolveErr     error
}

func (s *itemsStore) ExternalizeItems(_ context.Context, _ string, items []any) (json.RawMessage, bool, error) {
	s.guard.Lock()
	defer s.guard.Unlock()
	s.externalizeCalls++
	if s.externalizeErr != nil {
		return nil, false, s.externalizeErr
	}
	if s.decline {
		return nil, false, nil
	}
	body, err := json.Marshal(items)
	if err != nil {
		return nil, false, err
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if s.blobs == nil {
		s.blobs = map[string][]byte{}
	}
	s.blobs[digest] = body
	ref := fmt.Sprintf(`{"ns":"default","sha256":%q,"size":%d,"stored":%d,"codec":"zstd"}`, digest, len(body), len(body))
	return json.RawMessage(ref), true, nil
}

func (s *itemsStore) ResolveItems(_ context.Context, ref json.RawMessage) ([]any, error) {
	s.guard.Lock()
	defer s.guard.Unlock()
	s.resolveCalls++
	if s.resolveErr != nil {
		return nil, s.resolveErr
	}
	var parsed struct {
		Digest string `json:"sha256"`
	}
	if err := json.Unmarshal(ref, &parsed); err != nil {
		return nil, err
	}
	body, ok := s.blobs[parsed.Digest]
	if !ok {
		return nil, fmt.Errorf("blob %q not found", parsed.Digest)
	}
	var items []any
	return items, json.Unmarshal(body, &items)
}

// itemsState is the fake state plus the BatchItemsStore capability.
type itemsState struct {
	*fakeState
	*itemsStore
}

var _ BatchItemsStore = (*itemsState)(nil)

func newItemsState() *itemsState {
	return &itemsState{fakeState: newFakeState(), itemsStore: &itemsStore{}}
}

// shapedMapHandler emits exactly the items and batches it was built with, so a
// test controls the batch geometry (equal, short last batch, uneven, or batches
// that do not add up to the items).
type shapedMapHandler struct {
	items     []any
	batches   [][]any
	batchSize int
}

func (h *shapedMapHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (h *shapedMapHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"items":       h.items,
		"batches":     h.batches,
		"batch_size":  h.batchSize,
		"total":       len(h.items),
		"batch_count": len(h.batches),
	}}, nil
}

// itemsRun is one map workflow that has been expanded into batch tasks.
type itemsRun struct {
	eng     *Engine
	state   StateStore
	queue   *fakeQueue
	execID  types.ExecutionID
	batches []*Task
}

// expandShaped submits the single map node workflow, runs the map node and
// returns the batch tasks it expanded into. The process-wide items cache is
// emptied first so no test sees another's entries.
func expandShaped(t *testing.T, state StateStore, h *shapedMapHandler, opts ...Option) *itemsRun {
	t.Helper()
	return expandShapedWith(t, state, h, nil, opts...)
}

// expandShapedWith is expandShaped for a map node the test needs to adjust (its
// parameters or its on_error) before the workflow is compiled.
func expandShapedWith(t *testing.T, state StateStore, h *shapedMapHandler, adjust func(*types.NodeDef), opts ...Option) *itemsRun {
	t.Helper()
	batchItemsCache.Purge()
	mapNode := mapNodeWithEchoBody(h.batchSize)
	if adjust != nil {
		adjust(&mapNode)
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "items-ref",
		Nodes: []types.NodeDef{
			mapNode,
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": h,
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, opts...)
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })

	execID, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}
	executeTask(t, eng, roots[0])
	return &itemsRun{eng: eng, state: state, queue: queue, execID: execID, batches: queue.Drain()}
}

// numberedItems returns n distinct items.
func numberedItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	return items
}

// chunked splits items into windows of the given lengths, in order.
func chunked(items []any, lengths ...int) [][]any {
	var out [][]any
	at := 0
	for _, n := range lengths {
		out = append(out, items[at:at+n])
		at += n
	}
	return out
}

// payloadInt reads one of a batch payload's integer fields.
func payloadInt(t *testing.T, task *Task, key string) int {
	t.Helper()
	n, err := batchPayloadInt(task.Payload.Data, key)
	if err != nil {
		t.Fatalf("payload %s: %v", key, err)
	}
	return n
}

// TestItemsRefSlicesHalfOpen: every batch task of an externalized expansion
// carries the reference plus its half-open window, and resolving it gives back
// exactly the batch the map node produced. It runs three geometries because the
// windows are derived by summing batch lengths, not by multiplying a batch size:
// equal batches, a short last batch and batches of different lengths.
func TestItemsRefSlicesHalfOpen(t *testing.T) {
	items := numberedItems(6)
	cases := []struct {
		name    string
		lengths []int
		size    int
	}{
		{"equal batches", []int{2, 2, 2}, 2},
		{"short last batch", []int{4, 2}, 4},
		{"different lengths", []int{1, 3, 2}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := newItemsState()
			batches := chunked(items, tc.lengths...)
			run := expandShaped(t, state, &shapedMapHandler{items: items, batches: batches, batchSize: tc.size})
			if len(run.batches) != len(batches) {
				t.Fatalf("batch tasks = %d, want %d", len(run.batches), len(batches))
			}
			if state.externalizeCalls != 1 {
				t.Fatalf("ExternalizeItems calls = %d, want 1 per expansion", state.externalizeCalls)
			}
			start := 0
			for i, task := range run.batches {
				data := task.Payload.Data
				if _, inline := data["items"]; inline {
					t.Errorf("batch %d still carries items", i)
				}
				if _, inline := data["all_items"]; inline {
					t.Errorf("batch %d still carries all_items", i)
				}
				if data[batchItemsRefKey] == nil {
					t.Fatalf("batch %d has no %s", i, batchItemsRefKey)
				}
				end := start + tc.lengths[i]
				if got := payloadInt(t, task, batchItemsStartKey); got != start {
					t.Errorf("batch %d items_start = %d, want %d", i, got, start)
				}
				if got := payloadInt(t, task, batchItemsEndKey); got != end {
					t.Errorf("batch %d items_end = %d, want %d", i, got, end)
				}
				view, err := run.eng.resolveBatchView(context.Background(), task)
				if err != nil {
					t.Fatalf("resolveBatchView(batch %d): %v", i, err)
				}
				got, _ := view.Payload.Data["items"].([]any)
				if fmt.Sprint(got) != fmt.Sprint(batches[i]) {
					t.Errorf("batch %d items = %v, want %v", i, got, batches[i])
				}
				all, _ := view.Payload.Data["all_items"].([]any)
				if fmt.Sprint(all) != fmt.Sprint(items) {
					t.Errorf("batch %d all_items = %v, want %v", i, all, items)
				}
				start = end
			}
		})
	}
}

// assertInlineBatches checks that every batch task still carries its items
// inline, with none of the reference keys.
func assertInlineBatches(t *testing.T, run *itemsRun, batches [][]any, items []any) {
	t.Helper()
	if len(run.batches) != len(batches) {
		t.Fatalf("batch tasks = %d, want %d", len(run.batches), len(batches))
	}
	for i, task := range run.batches {
		data := task.Payload.Data
		for _, key := range []string{batchItemsRefKey, batchItemsStartKey, batchItemsEndKey} {
			if _, present := data[key]; present {
				t.Errorf("batch %d carries %s, want it inline", i, key)
			}
		}
		if got, _ := data["items"].([]any); fmt.Sprint(got) != fmt.Sprint(batches[i]) {
			t.Errorf("batch %d items = %v, want %v", i, got, batches[i])
		}
		if all, _ := data["all_items"].([]any); fmt.Sprint(all) != fmt.Sprint(items) {
			t.Errorf("batch %d all_items = %v, want %v", i, all, items)
		}
	}
}

// TestUnevenBatchesStayInline: batches that do not add up to the items array
// (here one element is in no batch) are not windows of it, so the whole
// expansion stays inline and the store is never asked to hold the array.
func TestUnevenBatchesStayInline(t *testing.T) {
	items := numberedItems(6)
	batches := chunked(items, 2, 3) // 5 of 6 items are batched
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: batches, batchSize: 3})
	assertInlineBatches(t, run, batches, items)
	if state.externalizeCalls != 0 {
		t.Fatalf("ExternalizeItems calls = %d, want 0 when sum(len(batches)) != len(items)", state.externalizeCalls)
	}
}

// TestBatchesOverCountStayInline is the mirror case: more batched elements than
// items.
func TestBatchesOverCountStayInline(t *testing.T) {
	items := numberedItems(4)
	batches := [][]any{items[:3], items[2:]} // 3 + 2 batched elements for 4 items
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: batches, batchSize: 3})
	assertInlineBatches(t, run, batches, items)
	if state.externalizeCalls != 0 {
		t.Fatalf("ExternalizeItems calls = %d, want 0", state.externalizeCalls)
	}
}

// TestDeclinedExternalizationStaysInline: externalized=false is the store
// saying "keep it inline" (capability off, or too small); the tasks must then be
// the inline ones.
func TestDeclinedExternalizationStaysInline(t *testing.T) {
	items := numberedItems(6)
	batches := chunked(items, 2, 2, 2)
	state := newItemsState()
	state.decline = true
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: batches, batchSize: 2})
	assertInlineBatches(t, run, batches, items)
	if state.externalizeCalls != 1 {
		t.Fatalf("ExternalizeItems calls = %d, want 1", state.externalizeCalls)
	}
}

// TestStateWithoutTheCapabilityStaysInline: a store that does not implement
// BatchItemsStore (every backend today but the Redis one) never externalizes.
func TestStateWithoutTheCapabilityStaysInline(t *testing.T) {
	items := numberedItems(6)
	batches := chunked(items, 2, 2, 2)
	run := expandShaped(t, newFakeState(), &shapedMapHandler{items: items, batches: batches, batchSize: 2})
	assertInlineBatches(t, run, batches, items)
}

// TestExternalizeFailureFailsTheExpansion: a store error is returned to the
// caller, not absorbed into an inline fallback. The map node's expansion is not
// started (no batch task is queued) and the error reaches CommitTaskResult.
func TestExternalizeFailureFailsTheExpansion(t *testing.T) {
	batchItemsCache.Purge()
	items := numberedItems(4)
	state := newItemsState()
	state.externalizeErr = errors.New("blob store down")

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "items-ref-fail",
		Nodes: []types.NodeDef{mapNodeWithEchoBody(2)},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	queue := &fakeQueue{}
	eng := New(state, queue)
	testRegistries.Store(eng, &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2},
	}})
	t.Cleanup(func() { testRegistries.Delete(eng) })
	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	lease, err := eng.BuildTaskLease(context.Background(), roots[0])
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}
	out, _ := (&shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2}).Execute(context.Background(), lease.Input)
	err = eng.CommitTaskResult(context.Background(), lease, TaskResult{Output: out})
	if err == nil || !errors.Is(err, state.externalizeErr) {
		t.Fatalf("CommitTaskResult error = %v, want it to wrap the store error", err)
	}
	if left := queue.Drain(); len(left) != 0 {
		t.Fatalf("%d batch tasks were queued despite the failed externalization", len(left))
	}
}

// runAllBatches executes the run's batches in the given order and returns the
// map node's final output.
func runAllBatches(t *testing.T, run *itemsRun, order []int) map[string]any {
	t.Helper()
	ctx := context.Background()
	for _, i := range order {
		if err := run.eng.ExecuteBatch(ctx, run.batches[i]); err != nil {
			t.Fatalf("ExecuteBatch(batch %d): %v", i, err)
		}
	}
	out, err := run.state.GetOutput(ctx, run.execID, "m")
	if err != nil || out == nil {
		t.Fatalf("map output = %v, err = %v", out, err)
	}
	return out
}

// TestInlineBatchesStillReadable: a batch task that carries its items inline --
// every task written before items_ref existed, and every task from a store that
// does not externalize -- is read unchanged, on an engine that does have the
// capability. The view is the task itself and the store is never asked.
func TestInlineBatchesStillReadable(t *testing.T) {
	items := numberedItems(6)
	state := newItemsState()
	state.decline = true
	exec := newEchoBodyExecutor()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2), batchSize: 2},
		WithBatchBodyExecutor(exec))
	assertInlineBatches(t, run, chunked(items, 2, 2, 2), items)

	for i, task := range run.batches {
		view, err := run.eng.resolveBatchView(context.Background(), task)
		if err != nil {
			t.Fatalf("resolveBatchView(inline batch %d): %v", i, err)
		}
		if view != task {
			t.Errorf("inline batch %d: view is a copy, want the task itself", i)
		}
	}
	out := runAllBatches(t, run, []int{0, 1, 2})
	if count, _ := out["count"].(int); count != 6 {
		t.Errorf("count = %v, want 6", out["count"])
	}
	if state.resolveCalls != 0 {
		t.Errorf("ResolveItems calls = %d for inline batches, want 0", state.resolveCalls)
	}
	for _, req := range exec.requests() {
		if len(req.AllItems) != 6 {
			t.Errorf("batch %d AllItems = %d elements, want 6", req.BatchIndex, len(req.AllItems))
		}
	}
}

// TestItemsCacheHitSkipsResolve: the batches of one map node share one stored
// array, so the store is read once for the first batch and every later batch is
// served from the process cache. Emptying the cache makes the next batch read
// the store again, which is what proves the count is the cache's doing.
func TestItemsCacheHitSkipsResolve(t *testing.T) {
	items := numberedItems(6)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2), batchSize: 2})
	ctx := context.Background()

	for i, task := range run.batches {
		if _, err := run.eng.resolveBatchView(ctx, task); err != nil {
			t.Fatalf("resolveBatchView(batch %d): %v", i, err)
		}
	}
	if state.resolveCalls != 1 {
		t.Fatalf("ResolveItems calls after 3 batches = %d, want 1 (cache hits must not reach the store)", state.resolveCalls)
	}
	batchItemsCache.Purge()
	if _, err := run.eng.resolveBatchView(ctx, run.batches[0]); err != nil {
		t.Fatalf("resolveBatchView after purge: %v", err)
	}
	if state.resolveCalls != 2 {
		t.Fatalf("ResolveItems calls after purge = %d, want 2", state.resolveCalls)
	}
}

// TestItemsCacheIsBoundedAndPerNamespace: the cache never holds more than its
// capacity, and the same reference under another namespace is not served from
// an entry resolved for the first.
func TestItemsCacheIsBoundedAndPerNamespace(t *testing.T) {
	batchItemsCache.Purge()
	for i := range batchItemsCacheEntries + 3 {
		batchItemsCache.Add(fmt.Sprintf("k%d", i), nil)
	}
	if got := batchItemsCache.Len(); got != batchItemsCacheEntries {
		t.Fatalf("cache holds %d entries, want it capped at %d", got, batchItemsCacheEntries)
	}

	items := numberedItems(2)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 1, 1), batchSize: 1})
	if _, err := run.eng.resolveBatchView(context.Background(), run.batches[0]); err != nil {
		t.Fatalf("resolveBatchView: %v", err)
	}
	other := namespace.WithNamespace(context.Background(), "tenant-b")
	if _, err := run.eng.resolveBatchView(other, run.batches[0]); err != nil {
		t.Fatalf("resolveBatchView(other namespace): %v", err)
	}
	if state.resolveCalls != 2 {
		t.Fatalf("ResolveItems calls = %d, want 2: a namespace must not read another's cached array", state.resolveCalls)
	}
}

// TestBuildSubgraphLeaseDoesNotMutateTask: BuildSubgraphLease restores the
// stored array into the lease it hands a runner, but the task it was given -- the
// object a queue or a runner directory persists -- must not gain the array.
func TestBuildSubgraphLeaseDoesNotMutateTask(t *testing.T) {
	items := numberedItems(6)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2), batchSize: 2})
	task := run.batches[1]
	before, err := json.Marshal(task.Payload.Data)
	if err != nil {
		t.Fatalf("marshal task payload: %v", err)
	}

	lease, payload, err := run.eng.BuildSubgraphLease(context.Background(), task)
	if err != nil {
		t.Fatalf("BuildSubgraphLease: %v", err)
	}
	for _, key := range []string{"all_items", "items"} {
		if _, present := task.Payload.Data[key]; present {
			t.Errorf("task payload gained %q after BuildSubgraphLease", key)
		}
	}
	if _, present := lease.Task.Payload.Data["all_items"]; present {
		t.Error("the lease's task payload carries all_items; the array belongs in SubgraphPayload.AllItems only")
	}
	after, _ := json.Marshal(task.Payload.Data)
	if string(before) != string(after) {
		t.Errorf("task payload changed:\nbefore %s\nafter  %s", before, after)
	}
	if fmt.Sprint(payload.AllItems) != fmt.Sprint(items) {
		t.Errorf("lease AllItems = %v, want the whole array %v", payload.AllItems, items)
	}
	if fmt.Sprint(payload.Items) != fmt.Sprint(items[2:4]) {
		t.Errorf("lease Items = %v, want %v", payload.Items, items[2:4])
	}
}

// TestSubgraphLeaseMatchesInline: the payload a runner receives is the same for
// an externalized expansion as for an inline one, field for field, because the
// control plane restores the array before the lease leaves it.
func TestSubgraphLeaseMatchesInline(t *testing.T) {
	items := numberedItems(7)
	lengths := []int{3, 3, 1}
	build := func(decline bool) []*SubgraphLeasePayload {
		state := newItemsState()
		state.decline = decline
		run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, lengths...), batchSize: 3})
		var out []*SubgraphLeasePayload
		for _, task := range run.batches {
			_, payload, err := run.eng.BuildSubgraphLease(context.Background(), task)
			if err != nil {
				t.Fatalf("BuildSubgraphLease: %v", err)
			}
			// Both differ between two independent runs by construction: the
			// execution ids, and the deadline, which is derived from the parent
			// lease's issue time and so from the wall clock of each run.
			payload.ChildExecID = ""
			payload.Deadline = time.Time{}
			out = append(out, payload)
		}
		return out
	}
	inline, external := build(true), build(false)
	for i := range inline {
		a, _ := json.Marshal(inline[i])
		b, _ := json.Marshal(external[i])
		if string(a) != string(b) {
			t.Errorf("batch %d lease payload differs:\ninline   %s\nexternal %s", i, a, b)
		}
	}
}

// TestItemsRefKeepsResultOrder: with the items in a store, a map node gives the
// same output as with them inline, and a last-batch-first arrival order still
// yields the results in batch order, because the barrier sorts by batch index
// and the batches only differ in how they got their items.
func TestItemsRefKeepsResultOrder(t *testing.T) {
	items := numberedItems(7)
	lengths := []int{3, 3, 1}
	run := func(decline bool, order []int) []any {
		state := newItemsState()
		state.decline = decline
		r := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, lengths...), batchSize: 3},
			WithBatchBodyExecutor(newEchoBodyExecutor()))
		out := runAllBatches(t, r, order)
		results, _ := out["results"].([]any)
		return results
	}
	inline := run(true, []int{0, 1, 2})
	if len(inline) != len(items) {
		t.Fatalf("inline results = %d, want %d", len(inline), len(items))
	}
	for name, order := range map[string][]int{
		"in order":         {0, 1, 2},
		"last batch first": {2, 0, 1},
		"reversed":         {2, 1, 0},
	} {
		got := run(false, order)
		if len(got) != len(inline) {
			t.Fatalf("%s: results = %d, want %d", name, len(got), len(inline))
		}
		for i := range inline {
			if fmt.Sprint(got[i]) != fmt.Sprint(inline[i]) {
				t.Errorf("%s: result %d = %v, want %v (inline)", name, i, got[i], inline[i])
			}
		}
	}
}

// TestExecuteBatchGivesTheBodyItsWindowAndTheWholeArray: the body executor sees
// the same request for an externalized batch as for an inline one -- this
// batch's items, and the whole array as $items -- including the global index.
func TestExecuteBatchGivesTheBodyItsWindowAndTheWholeArray(t *testing.T) {
	items := numberedItems(7)
	lengths := []int{3, 3, 1}
	exec := newEchoBodyExecutor()
	run := expandShaped(t, newItemsState(), &shapedMapHandler{items: items, batches: chunked(items, lengths...), batchSize: 3},
		WithBatchBodyExecutor(exec))
	runAllBatches(t, run, []int{2, 0, 1})

	seen := map[int]BatchBodyRequest{}
	for _, req := range exec.requests() {
		seen[req.BatchIndex] = req
	}
	chunks := chunked(items, lengths...)
	for i, want := range chunks {
		req, ok := seen[i]
		if !ok {
			t.Fatalf("batch %d never reached the body executor", i)
		}
		if fmt.Sprint(req.Items) != fmt.Sprint(want) {
			t.Errorf("batch %d Items = %v, want %v", i, req.Items, want)
		}
		if fmt.Sprint(req.AllItems) != fmt.Sprint(items) {
			t.Errorf("batch %d AllItems = %v, want the whole array", i, req.AllItems)
		}
		if req.BatchSize != 3 {
			t.Errorf("batch %d BatchSize = %d, want 3", i, req.BatchSize)
		}
	}
}

// roundTripTask sends a task through JSON the way a queue does, so items_ref
// comes back as a decoded map and the integers as float64.
func roundTripTask(t *testing.T, task *Task) *Task {
	t.Helper()
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	var back Task
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal task: %v", err)
	}
	back.ActivationID = task.ActivationID
	back.AutoDepth = task.AutoDepth
	return &back
}

// TestItemsRefSurvivesAQueueRoundTrip: after a JSON round trip the reference is
// a different Go value (a map, with its keys in another order) but names the
// same array, so it is served from the same cache entry and resolves to the same
// window.
func TestItemsRefSurvivesAQueueRoundTrip(t *testing.T) {
	items := numberedItems(6)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 4, 2), batchSize: 4})
	ctx := context.Background()

	if _, err := run.eng.resolveBatchView(ctx, run.batches[0]); err != nil {
		t.Fatalf("resolveBatchView(in process): %v", err)
	}
	back := roundTripTask(t, run.batches[1])
	view, err := run.eng.resolveBatchView(ctx, back)
	if err != nil {
		t.Fatalf("resolveBatchView(round-tripped): %v", err)
	}
	if got, _ := view.Payload.Data["items"].([]any); fmt.Sprint(got) != fmt.Sprint(items[4:]) {
		t.Errorf("round-tripped batch items = %v, want %v", got, items[4:])
	}
	if state.resolveCalls != 1 {
		t.Errorf("ResolveItems calls = %d, want 1: the round-tripped reference must hit the entry the in-process one filled", state.resolveCalls)
	}
}

// TestUnreadableItemsFailTheBatchWithoutRunningIt: a store that cannot produce
// the array fails the batch with that error before any body runs, for both ways
// of reading a batch.
func TestUnreadableItemsFailTheBatchWithoutRunningIt(t *testing.T) {
	items := numberedItems(4)
	state := newItemsState()
	exec := newEchoBodyExecutor()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2},
		WithBatchBodyExecutor(exec))
	state.resolveErr = errors.New("blob gone")
	batchItemsCache.Purge()

	if err := run.eng.ExecuteBatch(context.Background(), run.batches[0]); !errors.Is(err, state.resolveErr) {
		t.Errorf("ExecuteBatch error = %v, want it to wrap the store error", err)
	}
	if _, _, err := run.eng.BuildSubgraphLease(context.Background(), run.batches[0]); !errors.Is(err, state.resolveErr) {
		t.Errorf("BuildSubgraphLease error = %v, want it to wrap the store error", err)
	}
	if n := len(exec.requests()); n != 0 {
		t.Errorf("body executor ran %d times for a batch whose items could not be read", n)
	}
}

// TestRefWithoutCapabilityIsAnError: a task that points at stored items, read by
// an engine whose state store cannot resolve them, fails loudly instead of
// running a body over an empty array.
func TestRefWithoutCapabilityIsAnError(t *testing.T) {
	items := numberedItems(4)
	run := expandShaped(t, newItemsState(), &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2})
	bare := New(newFakeState(), &fakeQueue{})
	batchItemsCache.Purge()
	if _, err := bare.resolveBatchView(context.Background(), run.batches[0]); !errors.Is(err, errBatchItemsUnsupported) {
		t.Fatalf("resolveBatchView error = %v, want errBatchItemsUnsupported", err)
	}
}

// TestWindowOutsideTheArrayIsAnError: a window that does not fit the stored array
// (a corrupted task, or a reference that points at a different array) is
// rejected rather than sliced out of range.
func TestWindowOutsideTheArrayIsAnError(t *testing.T) {
	items := numberedItems(4)
	run := expandShaped(t, newItemsState(), &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2})
	for name, window := range map[string][2]int{"past the end": {2, 5}, "negative start": {-1, 2}, "inverted": {3, 1}} {
		task := roundTripTask(t, run.batches[0])
		task.Payload.Data[batchItemsStartKey] = window[0]
		task.Payload.Data[batchItemsEndKey] = window[1]
		if _, err := run.eng.resolveBatchView(context.Background(), task); err == nil {
			t.Errorf("%s: resolveBatchView accepted window %v over 4 items", name, window)
		}
	}
}

// TestResolveBatchViewIsSafeUnderConcurrency: batches of one map node resolve
// from many goroutines at once, all sharing the process cache. Run under -race
// this is what would flag an unsynchronized cache or a view that aliases the
// task's own payload map.
func TestResolveBatchViewIsSafeUnderConcurrency(t *testing.T) {
	items := numberedItems(8)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2, 2), batchSize: 2})

	var wg sync.WaitGroup
	for range 8 {
		for i, task := range run.batches {
			wg.Go(func() {
				view, err := run.eng.resolveBatchView(context.Background(), task)
				if err != nil {
					t.Errorf("resolveBatchView(batch %d): %v", i, err)
					return
				}
				if got, _ := view.Payload.Data["items"].([]any); fmt.Sprint(got) != fmt.Sprint(items[i*2:i*2+2]) {
					t.Errorf("batch %d items = %v, want %v", i, got, items[i*2:i*2+2])
				}
			})
		}
	}
	wg.Wait()
	for i, task := range run.batches {
		if _, inline := task.Payload.Data["items"]; inline {
			t.Errorf("batch %d task gained items under concurrent resolution", i)
		}
	}
}

// TestBatchViewItemsCannotGrowIntoNeighbourWindow: a batch's items are a window
// of the one shared array, so a reader that appends to them must not write into
// the next batch's window or into all_items. That is what the capacity of the
// window guarantees: it is exactly the window's length, so append reallocates.
func TestBatchViewItemsCannotGrowIntoNeighbourWindow(t *testing.T) {
	items := numberedItems(6)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2, 2), batchSize: 2})
	ctx := context.Background()
	want := fmt.Sprint(items)

	first, err := run.eng.resolveBatchView(ctx, run.batches[0])
	if err != nil {
		t.Fatalf("resolveBatchView(batch 0): %v", err)
	}
	window, _ := first.Payload.Data["items"].([]any)
	if len(window) != 2 || cap(window) != len(window) {
		t.Fatalf("batch 0 items: len = %d, cap = %d, want both 2 so that append cannot reuse the array", len(window), cap(window))
	}

	_ = append(window, "SENTINEL")

	all, _ := first.Payload.Data["all_items"].([]any)
	if got := fmt.Sprint(all); got != want {
		t.Errorf("all_items after an append to batch 0 items = %s, want %s", got, want)
	}
	second, err := run.eng.resolveBatchView(ctx, run.batches[1])
	if err != nil {
		t.Fatalf("resolveBatchView(batch 1): %v", err)
	}
	if got, _ := second.Payload.Data["items"].([]any); fmt.Sprint(got) != fmt.Sprint(items[2:4]) {
		t.Errorf("batch 1 items after an append to batch 0 items = %v, want %v", got, items[2:4])
	}
}

// TestSupersededBatchDoesNotReadTheStore: a batch whose parent generation is
// gone is dropped by ExecuteBatch before its items are read, so a stale batch
// neither costs a store read nor fails on a missing blob.
func TestSupersededBatchDoesNotReadTheStore(t *testing.T) {
	items := numberedItems(4)
	state := newItemsState()
	run := expandShaped(t, state, &shapedMapHandler{items: items, batches: chunked(items, 2, 2), batchSize: 2},
		WithBatchBodyExecutor(newEchoBodyExecutor()))
	state.mu.Lock()
	for key, node := range state.nodes {
		stale := *node
		stale.LeaseToken = "superseded"
		state.nodes[key] = &stale
	}
	state.mu.Unlock()
	batchItemsCache.Purge()

	if err := run.eng.ExecuteBatch(context.Background(), run.batches[0]); err != nil {
		t.Fatalf("ExecuteBatch(superseded): %v", err)
	}
	if state.resolveCalls != 0 {
		t.Fatalf("ResolveItems calls = %d for a superseded batch, want 0", state.resolveCalls)
	}
}
