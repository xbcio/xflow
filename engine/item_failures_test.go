package engine

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// failingBodyExecutor fails the items at the given GLOBAL indices and records
// which items it actually ran, so a test can assert both the reported outcome
// and what really executed.
type failingBodyExecutor struct {
	mu      sync.Mutex
	failing map[int]bool
	ran     []int
}

func newFailingBodyExecutor(failing ...int) *failingBodyExecutor {
	set := make(map[int]bool, len(failing))
	for _, i := range failing {
		set[i] = true
	}
	return &failingBodyExecutor{failing: set}
}

func (x *failingBodyExecutor) ExecuteBatchBody(_ context.Context, req BatchBodyRequest) ([]BatchItemResult, error) {
	out := make([]BatchItemResult, 0, len(req.Items))
	for i := range req.Items {
		idx := globalItemIndex(req.BatchIndex, req.BatchSize, i)
		x.mu.Lock()
		x.ran = append(x.ran, idx)
		x.mu.Unlock()
		if x.failing[idx] {
			out = append(out, BatchItemResult{Index: idx, Err: errors.New("item body failed")})
			if !req.ContinueOnError {
				break
			}
			continue
		}
		out = append(out, BatchItemResult{Index: idx, Data: map[string]any{"index": idx}})
	}
	return out, nil
}

func (x *failingBodyExecutor) ranItems() []int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]int(nil), x.ran...)
}

// countingItemObserver is the seam the metrics counter plugs into.
type countingItemObserver struct {
	mu    sync.Mutex
	calls []ObservedItemFailures
}

func (o *countingItemObserver) ObserveItemFailures(f ObservedItemFailures) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, f)
}

func (o *countingItemObserver) total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, c := range o.calls {
		n += c.Failed
	}
	return n
}

func (o *countingItemObserver) observed() []ObservedItemFailures {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ObservedItemFailures(nil), o.calls...)
}

// runMapWithFailures drives a 10-item map node whose body fails at the given
// global indices, and returns the map node's output.
func runMapWithFailures(t *testing.T, batchSize int, continueOnError bool, exec BatchBodyExecutor, opts ...Option) map[string]any {
	t.Helper()
	nd := mapNodeWithEchoBody(batchSize)
	nd.Parameters["continue_on_error"] = continueOnError
	def := &types.WorkflowDef{
		Name:  "map-failures",
		Nodes: []types.NodeDef{nd, {Name: "done", Type: "test.echo"}},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"xflow.map": &sizedMapHandler{batchSize: batchSize},
		"test.echo": &echoHandler{},
	}}
	eng := New(state, queue, append([]Option{WithBatchBodyExecutor(exec)}, opts...)...)
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()

	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	executeTask(t, eng, roots[0])
	for _, bt := range queue.Drain() {
		if err := eng.ExecuteBatch(ctx, bt); err != nil {
			t.Fatalf("ExecuteBatch: %v", err)
		}
	}
	out, err := state.GetOutput(ctx, execID, "m")
	if err != nil {
		t.Fatalf("GetOutput: %v", err)
	}
	return out
}

// This is the assertion the whole counter exists for, and the one most easily
// faked: "the execution succeeded" passes just as well when the failures were
// silently swallowed — which is exactly the scenario being guarded against. So
// the count of observed failures must equal the number that actually failed.
func TestContinueOnErrorCountsFailedItems(t *testing.T) {
	obs := &countingItemObserver{}
	out := runMapWithFailures(t, 10, true, newFailingBodyExecutor(2, 5, 7), WithItemFailureObserver(obs))

	results, _ := out["results"].([]any)
	if len(results) != 10 {
		t.Errorf("results length = %d, want 10: a failed item must hold its slot as "+
			"{_error, _index} so count stays equal to the item count", len(results))
	}
	if got := obs.total(); got != 3 {
		t.Errorf("observed %d item failures, want 3; without this signal a node "+
			"silently dropping data is indistinguishable from a healthy one", got)
	}
	for _, c := range obs.observed() {
		if c.NodeName != "m" || c.Workflow != "map-failures" {
			t.Errorf("observation = %+v, want workflow \"map-failures\" node \"m\"", c)
		}
	}
}

// A healthy map node must not report failures. Without this, an observer that
// fired unconditionally would satisfy the test above.
func TestNoItemFailuresObservedWhenEveryItemSucceeds(t *testing.T) {
	obs := &countingItemObserver{}
	runMapWithFailures(t, 3, true, newFailingBodyExecutor(), WithItemFailureObserver(obs))
	if got := obs.total(); got != 0 {
		t.Errorf("observed %d item failures on a healthy map node, want 0", got)
	}
}

// continue_on_error: false stops the batch's remaining items. Asserting only
// "the map node failed" is a fake probe: it passes both when item 0 failed and
// when item 2 failed after 0 and 1 had already run.
func TestContinueOnErrorFalseStopsRemainingItemsInTheBatch(t *testing.T) {
	exec := newFailingBodyExecutor(2)
	// One batch of 10 items, so "the rest of the batch" is items 3..9.
	runMapWithFailures(t, 10, false, exec)

	ran := exec.ranItems()
	// Positive evidence: the items before the failure did run. Without this half,
	// "nothing ran at all" would also pass.
	if len(ran) < 3 || ran[0] != 0 || ran[1] != 1 || ran[2] != 2 {
		t.Fatalf("body ran items %v, want it to have reached items 0,1,2 before stopping", ran)
	}
	for _, idx := range ran {
		if idx > 2 {
			t.Errorf("body ran item %d after the batch should have stopped at the failure "+
				"at index 2; false means stop this batch's remaining items", idx)
		}
	}
}

// The control plane escapes batches to runners, so ITS engine never runs a body
// and never reaches observeItemFailures. The counter has to fire where the
// runner's result arrives instead, or continue_on_error is observable only in
// embedded deployments — the ones least likely to need it.
func TestCommitSubgraphResultObservesTheRunnersItemFailures(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "map-remote-failures",
		Nodes: []types.NodeDef{{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	obs := &countingItemObserver{}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": &loopHandler{}}}
	eng := New(state, queue, WithItemFailureObserver(obs), WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)

	lease, _, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	// The shape a runner reports for a partially-failed batch: two items, one of
	// which holds an {_error, _index} placeholder. The batch itself succeeded, so
	// nothing else in this commit says a record was lost.
	if _, err := eng.CommitSubgraphResult(ctx, lease, TaskResult{Output: &types.Output{Data: map[string]any{
		"count":  2,
		"failed": 1,
		"items": []any{
			map[string]any{"ok": true},
			map[string]any{batchErrorKey: "item body failed", "_index": 1},
		},
	}}}); err != nil {
		t.Fatalf("CommitSubgraphResult() error = %v", err)
	}

	if got := obs.total(); got != 1 {
		t.Errorf("observed %d item failures from the runner's batch, want 1; the control "+
			"plane never runs a body, so this is the only place its batches can be counted", got)
	}
	for _, c := range obs.observed() {
		if c.NodeName != "loop" || c.Workflow != "map-remote-failures" {
			t.Errorf("observation = %+v, want workflow \"map-remote-failures\" node \"loop\"", c)
		}
	}
}

// A runner's healthy batch must not report failures.
func TestCommitSubgraphResultObservesNothingForAHealthyBatch(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "map-remote-healthy",
		Nodes: []types.NodeDef{{Name: "loop", Type: "xflow.map", Parameters: mapBodyParamsForTest()}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	obs := &countingItemObserver{}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"xflow.map": &loopHandler{}}}
	eng := New(state, queue, WithItemFailureObserver(obs), WithBatchBodyExecutor(newEchoBodyExecutor()))
	testRegistries.Store(eng, reg)
	t.Cleanup(func() { testRegistries.Delete(eng) })
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	batches := drainBatchTasks(t, eng, queue)
	lease, _, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if _, err := eng.CommitSubgraphResult(ctx, lease, TaskResult{Output: &types.Output{Data: map[string]any{
		"count": 1, "failed": 0, "items": []any{map[string]any{"ok": true}},
	}}}); err != nil {
		t.Fatalf("CommitSubgraphResult() error = %v", err)
	}
	if got := obs.total(); got != 0 {
		t.Errorf("observed %d item failures for a healthy runner batch, want 0", got)
	}
}
