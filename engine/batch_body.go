package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ErrNoBatchBodyExecutor reports that a batch reached this engine to be executed
// in process but no BatchBodyExecutor was supplied. It is a configuration fault,
// not a workflow fault: the alternative — reporting the batch's items back
// unchanged, as the pre-body pass-through did — makes a map node with a real
// body look like it ran while nothing in the body executed.
var ErrNoBatchBodyExecutor = errors.New("no batch body executor configured")

// BatchItemResult is one item's body outcome. It is what ends up in the map
// node's flattened results array, one entry per item regardless of how the
// items were batched.
type BatchItemResult struct {
	// Index is the item's GLOBAL position in the map node's items array, not
	// its position within the batch. batch_size is a durability policy, not a
	// semantic one: changing it must not change what an item is called.
	Index int
	// Data is the body sub-graph's collected output for this item. Nil when the
	// item failed.
	Data map[string]any
	// Err is non-nil when this item's body execution failed.
	Err error
}

// BatchBodyExecutor runs the body sub-graph once per item of one batch.
//
// The engine cannot do this itself: running a body means compiling a package,
// constructing a backend, and driving an inner execution — all IO, and all of
// it lives outside engine/. So the engine defines the shape of the work and
// whoever has those dependencies supplies the implementation, exactly as
// GroupExecutor does for group units.
//
// items are the batch's items in order; batchIndex and batchSize let the
// implementation compute each item's global index. An implementation reports
// one BatchItemResult per item and returns an error only when the batch could
// not be run at all (package compile failure, missing handler inventory,
// backend construction) — "the body ran and failed" is per-item Err, not a
// returned error, because those two need different retry treatment.
type BatchBodyExecutor interface {
	ExecuteBatchBody(ctx context.Context, req BatchBodyRequest) ([]BatchItemResult, error)
}

// BatchBodyRequest describes one batch's worth of body work.
type BatchBodyRequest struct {
	ExecutionID string
	// ParentNode is the map node the batch belongs to. Carried for diagnostics
	// and for metric labels; the executor does not resolve it.
	ParentNode string
	// Body is the map node's projected body package and its hash, both computed
	// once at compile time. Every batch of the same map node carries the same
	// pair, which is what lets a hash-keyed package cache compile the body once
	// regardless of how many batches ran.
	Body       *graph.SubgraphPackage
	BodyHash   string
	BatchIndex int
	// BatchSize is the map node's declared batch size, needed to compute global
	// item indices. Zero or negative means "derive from this batch's length",
	// which is only correct for a single-batch expansion.
	BatchSize int
	Items     []any
	// AllItems is the map node's entire items array, exposed to the body as
	// $items. It is a full copy per batch — a known trade-off recorded in the
	// design, kept because $items is a promised DSL root.
	AllItems []any
	// ContinueOnError, when false, stops the batch at its first failed item.
	// The already-executed items keep their side effects: there is no rollback.
	ContinueOnError bool
	// BodyConcurrency is the maximum number of this batch's items that may run
	// at the same time. Zero or one means serial, which is the default and the
	// behaviour of every map node written before this field existed.
	//
	// It is a cap rather than a bool because the cap is the point. A batch is a
	// deliberately-sized durability unit, and running all of its items at once
	// multiplies that batch's peak resource use by its length -- for the wasm
	// bodies this exists for, 40 concurrent items means 40 live guest instances
	// with their own linear memories, not 4.
	//
	// It also weakens fail-fast, which is why it must be opt-in: under
	// ContinueOnError=false the serial loop promises that no item AFTER the
	// failure runs, and with items already in flight only the weaker "no item is
	// STARTED after the failure is observed" survives. There is no rollback for
	// the ones that did start.
	BodyConcurrency int
	// Runtime is the OUTER submission's runtime, forwarded so a body member's
	// $vars sees the per-submission half too. $vars is the union of the
	// workflow's static Context.Vars -- which travel inside Body.Def -- and
	// Runtime.Vars, which have no other way in: a batch is dispatched from a
	// task, not from the submission, so without this field the runtime half
	// stops at the map node. A group member already gets both, because a group
	// lease carries a whole *types.Input; a batch lease carries items.
	Runtime *types.Runtime
	// OuterNodes holds the outputs of the outer-graph nodes this body reads
	// through $nodes['name'], snapshotted once when the batch was scheduled.
	// Every item of the batch sees the same values -- the spec's semantics are a
	// snapshot as of entering the map node, not a live read.
	//
	// It cannot ride Scope alongside $item/$index/$items: BuildExprEnv assigns
	// env["$nodes"] AFTER spreading Input.Data, so a "$nodes" key arriving
	// through the scope is overwritten by the inner node's own (empty) Nodes.
	// This travels to types.Input.Nodes instead, the field $nodes actually reads.
	//
	// nil when the body reads no outer node, which is every body written before
	// cross-domain reads existed.
	OuterNodes map[string]any
	// TraceID and SpanID are the OUTER execution's trace identity, forwarded so
	// the body's sub-execution continues the same trace instead of starting a
	// detached one.
	//
	// They are isomorphic to Runtime: both are execution-scoped values a batch
	// task cannot read for itself, because a batch is dispatched from the
	// parent's expansion rather than from a submission. A group needs no
	// equivalent field -- its lease carries a whole *types.Input, which already
	// has TraceID/SpanID on it.
	//
	// Empty when the outer submission carried no trace, which is every
	// non-instrumented caller.
	TraceID string
	SpanID  string
}

// WithBatchBodyExecutor supplies the body sub-graph executor used by
// ExecuteBatch. Without it, a batch executed in process has no body to run.
func WithBatchBodyExecutor(be BatchBodyExecutor) Option {
	return func(e *Engine) { e.batchBodyExecutor = be }
}

// globalItemIndex maps a position within one batch to the item's position in
// the map node's whole items array.
func globalItemIndex(batchIndex, batchSize, posInBatch int) int {
	if batchSize <= 0 {
		return posInBatch
	}
	return batchIndex*batchSize + posInBatch
}

// BatchResultForCommit folds per-item results into the batch result map the
// expansion barrier stores, plus the batch's verdict.
//
// Both paths that run a body — in-process ExecuteBatch and the runner's
// SubgraphRuntime — go through this function so the two cannot disagree about
// when a batch counts as failed. They report the verdict differently (one
// stamps the result map directly, the other returns it as the lease's error for
// CommitSubgraphResult to stamp), but deriving it is the same decision.
//
// continueOnError is what the verdict turns on, because it is what the failure
// MEANS:
//
//   - false: any failed item fails the batch. The body stopped there, so the
//     remaining items never ran and the results array is short. Calling that a
//     success would terminalize the map node as Success with a hole in its
//     results — the opposite of what "stop on error" asked for.
//   - true: a partially-failed batch SUCCEEDS. The {_error, _index}
//     placeholders are the deliverable; the map node stays successful and
//     downstream filters them. Failing the batch here would make the setting a
//     no-op.
//
// A batch whose every item failed is failed under either setting. An empty batch
// is not a failure — an expansion can legitimately contain one.
func BatchResultForCommit(results []BatchItemResult, continueOnError bool) (map[string]any, error) {
	data := batchResultData(results)
	if len(results) == 0 {
		return data, nil
	}
	var first error
	succeeded := 0
	for _, r := range results {
		if r.Err == nil {
			succeeded++
			continue
		}
		if first == nil {
			first = r.Err
		}
	}
	if first == nil {
		return data, nil
	}
	if continueOnError && succeeded > 0 {
		return data, nil
	}
	return data, first
}

// batchResultData converts per-item results into the batch result map the
// expansion barrier stores. The items array is what completeLoopSplit flattens,
// so its length — not the batch count — is what the map node reports as count.
//
// A failed item occupies its slot as {_error, _index} rather than being dropped,
// which is what makes "count always equals the input length" true and lets a
// downstream filter distinguish failures from data.
func batchResultData(results []BatchItemResult) map[string]any {
	items := make([]any, 0, len(results))
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			items = append(items, map[string]any{
				batchErrorKey: r.Err.Error(),
				"_index":      r.Index,
			})
			continue
		}
		items = append(items, r.Data)
	}
	return map[string]any{
		"items":  items,
		"count":  len(items),
		"failed": failed,
	}
}

// batchBodyError builds the error a batch reports when its body could not run
// at all, as distinct from a body that ran and failed.
func batchBodyError(parentNode string, batchIndex int, cause error) error {
	return fmt.Errorf("run body for %q batch %d: %w", parentNode, batchIndex, cause)
}
