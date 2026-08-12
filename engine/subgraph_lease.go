package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ErrBatchLeaseRequired is returned by BuildTaskLease for a NodeBatch task.
// Batches carry their own lease shape (BuildSubgraphLease) because they name a
// synthetic node outside the compiled graph and commit through the expansion
// barrier rather than the node path. Callers that route batches must branch on
// this rather than treating it as a dispatch failure.
var ErrBatchLeaseRequired = errors.New("batch task requires BuildSubgraphLease")

// SubgraphLeasePayload carries everything a runner needs to execute one batch
// of a map expansion. It is the NodeBatch counterpart of GroupLeasePayload:
// TaskLease.Input is nil for a batch task, and this payload is the single
// source of truth.
//
// The body sub-graph package is not carried yet — the batch still executes as
// the pass-through it always was. What this payload does today is give the
// batch an identity of its own on the wire, so it stops borrowing the map
// node's.
type SubgraphLeasePayload struct {
	ProtocolVersion int    `json:"protocol_version"`
	ParentNode      string `json:"parent_node"`
	ParentNodeIdx   int    `json:"parent_node_idx"`
	BatchIndex      int    `json:"batch_index"`
	// ChildExecID identifies the sub-execution this batch completes. The
	// engine needs it to resolve the batch back to its slot in the parent's
	// child generation.
	ChildExecID types.ExecutionID `json:"child_exec_id"`
	Items       []any             `json:"items,omitempty"`
	Deadline    time.Time         `json:"deadline,omitempty"`
	// Package and PackageHash are the map node's projected body and its hash,
	// both computed once at compile time. A runner has no compiled graph — it
	// never saw the workflow definition — so the body must travel to it, exactly
	// as a group's package does on GroupLeasePayload. Every batch of one map node
	// carries the same pair, which is what lets a hash-keyed package cache
	// compile the body once however many batches arrive.
	Package     *graph.SubgraphPackage `json:"package,omitempty"`
	PackageHash string                 `json:"package_hash,omitempty"`
	// BatchSize is the map node's declared batch size. The runner needs it to
	// turn a position within this batch into the item's global index: $index must
	// not change when batch_size does.
	BatchSize int `json:"batch_size,omitempty"`
	// AllItems is the map node's entire items array, exposed to the body as
	// $items. A full copy per batch — the same trade-off the in-process payload
	// makes, kept because $items is a promised DSL root.
	AllItems []any `json:"all_items,omitempty"`
	// ContinueOnError, when false, stops this batch at its first failed item.
	ContinueOnError bool `json:"continue_on_error,omitempty"`
	// Runtime is the outer submission's runtime, carried so a body member's
	// $vars sees the per-submission half. The static half already travels
	// inside Package.Def.Context; Runtime.Vars have no other route to a runner,
	// which never saw the submission. A group lease carries the equivalent
	// inside its whole *types.Input — a batch lease carries items instead, so
	// this field is where the same information goes.
	//
	// Contains only what the submitter put in Runtime.Vars (tenant, namespace,
	// and the like). It is NOT a credential channel: credentials reach a runner
	// through declarative injection, never through $vars.
	Runtime *types.Runtime `json:"runtime,omitempty"`
}

// BuildSubgraphLease assembles a runner-facing lease for a queued batch task.
//
// It deliberately does NOT go through AcquireTaskLease. A batch task names a
// synthetic node ("m/_batch/0") that exists only in the expansion payload, so
// acquiring a node lease for it would write live state for a node the compiled
// graph never declared. The authoritative fence for the whole expansion is the
// parent map node's lease, which the batch payload carries and which
// CompleteExpandedSubExecution verifies on commit — a batch that arrives after
// the parent was reclaimed is rejected there, exactly as it was when batches
// ran on the control plane.
func (e *Engine) BuildSubgraphLease(ctx context.Context, t *Task) (*TaskLease, *SubgraphLeasePayload, error) {
	if t == nil {
		return nil, nil, fmt.Errorf("build subgraph lease: nil task")
	}
	if t.Type != TaskTypeNodeBatch {
		return nil, nil, fmt.Errorf("build subgraph lease: task type %v is not a batch", t.Type)
	}

	parentLease, childExecID, items, err := expansionBatchLease(t)
	if err != nil {
		return nil, nil, fmt.Errorf("build subgraph lease: %w", err)
	}

	g, active, err := e.loadActiveGraph(ctx, parentLease.Task.ExecutionID)
	if err != nil {
		return nil, nil, err
	}
	if !active {
		return nil, nil, ErrExecutionInactive
	}
	if parentLease.Task.NodeIdx < 0 || parentLease.Task.NodeIdx >= g.NodeCount() {
		return nil, nil, fmt.Errorf("build subgraph lease: parent node index %d is out of range", parentLease.Task.NodeIdx)
	}
	meta := g.NodeAt(parentLease.Task.NodeIdx)

	batchIndex, err := batchIndexOf(t)
	if err != nil {
		return nil, nil, fmt.Errorf("build subgraph lease: %w", err)
	}

	// The lease identity IS the parent's. A batch is a delivery vehicle for
	// work the parent already fenced, not an independent claim, so it must not
	// mint a token that CompleteExpandedSubExecution would then reject.
	lease := &TaskLease{
		LeaseID:     parentLease.LeaseID,
		LeaseToken:  parentLease.LeaseToken,
		Attempt:     parentLease.Attempt,
		Task:        *t,
		NodeType:    meta.Type,
		NodeVersion: meta.Version,
		IssuedAt:    time.Now().UTC(),
		TTL:         e.defaultLeaseTTL,
	}

	payload := &SubgraphLeasePayload{
		ProtocolVersion: 1,
		ParentNode:      parentLease.Task.NodeName,
		ParentNodeIdx:   parentLease.Task.NodeIdx,
		BatchIndex:      batchIndex,
		ChildExecID:     childExecID,
		Items:           items,
		ContinueOnError: mapContinueOnError(meta),
	}
	// The body and the batching context are what turn this lease from a
	// pass-through into runnable work. They come from two different places: the
	// package from the compiled graph (projected once at compile time), the
	// batching context from the task payload — at this point the map node is
	// still Waiting, so its output cannot be read back.
	if body := g.BodyAt(parentLease.Task.NodeIdx); body != nil {
		payload.Package = body.Package
		payload.PackageHash = body.Hash
	}
	payload.AllItems, payload.BatchSize = mapBatchingContext(t, len(items))
	// The runtime half of $vars: see the field's doc comment. Read from the
	// execution snapshot because a batch task carries no input of its own.
	runtime, err := e.executionRuntime(ctx, parentLease.Task.ExecutionID)
	if err != nil {
		return nil, nil, fmt.Errorf("build subgraph lease: %w", err)
	}
	payload.Runtime = runtime
	lease.SubgraphPayload = payload
	return lease, payload, nil
}

// CommitSubgraphResult completes one batch's sub-execution through the parent's
// expansion fence and, once the final batch reports, finalizes the map node and
// advances downstream exactly once.
//
// This is the barrier the ordinary node commit path does not have: committing a
// batch as a normal node result terminalizes the map node on the FIRST batch,
// firing downstream while the other batches are still in flight.
func (e *Engine) CommitSubgraphResult(ctx context.Context, lease *TaskLease, result TaskResult) (CommitOutcome, error) {
	if lease == nil || lease.SubgraphPayload == nil {
		return CommitOutcomeStaleToken, ErrInvalidLeaseToken
	}
	payload := lease.SubgraphPayload

	status := types.ExecutionStatusSuccess
	batchResult := map[string]any{}
	if result.Output != nil && result.Output.Data != nil {
		batchResult = result.Output.Data
	}
	if result.Error != nil {
		status = types.ExecutionStatusFailed
		// Stamp the failure into the batch's own result. The all-done barrier
		// hands completeLoopSplit only the results array, and the batch that
		// reports last is usually not the batch that failed — without this the
		// verdict would be unrecoverable and the map node would commit success
		// with a hole in its results.
		batchResult = cloneMap(batchResult)
		if batchResult == nil {
			batchResult = map[string]any{}
		}
		batchResult[batchErrorKey] = result.Error.Error()
	}

	// Count the batch's failed items. This is the only place a runner's batches
	// can be counted: the control plane escapes them (WithRemoteBatchExecution),
	// so its engine never runs a body and never reaches observeItemFailures.
	//
	// The count is read back OUT of the reported result rather than taken from a
	// dedicated field, because the placeholders are what actually reach
	// downstream — counting anything else would let the metric and the data
	// disagree.
	e.observeReportedItemFailures(ctx, lease, batchResult)

	// Reconstruct the parent lease the expansion layer fences against. Its
	// identity travels on the batch lease itself (BuildSubgraphLease copies it
	// verbatim), so a batch from a superseded parent generation carries the old
	// token and is rejected below rather than corrupting the new generation.
	parentLease := &TaskLease{
		LeaseID:    lease.LeaseID,
		LeaseToken: lease.LeaseToken,
		Attempt:    lease.Attempt,
		Task: Task{
			ExecutionID:  lease.Task.ExecutionID,
			NodeName:     payload.ParentNode,
			NodeIdx:      payload.ParentNodeIdx,
			Type:         TaskTypeNodeBatch,
			ActivationID: lease.Task.ActivationID,
			AutoDepth:    lease.Task.AutoDepth,
		},
	}

	expander, ok := e.state.(LeaseExpander)
	if !ok {
		return CommitOutcomeTransientError, ErrAtomicCommitUnsupported
	}
	allDone, accepted, results, err := expander.CompleteExpandedSubExecution(ctx, parentLease, payload.ChildExecID, status, batchResult)
	if err != nil {
		return CommitOutcomeTransientError, err
	}
	if !accepted {
		// A stale batch from a reclaimed parent generation. Not an error: the
		// fence did its job.
		return CommitOutcomeStaleToken, nil
	}
	if !allDone {
		return CommitOutcomeAccepted, nil
	}

	g, active, err := e.loadActiveGraph(ctx, parentLease.Task.ExecutionID)
	if err != nil {
		return CommitOutcomeTransientError, fmt.Errorf("load graph for map parent %q: %w", parentLease.Task.ExecutionID, err)
	}
	if !active {
		return CommitOutcomeExecutionInactive, nil
	}
	if err := e.completeLoopSplit(ctx, parentLease, g, results); err != nil {
		return CommitOutcomeTransientError, err
	}
	return CommitOutcomeAccepted, nil
}

// observeReportedItemFailures counts the {_error, _index} placeholders in a
// batch result a runner reported.
//
// The workflow name needs the compiled graph, and this runs on every batch
// commit, so the lookup is skipped entirely when no observer is installed and
// when the batch had no failures — the common case for both.
func (e *Engine) observeReportedItemFailures(ctx context.Context, lease *TaskLease, batchResult map[string]any) {
	if e.itemFailureObserver == nil {
		return
	}
	items, ok := batchResult["items"].([]any)
	if !ok {
		return
	}
	failed := 0
	for _, item := range items {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if _, isErr := row[batchErrorKey]; isErr {
			failed++
		}
	}
	if failed == 0 {
		return
	}
	workflow := ""
	if g, active, err := e.loadActiveGraph(ctx, lease.Task.ExecutionID); err == nil && active {
		workflow = g.Name()
	}
	e.itemFailureObserver.ObserveItemFailures(ObservedItemFailures{
		Workflow: workflow,
		NodeName: lease.SubgraphPayload.ParentNode,
		Failed:   failed,
		Total:    len(items),
	})
}

// batchIndexOf reads the batch index the expansion layer stamped on the task.
func batchIndexOf(t *Task) (int, error) {
	if t == nil || t.Payload == nil || t.Payload.Data == nil {
		return 0, fmt.Errorf("batch task missing payload")
	}
	return batchPayloadInt(t.Payload.Data, "batch_index")
}
