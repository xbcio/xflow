package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// expandsIntoSubExecutions reports whether this node's successful output is a
// fan-out descriptor to expand rather than a value to commit.
//
// The answer is a structural property of the graph, not of the payload: a node
// expands exactly when the compiler projected a sub-graph body for it. That is
// decidable at compile time, so it is decided there —
// engine/graph.projectNodeBodies puts the body on the node, and the snapshot
// decoder refuses to load a node that declares a body without one, which is
// what makes BodyAt authoritative here.
//
// The previous criterion sniffed the payload for "_loop"/"_split" marker keys.
// Payloads cannot answer this question: any handler can name a field "_loop",
// and doing so turned it into a fan-out node whose batches then had no body to
// run — every batch failed, retried, and the execution hung until its deadline.
// The markers were removed rather than renamed, because a key that must be
// present for correctness but that nothing can validate is a liability whatever
// it is called.
func expandsIntoSubExecutions(g *graph.Graph, nodeIdx int) bool {
	return g != nil && g.BodyAt(nodeIdx) != nil
}

// taskResultExpands narrows expandsIntoSubExecutions to the results that
// actually fan out. Only a success does: a map node that failed, or that routed
// to its error port, is an ordinary node failure. The payload-sniffing criterion
// got this for free, because a failed task carries no output to sniff; a
// criterion read off the graph has to say it.
//
// Measured: dropping the narrowing changes no observable behaviour today. A
// failing map then reaches commitLegacyTaskResult instead, whose error branch
// runs the same retry budget and OnError strategy and whose commit redirects
// back to commitAcyclicNode for any acyclic graph — the two paths converge. The
// narrowing is kept because that convergence is incidental: it holds only as
// long as the legacy path keeps mirroring the acyclic one, and a failure has no
// business entering an expansion path to begin with.
func taskResultExpands(g *graph.Graph, lease *TaskLease, result TaskResult) bool {
	if result.Error != nil || result.Output == nil || result.Output.Error != nil {
		return false
	}
	if outputPortRetryError(result.Output) != nil {
		return false
	}
	return expandsIntoSubExecutions(g, lease.Task.NodeIdx)
}

// expandLoopSplit starts one lease-fenced child generation. The parent stays
// waiting with its original lease metadata, so the normal sweeper can reclaim
// a crash between child creation, batch delivery, and parent finalization.
func (e *Engine) expandLoopSplit(ctx context.Context, lease *TaskLease, g *graph.Graph, data map[string]any) error {
	if lease == nil {
		return ErrInvalidLeaseToken
	}
	batches, err := loopSplitBatches(data)
	if err != nil {
		return fmt.Errorf("decode loop/split batches for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if len(batches) == 0 {
		return e.completeLoopSplit(ctx, lease, g, []map[string]any{})
	}

	expander, ok := e.state.(DurableLeaseExpander)
	if !ok {
		return ErrAtomicCommitUnsupported
	}
	children := make([]SubExecution, 0, len(batches))
	entries := make([]OutboxEntry, 0, len(batches))
	for i, batch := range batches {
		childID := expansionChildID(lease, i)
		children = append(children, SubExecution{
			ParentExecID: lease.Task.ExecutionID,
			ParentNode:   lease.Task.NodeName,
			ChildExecID:  childID,
			BatchIndex:   i,
			Status:       types.ExecutionStatusRunning,
		})
		entries = append(entries, OutboxEntry{
			ID:   expansionOutboxID(lease, i),
			Task: expansionBatchTask(lease, childID, i, batch, data),
		})
	}
	started, err := expander.BeginTaskExpansionWithOutbox(ctx, lease, children, entries)
	if err != nil {
		return fmt.Errorf("begin durable loop/split expansion %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if !started {
		return ErrInvalidLeaseToken
	}
	if err := e.FlushOutbox(ctx, lease.Task.ExecutionID); err != nil {
		return fmt.Errorf("deliver loop/split batches for %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}

// loopSplitBatches accepts both native Go batches and the []any representation
// produced when a runner result is decoded from JSON.
func loopSplitBatches(data map[string]any) ([][]any, error) {
	raw, exists := data["batches"]
	if !exists || raw == nil {
		return nil, nil
	}
	if batches, ok := raw.([][]any); ok {
		return batches, nil
	}
	rows, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("batches has type %T, want array", raw)
	}
	batches := make([][]any, 0, len(rows))
	for index, row := range rows {
		if row == nil {
			batches = append(batches, nil)
			continue
		}
		items, ok := row.([]any)
		if !ok {
			return nil, fmt.Errorf("batch %d has type %T, want array", index, row)
		}
		batches = append(batches, items)
	}
	return batches, nil
}

// expansionBatchTask builds one batch's durable task. data is the map node's own
// output, from which the batch carries forward what the body needs but cannot
// derive: batch_size (to turn a position within a batch into a global item
// index) and the whole items array (exposed to the body as $items).
//
// These travel on every batch rather than being read back from the map node's
// stored output because at expansion time that output does not exist yet — the
// map node is still Waiting and only terminalizes once every batch has reported.
// The cost is one copy of the items array per batch, which the design accepts:
// $items is a promised DSL root, and making it lazy is a later optimization.
func expansionBatchTask(lease *TaskLease, childID types.ExecutionID, batchIndex int, items []any, data map[string]any) Task {
	payload := map[string]any{
		"_batch_exec":          true,
		"parent_exec_id":       string(lease.Task.ExecutionID),
		"parent_node":          lease.Task.NodeName,
		"parent_node_idx":      lease.Task.NodeIdx,
		"parent_lease_id":      string(lease.LeaseID),
		"parent_lease_token":   string(lease.LeaseToken),
		"parent_attempt":       lease.Attempt,
		"parent_activation_id": lease.Task.ActivationID,
		"parent_auto_depth":    lease.Task.AutoDepth,
		"child_exec_id":        string(childID),
		"batch_index":          batchIndex,
		"items":                items,
	}
	if size, err := batchPayloadInt(data, "batch_size"); err == nil && size > 0 {
		payload["batch_size"] = size
	}
	if all, ok := data["items"].([]any); ok {
		payload["all_items"] = all
	}
	return Task{
		ExecutionID: lease.Task.ExecutionID,
		NodeName:    fmt.Sprintf("%s/_batch/%d", lease.Task.NodeName, batchIndex),
		NodeIdx:     lease.Task.NodeIdx,
		Type:        TaskTypeNodeBatch,
		Payload:     &types.SignalPayload{Data: payload},
	}
}

func expansionOutboxID(lease *TaskLease, batchIndex int) string {
	return fmt.Sprintf("expand/%s/%s/%s/%d", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID, batchIndex)
}

func expansionChildID(lease *TaskLease, batchIndex int) types.ExecutionID {
	return types.ExecutionID(fmt.Sprintf("%s/sub/%s/%s/%d", lease.Task.ExecutionID, lease.Task.NodeName, lease.LeaseID, batchIndex))
}

// ExecuteBatch runs one batch of a loop/split expansion in process: it executes
// the body sub-graph once per item and reports the batch through the expansion
// barrier. Its parent lease fence makes delayed or duplicate old batches
// harmless after recovery has issued a newer parent lease.
//
// Deployments that route batches to a runner never reach this path (see
// WithRemoteBatchExecution); the runner's own body runtime does the same work
// and reports through CommitSubgraphResult.
func (e *Engine) ExecuteBatch(ctx context.Context, t *Task) error {
	lease, childExecID, items, err := expansionBatchLease(t)
	if err != nil {
		return err
	}
	batchIndex, err := batchIndexOf(t)
	if err != nil {
		return err
	}

	// The graph and the map node's own output are loaded BEFORE the body runs:
	// batch_size and the full items array live there, not in the batch payload,
	// and an inactive execution should not run body side effects at all.
	g, active, err := e.loadActiveGraph(ctx, lease.Task.ExecutionID)
	if err != nil {
		return fmt.Errorf("load graph for loop/split parent %q: %w", lease.Task.ExecutionID, err)
	}
	if !active {
		return ErrExecutionInactive
	}

	// Check the parent fence BEFORE running the body. CompleteExpandedSubExecution
	// rejects a stale batch afterwards, which was enough while the batch was a
	// pass-through with nothing to undo — now the body has side effects, and a
	// batch from a superseded generation would apply them before being told it no
	// longer counts.
	//
	// This is best-effort, not a claim: there is no atomic "claim this batch"
	// primitive, so a parent reclaimed in the window between this check and the
	// body still gets duplicate side effects. That narrows the window rather than
	// closing it, which is why body nodes with side effects must be idempotent on
	// a business key from $item.
	if stale, err := e.batchLeaseIsStale(ctx, lease); err != nil {
		return err
	} else if stale {
		return nil
	}

	result, err := e.runBatchBody(ctx, g, lease, t, batchIndex, items)
	if err != nil {
		return err
	}

	expander, ok := e.state.(LeaseExpander)
	if !ok {
		return ErrAtomicCommitUnsupported
	}
	status := types.ExecutionStatusSuccess
	if _, failed := result[batchErrorKey]; failed {
		status = types.ExecutionStatusFailed
	}
	allDone, accepted, results, err := expander.CompleteExpandedSubExecution(ctx, lease, childExecID, status, result)
	if err != nil {
		return err
	}
	if !accepted || !allDone {
		return nil
	}
	return e.completeLoopSplit(ctx, lease, g, results)
}

// batchLeaseIsStale reports whether the parent generation this batch belongs to
// has already been superseded — the same condition CompleteExpandedSubExecution
// enforces authoritatively, checked early so the body does not run for a batch
// whose result will be discarded.
func (e *Engine) batchLeaseIsStale(ctx context.Context, lease *TaskLease) (bool, error) {
	node, err := e.state.GetNode(ctx, lease.Task.ExecutionID, lease.Task.NodeName)
	if err != nil {
		return false, fmt.Errorf("read loop/split parent %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	if node == nil {
		return true, nil
	}
	return node.Status != types.NodeStatusWaiting ||
		node.LeaseID != lease.LeaseID ||
		node.LeaseToken != lease.LeaseToken ||
		node.Attempt != lease.Attempt ||
		node.ActivationID != lease.Task.ActivationID, nil
}

// runBatchBody executes the body once per item and folds the outcomes into the
// batch result the expansion barrier stores.
//
// A batch whose items all failed is itself a failed batch. A batch with SOME
// failed items under continue_on_error is a successful batch carrying
// {_error, _index} placeholders: the map node stays successful and downstream
// filters them out, which is the whole point of the setting.
func (e *Engine) runBatchBody(ctx context.Context, g *graph.Graph, lease *TaskLease, t *Task, batchIndex int, items []any) (map[string]any, error) {
	if e.batchBodyExecutor == nil {
		return nil, batchBodyError(lease.Task.NodeName, batchIndex, ErrNoBatchBodyExecutor)
	}
	if lease.Task.NodeIdx < 0 || lease.Task.NodeIdx >= g.NodeCount() {
		return nil, batchBodyError(lease.Task.NodeName, batchIndex,
			fmt.Errorf("parent node index %d is out of range", lease.Task.NodeIdx))
	}
	meta := g.NodeAt(lease.Task.NodeIdx)
	body := g.BodyAt(lease.Task.NodeIdx)
	if body == nil {
		// Unreachable by construction: a batch exists only because this node
		// expanded, and expandsIntoSubExecutions expands only what has a body.
		// Kept as an assertion rather than a nil dereference two lines down.
		return nil, batchBodyError(lease.Task.NodeName, batchIndex,
			fmt.Errorf("node %q expanded into batches without a projected body", meta.Name))
	}

	allItems, batchSize := mapBatchingContext(t, len(items))
	continueOnError := mapContinueOnError(meta)
	// The outer submission's Runtime.Vars are the half of $vars that does not
	// travel inside the projected body package (Context.Vars does). Read from
	// the execution snapshot -- the same source buildInput uses for a regular
	// node -- because a batch task carries no input of its own.
	runtime, err := e.executionRuntime(ctx, lease.Task.ExecutionID)
	if err != nil {
		return nil, batchBodyError(lease.Task.NodeName, batchIndex, err)
	}
	itemResults, err := e.batchBodyExecutor.ExecuteBatchBody(ctx, BatchBodyRequest{
		ExecutionID:     string(lease.Task.ExecutionID),
		ParentNode:      lease.Task.NodeName,
		Body:            body.Package,
		BodyHash:        body.Hash,
		BatchIndex:      batchIndex,
		BatchSize:       batchSize,
		Items:           items,
		AllItems:        allItems,
		ContinueOnError: continueOnError,
		Runtime:         runtime,
	})
	if err != nil {
		// The body could not be RUN — compile failure, missing handler, backend
		// construction. Distinct from a body that ran and failed, and the
		// distinction is why this is a returned error: it is a system fault, so
		// the task's normal error path decides whether a retry is worth trying.
		return nil, batchBodyError(lease.Task.NodeName, batchIndex, err)
	}

	e.observeItemFailures(g.Name(), lease.Task.NodeName, itemResults)
	result, failure := BatchResultForCommit(itemResults, continueOnError)
	if failure != nil {
		result[batchErrorKey] = failure.Error()
	}
	return result, nil
}

// observeItemFailures reports this batch's failed-item count.
//
// It fires for a partially-failed batch as well as a wholly-failed one — the
// partial case is the one that needs it, because that batch commits as a success
// and the execution reports Success. A wholly-failed batch is already visible
// through the map node's own failure.
func (e *Engine) observeItemFailures(workflow, nodeName string, results []BatchItemResult) {
	if e.itemFailureObserver == nil {
		return
	}
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		}
	}
	if failed == 0 {
		return
	}
	e.itemFailureObserver.ObserveItemFailures(ObservedItemFailures{
		Workflow: workflow,
		NodeName: nodeName,
		Failed:   failed,
		Total:    len(results),
	})
}

// mapBatchingContext recovers the two things a body needs that its own items
// slice does not carry: the map node's whole items array (exposed as $items) and
// its batch_size (needed to turn a position within a batch into a global index).
//
// Both are read from the batch task's payload, where expansion stamped them.
// They cannot be read back from the map node's stored output: at the time a batch
// runs, the map node is still Waiting and has no output — it terminalizes only
// once every batch has reported.
//
// A payload missing them degrades rather than fails the batch: batch_size falls
// back to this batch's own length, which is exactly right for a single-batch
// expansion and merely makes $index batch-relative otherwise. Failing work that
// would otherwise succeed is the worse trade.
func mapBatchingContext(t *Task, batchLen int) ([]any, int) {
	if t == nil || t.Payload == nil || t.Payload.Data == nil {
		return nil, batchLen
	}
	data := t.Payload.Data
	allItems, _ := data["all_items"].([]any)
	batchSize, err := batchPayloadInt(data, "batch_size")
	if err != nil || batchSize <= 0 {
		batchSize = batchLen
	}
	return allItems, batchSize
}

// mapContinueOnError reads the map node's continue_on_error parameter. Only a
// literal true enables it: an unevaluated expression is not a promise that
// failures are tolerable.
func mapContinueOnError(meta graph.NodeMeta) bool {
	if meta.Parameters == nil {
		return false
	}
	enabled, _ := meta.Parameters["continue_on_error"].(bool)
	return enabled
}

// batchPayloadInt coerces one of the expansion payload's integer fields. A
// payload that round-tripped through JSON carries float64 where the expansion
// layer wrote int, so every reader has to accept all three.
func batchPayloadInt(data map[string]any, name string) (int, error) {
	switch value := data[name].(type) {
	case int:
		return value, nil
	case int64:
		return int(value), nil
	case float64:
		return int(value), nil
	default:
		return 0, fmt.Errorf("batch task has invalid %s", name)
	}
}

func expansionBatchLease(t *Task) (*TaskLease, types.ExecutionID, []any, error) {
	if t == nil || t.Payload == nil || t.Payload.Data == nil {
		return nil, "", nil, fmt.Errorf("batch task missing payload")
	}
	data := t.Payload.Data
	stringField := func(name string) (string, error) {
		value, ok := data[name].(string)
		if !ok || value == "" {
			return "", fmt.Errorf("batch task has invalid %s", name)
		}
		return value, nil
	}
	intField := func(name string) (int, error) { return batchPayloadInt(data, name) }

	parentExecID, err := stringField("parent_exec_id")
	if err != nil {
		return nil, "", nil, err
	}
	parentNode, err := stringField("parent_node")
	if err != nil {
		return nil, "", nil, err
	}
	childID, err := stringField("child_exec_id")
	if err != nil {
		return nil, "", nil, err
	}
	leaseID, err := stringField("parent_lease_id")
	if err != nil {
		return nil, "", nil, err
	}
	leaseToken, err := stringField("parent_lease_token")
	if err != nil {
		return nil, "", nil, err
	}
	nodeIdx, err := intField("parent_node_idx")
	if err != nil {
		return nil, "", nil, err
	}
	attempt, err := intField("parent_attempt")
	if err != nil {
		return nil, "", nil, err
	}
	activationID, err := intField("parent_activation_id")
	if err != nil {
		return nil, "", nil, err
	}
	autoDepth, err := intField("parent_auto_depth")
	if err != nil {
		return nil, "", nil, err
	}
	items, _ := data["items"].([]any)
	return &TaskLease{
		LeaseID:    LeaseID(leaseID),
		LeaseToken: LeaseToken(leaseToken),
		Attempt:    attempt,
		Task: Task{
			ExecutionID:  types.ExecutionID(parentExecID),
			NodeName:     parentNode,
			NodeIdx:      nodeIdx,
			Type:         TaskTypeNodeBatch,
			ActivationID: activationID,
			AutoDepth:    autoDepth,
		},
	}, types.ExecutionID(childID), items, nil
}

// batchErrorKey marks a batch result as a failure. It travels inside the batch's
// own result map because that map is the only thing CompleteExpandedSubExecution
// hands back at the all-done barrier: the batch that reports last is usually not
// the batch that failed, so the verdict cannot be derived from the final
// commit's own TaskResult.
const batchErrorKey = "_error"

// failedBatchErrors collects the error messages stamped on failed batches, in
// batch order. Empty means every batch succeeded.
func failedBatchErrors(results []map[string]any) []string {
	var msgs []string
	for _, result := range results {
		if result == nil {
			continue
		}
		if msg, ok := result[batchErrorKey].(string); ok {
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// flattenBatchResults turns the barrier's per-BATCH results into the per-ITEM
// array downstream sees. The barrier hands back one entry per batch, in batch
// order, each carrying its own items array — so concatenating them in order
// yields the items in their original order.
//
// This is what keeps batch_size invisible to semantics: count is the item count,
// so raising batch_size changes how many sub-executions ran and nothing about
// the array downstream reads. A failed item keeps its slot as {_error, _index},
// which is what makes count equal the input length even when items failed.
func flattenBatchResults(results []map[string]any) []any {
	flat := make([]any, 0, len(results))
	for _, batch := range results {
		if batch == nil {
			continue
		}
		items, ok := batch["items"].([]any)
		if !ok {
			// A batch that reports no items array at all — a failed batch whose
			// body never produced one. Keep the batch result itself so the
			// failure is visible rather than silently dropping a slot.
			flat = append(flat, batch)
			continue
		}
		flat = append(flat, items...)
	}
	return flat
}

// completeLoopSplit terminalizes a fully completed child generation through
// the same token-fenced commit path as other node results. In an acyclic graph
// that also writes the durable downstream advance intent.
//
// A generation with any failed batch is a failed map node: its results array has
// a hole where that batch's items should be, and committing success would fire
// downstream on silently incomplete data. The map node's own OnError decides
// what a failure means, exactly as it does for a node that failed directly.
func (e *Engine) completeLoopSplit(ctx context.Context, lease *TaskLease, g *graph.Graph, results []map[string]any) error {
	flat := flattenBatchResults(results)
	output := map[string]any{
		"results": flat,
		"count":   len(flat),
	}
	if failures := failedBatchErrors(results); len(failures) > 0 {
		return e.failLoopSplit(ctx, lease, g, output, failures, len(results))
	}
	outcome, err := e.commitLegacyNode(ctx, lease, types.NodeStatusSuccess, output, "main", "", false)
	if outcome == CommitOutcomeStaleToken || outcome == CommitOutcomeDuplicateTerminal || outcome == CommitOutcomeExecutionInactive {
		return nil
	}
	if err != nil {
		return fmt.Errorf("finalize loop/split node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}

// failLoopSplit terminalizes a map node whose generation contained a failed
// batch. It routes through ApplyOnError so "stop" aborts the execution while
// error_output/main_output/continue keep it running with the error attached,
// matching what any other failing node in the graph does.
//
// It deliberately does not retry. tryRetryWithAttempt would re-run the map node
// itself, re-expanding every batch — including the ones that already succeeded
// and already had their side effects. Retrying a partially applied expansion is
// a decision for the workflow author, not a default.
func (e *Engine) failLoopSplit(ctx context.Context, lease *TaskLease, g *graph.Graph, output map[string]any, failures []string, batchCount int) error {
	cause := fmt.Errorf("%d of %d batches failed: %s", len(failures), batchCount, strings.Join(failures, "; "))
	if lease.Task.NodeIdx < 0 || lease.Task.NodeIdx >= g.NodeCount() {
		return fmt.Errorf("finalize failed loop/split node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, cause)
	}
	meta := g.NodeAt(lease.Task.NodeIdx)
	// ApplyOnError's non-fatal strategies copy output.Data before merging the
	// error in, so the results array survives: a downstream error branch can
	// still see which batches did produce items.
	decision := ApplyOnError(meta.OnError, cause, nil, &types.Output{Data: output})
	outcome, err := e.commitLegacyNodeWithClassification(ctx, lease, decision.NodeStatus, decision.Output,
		decision.RoutePort, decision.ErrorMessage, decision.ExecFatal,
		buildEffectiveClassification(cause, nil, false))
	if outcome == CommitOutcomeStaleToken || outcome == CommitOutcomeDuplicateTerminal || outcome == CommitOutcomeExecutionInactive {
		return nil
	}
	if err != nil {
		return fmt.Errorf("finalize failed loop/split node %q/%q: %w", lease.Task.ExecutionID, lease.Task.NodeName, err)
	}
	return nil
}

// executionRuntime reads an execution's submission Runtime from the snapshot.
// A batch task is dispatched from the parent's expansion, not from the
// submission, so it is the only way the runtime half of $vars reaches a body.
// A missing snapshot is not an error here: the caller has already established
// the execution is active via loadActiveGraph, and an execution that finished
// in the window between simply has no runtime to forward.
func (e *Engine) executionRuntime(ctx context.Context, id types.ExecutionID) (*types.Runtime, error) {
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get execution %q: %w", id, err)
	}
	if snap == nil {
		return nil, nil
	}
	return cloneRuntime(snap.Runtime), nil
}
