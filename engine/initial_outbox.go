package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// initialTask couples the durable task intent with the legacy queue error
// description used when an external StateStore has not opted into the atomic
// scheduling extension.
type initialTask struct {
	task      Task
	operation string
}

func submitInitialTasks(id types.ExecutionID, g *graph.Graph) []initialTask {
	if g.AllowCycles() {
		start := g.NodeAt(g.StartIndex())
		return []initialTask{{
			task: Task{
				ExecutionID:  id,
				NodeName:     start.Name,
				NodeIdx:      g.StartIndex(),
				Type:         execTaskTypeForNode(start),
				ActivationID: 1,
			},
			operation: fmt.Sprintf("enqueue start node %q", start.Name),
		}}
	}

	tasks := make([]initialTask, 0)
	for unitIdx := 0; unitIdx < g.UnitCount(); unitIdx++ {
		if g.UnitInDegreeAt(unitIdx) != 0 {
			continue
		}
		if g.UnitKindAt(unitIdx) == graph.UnitGroup {
			gm := g.GroupMetaAt(unitIdx)
			tasks = append(tasks, initialTask{
				task: Task{
					ExecutionID: id,
					NodeName:    gm.Name,
					NodeIdx:     gm.EntryIdx,
					UnitIdx:     unitIdx,
					Type:        TaskTypeGroupExec,
				},
				operation: fmt.Sprintf("enqueue root group %q", gm.Name),
			})
			continue
		}
		nodeIdx := g.UnitNodeIndex(unitIdx)
		node := g.NodeAt(nodeIdx)
		tasks = append(tasks, initialTask{
			task: Task{
				ExecutionID: id,
				NodeName:    node.Name,
				NodeIdx:     nodeIdx,
				UnitIdx:     unitIdx,
				Type:        execTaskTypeForNode(node),
			},
			operation: fmt.Sprintf("enqueue root node %q", node.Name),
		})
	}
	return tasks
}

// startExecution chooses the durable creation protocol whenever the StateStore
// supplies it. Once CreateExecutionWithOutbox succeeds, an inline queue
// outage cannot invalidate the execution: the background OutboxDispatcher
// will retry delivery from durable state. Legacy StateStores keep their
// original create-then-enqueue semantics for backwards compatibility.
//
// A graph that declares settings.concurrency takes the concurrency admission
// path instead, and fails closed when the StateStore cannot enforce one. A
// graph containing sub-workflow nodes fails closed even earlier, when the
// engine cannot resolve or park one (admitSubWorkflows).
func (e *Engine) startExecution(ctx context.Context, snap *ExecutionSnapshot, tasks []initialTask) (types.ExecutionID, error) {
	if err := e.admitSubWorkflows(snap.Graph); err != nil {
		return "", err
	}
	// Submit, Invoke and a sub-workflow child all create their snapshot here, so
	// this is the one place the execution deadline is stamped for them. It is
	// stamped before the concurrency admission below: a queued execution's wait
	// counts against the budget, so the clock starts at admission.
	stampExecutionDeadline(snap)
	admission := concurrencyAdmissionFor(ctx, snap.Graph)
	if admission != nil {
		return e.startLimitedExecution(ctx, snap, tasks, admission)
	}
	if state, ok := e.state.(AtomicStateStore); ok {
		entries := make([]OutboxEntry, 0, len(tasks))
		for _, initial := range tasks {
			entries = append(entries, OutboxEntry{
				ID:   initialOutboxID(snap.ID, initial.task.NodeName, initial.task.ActivationID),
				Task: initial.task,
			})
		}
		if err := state.CreateExecutionWithOutbox(ctx, snap, entries); err != nil {
			return "", fmt.Errorf("create execution: %w", err)
		}
		e.cacheExecutionGraph(snap.ID, snap.Graph)
		e.flushInitialOutbox(ctx, snap.ID)
		return snap.ID, nil
	}

	if err := e.state.CreateExecution(ctx, snap); err != nil {
		return "", fmt.Errorf("create execution: %w", err)
	}
	e.cacheExecutionGraph(snap.ID, snap.Graph)
	for _, initial := range tasks {
		task := initial.task
		// A skip intent is resolved by the atomic commit protocol this legacy
		// store does not provide; enqueueing it would only fail on dequeue.
		if task.Type == TaskTypeNodeSkip {
			continue
		}
		if err := e.queue.Enqueue(ctx, &task); err != nil {
			return "", e.failInitialExecution(ctx, snap.ID, initial.operation, err)
		}
	}
	return snap.ID, nil
}

// startLimitedExecution is the creation path for a graph that declares
// settings.concurrency.
//
// Every refusal happens before the store is asked to persist anything, so a
// rejected submission leaves no execution, no graph cache entry and no task —
// which is what makes "reject" observable as a 429 rather than as a
// zero-length execution.
func (e *Engine) startLimitedExecution(ctx context.Context, snap *ExecutionSnapshot, tasks []initialTask, admission *ConcurrencyAdmission) (types.ExecutionID, error) {
	// The capability assertion is deliberately made here and not at
	// registration: a StateStore can gain the capability in a new build without
	// the workflow having to be re-registered, and the failure mode of getting
	// it wrong (running unlimited) is the one the limit exists to prevent.
	state, ok := e.state.(ConcurrencyLimitedStateStore)
	if !ok {
		return "", fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyUnsupported)
	}

	entries := make([]OutboxEntry, 0, len(tasks))
	for _, initial := range tasks {
		entries = append(entries, OutboxEntry{
			ID:   initialOutboxID(snap.ID, initial.task.NodeName, initial.task.ActivationID),
			Task: initial.task,
		})
	}
	res, err := state.CreateExecutionWithConcurrency(ctx, snap, entries, *admission)
	if err != nil {
		// QUEUE mode refused by the store is returned as-is rather than dressed
		// up as a creation failure: a definition that uses the queue is not a
		// broken submission, it is asking for a capability the store has said it
		// lacks, and that distinction is what makes the refusal actionable.
		// A full queue is the same shape of answer — a refusal the submitter can
		// act on — so it keeps its sentinel and gains the workflow it belongs to.
		switch {
		case errors.Is(err, ErrConcurrencyQueueUnsupported):
			return "", fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyQueueUnsupported)
		case errors.Is(err, ErrConcurrencyQueueFull):
			return "", fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyQueueFull)
		}
		return "", fmt.Errorf("create execution: %w", err)
	}
	e.cacheExecutionGraph(snap.ID, snap.Graph)
	if res.Queued {
		// The store holds these intents until promotion; delivering them now
		// would start a queued execution's roots while it holds no slot.
		return snap.ID, nil
	}
	e.flushInitialOutbox(ctx, snap.ID)
	return snap.ID, nil
}

func (e *Engine) cacheExecutionGraph(id types.ExecutionID, g *graph.Graph) {
	e.graphs.Put(id, g, g.EstimatedHeapBytes())
}

// flushInitialOutbox is intentionally best-effort. State and the initial
// durable intents were committed together, so reporting a temporary queue
// outage as a failed submission would misrepresent a still-recoverable,
// running execution and encourage callers to create duplicates.
func (e *Engine) flushInitialOutbox(ctx context.Context, id types.ExecutionID) {
	if err := e.FlushOutbox(ctx, id); err != nil && e.logger != nil {
		e.logger.Error("initial durable outbox delivery deferred", "execution_id", string(id), "err", err)
	}
}
