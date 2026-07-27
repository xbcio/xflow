package engine

import (
	"context"
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
				Type:         TaskTypeNodeExec,
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
				Type:        TaskTypeNodeExec,
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
func (e *Engine) startExecution(ctx context.Context, snap *ExecutionSnapshot, tasks []initialTask) (types.ExecutionID, error) {
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
		if err := e.queue.Enqueue(ctx, &task); err != nil {
			return "", e.failInitialExecution(ctx, snap.ID, initial.operation, err)
		}
	}
	return snap.ID, nil
}

func (e *Engine) cacheExecutionGraph(id types.ExecutionID, g *graph.Graph) {
	e.mu.Lock()
	e.graphs[id] = g
	e.mu.Unlock()
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
