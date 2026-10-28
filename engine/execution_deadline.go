package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// stampExecutionDeadline sets snap.Deadline to CreatedAt + Graph.Timeout() when
// the graph declares settings.timeout, and leaves the snapshot untouched
// otherwise (zero Deadline means no execution-level budget).
//
// The backends own CreatedAt, but they stamp it inside the create call, after
// the engine has already built the snapshot, so the engine cannot read the
// instant it is about to be given. It therefore stamps CreatedAt itself, from
// the same clock reading the deadline is derived from, and a backend that
// preserves a non-zero CreatedAt (the local one does) then reports exactly
// Deadline - Timeout as the start. Only a graph with a timeout pays this: any
// other snapshot reaches the backend unchanged.
func stampExecutionDeadline(snap *ExecutionSnapshot) {
	if snap.Graph == nil || snap.Graph.Timeout() <= 0 {
		return
	}
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	snap.Deadline = snap.CreatedAt.Add(snap.Graph.Timeout())
}

// clampToExecutionDeadline tightens d, a lease or wait deadline, to the
// execution deadline: the earlier of the two wins. A zero execDeadline (no
// execution-level budget) returns d unchanged, and a zero d (unbounded) takes
// the execution deadline, so an unlimited node still ends with its execution.
func clampToExecutionDeadline(execDeadline, d time.Time) time.Time {
	if execDeadline.IsZero() {
		return d
	}
	if d.IsZero() || execDeadline.Before(d) {
		return execDeadline
	}
	return d
}

// executionDeadline returns the execution-level deadline the lease and wait
// paths clamp against, or the zero time when the execution has none.
//
// Only a graph declaring settings.timeout can have one, so every other graph
// answers from g alone and never touches the store: the paths that call this
// stay byte-identical for it. The deadline is written once at creation and
// never changes, which is what lets the answer be memoized per execution (the
// same reasoning as ExecutionTraceCarrier); EvictExecution drops it together
// with the graph. It is read through GetExecution rather than the narrow
// input read because that read would have to grow the field in every backend
// at once, and it costs one read per execution, not one per lease.
func (e *Engine) executionDeadline(ctx context.Context, id types.ExecutionID, g *graph.Graph) (time.Time, error) {
	if g == nil || g.Timeout() <= 0 {
		return time.Time{}, nil
	}
	e.mu.RLock()
	cached, hit := e.executionDeadlines[id]
	e.mu.RUnlock()
	if hit {
		return cached, nil
	}
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return time.Time{}, fmt.Errorf("get execution %q: %w", id, err)
	}
	if snap == nil {
		return time.Time{}, nil
	}
	e.mu.Lock()
	if e.executionDeadlines == nil {
		e.executionDeadlines = make(map[types.ExecutionID]time.Time)
	}
	e.executionDeadlines[id] = snap.Deadline
	e.mu.Unlock()
	return snap.Deadline, nil
}
