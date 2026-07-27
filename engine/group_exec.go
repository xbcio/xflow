package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/google/uuid"
)

// GroupExit is one boundary output produced by a member node during group
// execution. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// GroupExecutor runs one pinned group unit locally and returns its boundary
// outputs. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExecutor interface {
	ExecuteGroup(ctx context.Context, task *Task, meta graph.GroupMeta) (exits []GroupExit, fatal bool, err error)
}

// WithGroupExecutor sets the optional group executor capability.
func WithGroupExecutor(ge GroupExecutor) Option {
	return func(e *Engine) { e.groupExecutor = ge }
}

// executeGroup handles a TaskTypeGroupExec task: acquires a group lease, calls
// the GroupExecutor, and commits the result. commitGroup (Task 12) will
// implement the actual downstream propagation; for now it is a stub.
func (e *Engine) executeGroup(ctx context.Context, task *Task, flush bool) error {
	if e.groupExecutor == nil {
		return fmt.Errorf("no group executor configured for group %q", task.NodeName)
	}
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if task.UnitIdx < 0 || task.UnitIdx >= g.UnitCount() || g.UnitKindAt(task.UnitIdx) != graph.UnitGroup {
		return fmt.Errorf("group exec unit %d out of range or not a group", task.UnitIdx)
	}
	meta := g.GroupMetaAt(task.UnitIdx)

	// Build group lease. Same ID/TTL generation as BuildTaskLease (lease.go).
	lease := &GroupLease{
		ExecutionID:  task.ExecutionID,
		GroupUnitIdx: task.UnitIdx,
		LeaseID:      LeaseID("lease-" + uuid.New().String()),
		LeaseToken:   LeaseToken("token-" + uuid.New().String()),
		Attempt:      1, // Milestone A: group retry (attempt increment) belongs to Milestone B
		IssuedAt:     time.Now().UTC(),
		TTL:          e.defaultLeaseTTL,
	}

	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return fmt.Errorf("state store does not support group leases")
	}
	acquired, err := gs.AcquireGroupLease(ctx, lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil // already owned by another executor; safe to discard
	}

	exits, fatal, execErr := e.groupExecutor.ExecuteGroup(ctx, task, meta)
	return e.commitGroup(ctx, g, lease, meta, exits, fatal, execErr, flush)
}

// commitGroup is a stub for Task 12. It will commit the group result, propagate
// downstream arrivals, and finalize the execution if all units are done.
func (e *Engine) commitGroup(_ context.Context, g *graph.Graph, lease *GroupLease, meta graph.GroupMeta, exits []GroupExit, fatal bool, execErr error, flush bool) error {
	_ = g
	_ = lease
	_ = meta
	_ = exits
	_ = fatal
	_ = execErr
	_ = flush
	// Placeholder for Task 12: will build GroupCommitRequest and call
	// GroupStateStore.CommitGroup, then FlushOutbox if flush==true.
	return nil
}

// downstreamUnitArrivals is a placeholder for Task 12: computes DownstreamArrival
// entries from group boundary outputs for the unit-level advance path.
func downstreamUnitArrivals(_ *graph.Graph, _ int, _ []GroupExit) []DownstreamArrival {
	return nil
}
