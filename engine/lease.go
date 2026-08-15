package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// ExecutionTraceCarrier returns the W3C traceparent/tracestate carrier
// captured at submission and persisted on the execution snapshot. The control
// plane uses it to parent the asynchronous dispatch span to the original
// submit/invoke trace via a real W3C ExtractCarrier round-trip (not a
// trace_id/span_id string parent). Returns nil when the execution is gone or
// tracing was disabled at submission time.
func (e *Engine) ExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, error) {
	snap, err := e.state.GetExecution(ctx, id)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, nil
	}
	return snap.TraceCarrier, nil
}

// BuildTaskLease assembles a runner-facing task lease from a queued task.
// The lease includes both handler routing metadata and the concrete input, so
// runner-side code does not need to access graph or state internals.
func (e *Engine) BuildTaskLease(ctx context.Context, t *Task) (*TaskLease, error) {
	if t == nil {
		return nil, fmt.Errorf("build task lease: nil task")
	}
	if handled, err := e.HandleSystemTask(ctx, t); err != nil {
		return nil, err
	} else if handled {
		return nil, ErrSystemTaskHandled
	}
	// A batch task names a synthetic node that the compiled graph never
	// declared, so the node-lease path below would write live state for a node
	// that does not exist and commit it as an ordinary node result — firing
	// downstream on the first batch instead of the last. Batches have their own
	// lease path; fail closed rather than mint that lease here.
	if t.Type == TaskTypeNodeBatch {
		return nil, ErrBatchLeaseRequired
	}

	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, ErrExecutionInactive
	}
	if _, err := e.checkTaskRouteActive(ctx, g, t); err != nil {
		return nil, err
	}

	leaseID := LeaseID("lease-" + uuid.New().String())
	leaseToken := LeaseToken("token-" + uuid.New().String())
	issuedAt := time.Now().UTC()
	ttl := e.defaultLeaseTTL
	lease := &TaskLease{
		LeaseID:    leaseID,
		LeaseToken: leaseToken,
		Task:       *t,
		IssuedAt:   issuedAt,
		TTL:        ttl,
	}
	prev, acquired, err := e.state.AcquireTaskLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, e.classifyLeaseAcquireFailure(g, t, prev, issuedAt)
	}

	// Attempt counts retries WITHIN a single activation. A cyclic node that
	// re-enters carries a new (higher) ActivationID; that is a fresh execution
	// of the node, not a retry, so the attempt counter must restart at 1.
	// Carrying prev.Attempt+1 across activation boundaries would let a node that
	// loops N times exhaust its per-activation MaxAttempts budget and be
	// misclassified as permanently failed.
	lease.Attempt = 1
	if prev != nil && prev.ActivationID == t.ActivationID {
		lease.Attempt = prev.Attempt + 1
	}

	meta := g.NodeAt(t.NodeIdx)
	input, err := e.buildInput(ctx, t, g)
	if err != nil {
		released, releaseErr := e.ReleaseTaskLease(ctx, lease)
		if releaseErr != nil {
			return nil, fmt.Errorf("build input for %q/%q: %w (release lease: %v)", t.ExecutionID, t.NodeName, err, releaseErr)
		}
		if !released {
			return nil, fmt.Errorf("build input for %q/%q: %w (lease was no longer active)", t.ExecutionID, t.NodeName, err)
		}
		return nil, fmt.Errorf("build input for %q/%q: %w", t.ExecutionID, t.NodeName, err)
	}
	lease.Input = input
	lease.NodeType = meta.Type
	lease.NodeVersion = meta.Version

	started := prev == nil || prev.Status != types.NodeStatusRunning
	if started && e.hooks != nil {
		safeHook(ctx, e.logger, func(hookCtx context.Context) {
			e.hooks.OnNodeStart(hookCtx, t.ExecutionID, t.NodeName)
		})
	}
	return lease, nil
}

// RecoverTaskLease rebuilds the runner-facing representation of an already
// issued lease without mutating node state. It closes the durable handoff gap
// where a control-plane process crashes after BuildTaskLease has committed the
// engine lease but before the RunnerDirectory records or delivers it.
//
// The returned lease preserves the existing ID, token, attempt, issue time,
// and TTL. Callers must only use it to replay the same task; issuing a fresh
// lease remains the responsibility of BuildTaskLease after fenced revocation.
func (e *Engine) RecoverTaskLease(ctx context.Context, task *Task) (*TaskLease, error) {
	if task == nil {
		return nil, fmt.Errorf("recover task lease: nil task")
	}
	// A batch is leased to a runner, so a response-loss replay must be able to
	// rebuild it. BuildSubgraphLease mutates nothing — it derives the lease from
	// the task payload and the graph — so rebuilding is a replay of the same
	// lease, not the issue of a second one. NodeAdvance and NodeSkip remain
	// unrecoverable: they are control-plane-internal and never leave the engine.
	if task.Type == TaskTypeNodeBatch {
		lease, _, err := e.BuildSubgraphLease(ctx, task)
		return lease, err
	}
	if task.Type == TaskTypeNodeAdvance || task.Type == TaskTypeNodeSkip {
		return nil, ErrLeaseNotRecoverable
	}
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return nil, err
	}
	if !active || task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() || g.NodeAt(task.NodeIdx).Name != task.NodeName {
		return nil, ErrExecutionInactive
	}

	node, err := e.state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil {
		return nil, fmt.Errorf("recover task lease %q/%q: get node: %w", task.ExecutionID, task.NodeName, err)
	}
	if node == nil || node.Status != types.NodeStatusRunning || node.LeaseID == "" || node.LeaseToken == "" {
		return nil, ErrLeaseNotRecoverable
	}
	if task.ActivationID > 0 && node.ActivationID != task.ActivationID {
		return nil, ErrExecutionInactive
	}
	if !node.LeaseIssuedAt.IsZero() && node.LeaseTTL > 0 && !time.Now().Before(node.LeaseIssuedAt.Add(node.LeaseTTL)) {
		return nil, ErrLeaseNotRecoverable
	}

	input, err := e.buildInput(ctx, task, g)
	if err != nil {
		return nil, fmt.Errorf("recover task lease %q/%q: build input: %w", task.ExecutionID, task.NodeName, err)
	}
	meta := g.NodeAt(task.NodeIdx)
	return &TaskLease{
		LeaseID:     node.LeaseID,
		LeaseToken:  node.LeaseToken,
		Attempt:     node.Attempt,
		Task:        *task,
		Input:       input,
		NodeType:    meta.Type,
		NodeVersion: meta.Version,
		IssuedAt:    node.LeaseIssuedAt,
		TTL:         node.LeaseTTL,
	}, nil
}

// TaskRouting returns runner placement metadata for a queued task without
// issuing a lease or mutating node attempt state.
func (e *Engine) TaskRouting(ctx context.Context, t *Task) (TaskRouting, error) {
	g, active, err := e.loadActiveGraph(ctx, t.ExecutionID)
	if err != nil {
		return TaskRouting{}, err
	}
	if !active {
		return TaskRouting{}, ErrExecutionInactive
	}
	if _, err := e.checkTaskRouteActive(ctx, g, t); err != nil {
		return TaskRouting{}, err
	}

	unitIdx := t.UnitIdx
	if unitIdx >= 0 && unitIdx < g.UnitCount() && g.UnitKindAt(unitIdx) == graph.UnitGroup {
		gm := g.GroupMetaAt(unitIdx)
		pkg, _, err := graph.ProjectGroupPackage(g, unitIdx)
		if err != nil {
			return TaskRouting{}, fmt.Errorf("project group package for routing: %w", err)
		}
		return TaskRouting{
			NodeType:       GroupNodeType,
			RunnerSelector: cloneRunnerSelector(gm.RunnerSelector),
			Requirements:   RequirementsFromGraphPackage(pkg.Requirements),
		}, nil
	}

	meta := g.NodeAt(t.NodeIdx)
	return TaskRouting{
		NodeType:       meta.Type,
		NodeVersion:    meta.Version,
		RunnerSelector: cloneRunnerSelector(meta.RunnerSelector),
		Requirements: NormalizeRequirements([]CapabilityRequirement{{
			NodeType:    meta.Type,
			NodeVersion: meta.Version,
		}}),
	}, nil
}

func cloneRunnerSelector(selector *types.RunnerSelector) *types.RunnerSelector {
	if selector == nil {
		return nil
	}
	out := &types.RunnerSelector{
		Mode: selector.Mode,
	}
	if len(selector.MatchLabels) > 0 {
		out.MatchLabels = make(map[string]string, len(selector.MatchLabels))
		for key, value := range selector.MatchLabels {
			out.MatchLabels[key] = value
		}
	}
	return out
}

// classifyNodeForTask captures the route-staleness checks shared between
// checkTaskRouteActive and classifyLeaseAcquireFailure. It returns nil if the
// task is still routable to the node, otherwise the reason the route is
// inactive (ErrExecutionInactive). The lease-active check is intentionally
// handled by classifyLeaseAcquireFailure only.
func (e *Engine) classifyNodeForTask(g *graph.Graph, t *Task, ns *NodeSnapshot) error {
	if g.AllowCycles() && t.ActivationID <= 0 {
		return ErrExecutionInactive
	}
	if ns != nil && g.AllowCycles() && ns.ActivationID > t.ActivationID {
		return ErrExecutionInactive
	}
	if ns != nil && types.IsTerminalNodeStatus(ns.Status) && (!g.AllowCycles() || ns.ActivationID >= t.ActivationID) {
		return ErrExecutionInactive
	}
	if ns != nil && ns.Status == types.NodeStatusCommitting {
		return ErrExecutionInactive
	}
	return nil
}

func (e *Engine) checkTaskRouteActive(ctx context.Context, g *graph.Graph, t *Task) (*NodeSnapshot, error) {
	ns, err := e.state.GetNode(ctx, t.ExecutionID, t.NodeName)
	if err != nil {
		return nil, err
	}
	if cerr := e.classifyNodeForTask(g, t, ns); cerr != nil {
		return nil, cerr
	}
	return ns, nil
}

func (e *Engine) classifyLeaseAcquireFailure(g *graph.Graph, t *Task, ns *NodeSnapshot, now time.Time) error {
	if cerr := e.classifyNodeForTask(g, t, ns); cerr != nil {
		return cerr
	}
	if ns != nil && ns.Status == types.NodeStatusRunning && ns.LeaseToken != "" {
		deadline := ns.LeaseIssuedAt.Add(ns.LeaseTTL)
		if ns.LeaseIssuedAt.IsZero() || ns.LeaseTTL <= 0 || now.Before(deadline) {
			return ErrLeaseAlreadyActive
		}
	}
	return ErrExecutionInactive
}

// ReleaseTaskLease immediately releases a lease that an execution boundary
// knows was never handed to a handler or remote runner. It verifies the lease
// token before resetting the node and records the exact original task in the
// durable outbox when supported, so a stale delivery cannot revoke a newer
// owner or lose resume payload metadata.
//
// Unknown execution outcomes must not use this method: leave those leases in
// place for their normal expiry and recovery path to avoid duplicate effects.
func (e *Engine) ReleaseTaskLease(ctx context.Context, lease *TaskLease) (bool, error) {
	if lease == nil || lease.LeaseToken == "" {
		return false, ErrInvalidLeaseToken
	}

	task := lease.Task
	if state, ok := e.state.(AtomicStateStore); ok {
		released, err := state.RevokeLeaseWithOutbox(ctx, task.ExecutionID, task.NodeName, lease.LeaseToken, OutboxEntry{
			ID:   requeueOutboxID(task.ExecutionID, task.NodeName, task.ActivationID, lease.LeaseID),
			Task: task,
		})
		if err != nil {
			return false, fmt.Errorf("release task lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		if !released {
			return false, nil
		}
		if err := e.FlushOutbox(ctx, task.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver released task outbox %q/%q: %w", task.ExecutionID, task.NodeName, err)
		}
		return true, nil
	}

	released, err := e.state.RevokeLease(ctx, task.ExecutionID, task.NodeName, lease.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("release task lease %q/%q: %w", task.ExecutionID, task.NodeName, err)
	}
	if !released {
		return false, nil
	}
	if err := e.queue.Enqueue(ctx, &task); err != nil {
		// Enqueue failed after RevokeLease succeeded — the node is now Pending
		// with no in-flight task AND no active lease. The lease sweeper CANNOT
		// recover it: ListExpiredLeases only returns Running/Committing/Waiting
		// nodes with an expired lease, so a Pending-no-lease node is invisible to
		// it. This legacy (non-AtomicStateStore) path therefore has no automatic
		// recovery; the error is surfaced to the caller. Atomic backends avoid the
		// window entirely by persisting the revocation and redelivery intent in
		// one durable-outbox transition (see RevokeLeaseWithOutbox below). The
		// only legacy backend is the local in-memory queue, whose Enqueue fails
		// only on shutdown or context cancellation, so this is not a steady-state
		// data-loss path — but callers must not assume the node self-heals.
		return true, fmt.Errorf("re-enqueue released task %q/%q (node left pending, not auto-recoverable): %w", task.ExecutionID, task.NodeName, err)
	}
	return true, nil
}

// ReclaimLease revokes an expired task lease and re-enqueues the exact queued
// task so a healthy runner can pick it up. The persisted task type and resume
// payload are required for committing-state recovery: replaying a resume as a
// normal execution would lose the external signal that selected its path.
// Atomic StateStores persist the token-fenced revocation and redelivery intent
// together.
func (e *Engine) ReclaimLease(ctx context.Context, lease ExpiredLease) (bool, error) {
	if lease.LeaseToken == "" {
		return false, nil
	}
	// Restore the namespace that owns this lease. The sweeper runs with the
	// default namespace in its context, but ListExpiredLeases discovers leases
	// across all namespaces and stamps each ExpiredLease with its owner.
	// Reclaim must operate in that owner's namespace or the store keys will
	// not match and the lease will be silently fail-closed.
	if lease.Namespace != "" {
		ctx = namespace.WithNamespace(ctx, lease.Namespace)
	}
	task := Task{
		ExecutionID:  lease.ExecutionID,
		NodeName:     lease.NodeName,
		NodeIdx:      lease.NodeIdx,
		UnitIdx:      lease.UnitIdx,
		Type:         lease.TaskType,
		Payload:      cloneSignalPayload(lease.Payload),
		ActivationID: lease.ActivationID,
		AutoDepth:    lease.AutoDepth,
	}
	// A group unit leases through its own state (group:<unitIdx>:status/meta),
	// not through the entry node's. Sending it down the node path below would
	// fence against a node that never entered "running", so the revoke returns
	// false and the sweeper records the unit as already handled — leaving it
	// "running" forever, since AcquireGroupLease refuses a running unit.
	if lease.TaskType == TaskTypeGroupExec {
		return e.reclaimGroupLease(ctx, lease, task)
	}
	if state, ok := e.state.(AtomicStateStore); ok {
		revoked, err := state.RevokeLeaseWithOutbox(ctx, lease.ExecutionID, lease.NodeName, lease.LeaseToken, OutboxEntry{
			ID:   requeueOutboxID(lease.ExecutionID, lease.NodeName, lease.ActivationID, lease.LeaseID),
			Task: task,
		})
		if err != nil {
			return false, fmt.Errorf("revoke lease %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
		}
		if !revoked {
			return false, nil
		}
		if err := e.FlushOutbox(ctx, lease.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver reclaimed task outbox %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
		}
		return true, nil
	}

	revoked, err := e.state.RevokeLease(ctx, lease.ExecutionID, lease.NodeName, lease.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("revoke lease %q/%q: %w", lease.ExecutionID, lease.NodeName, err)
	}
	if !revoked {
		return false, nil
	}
	if err := e.queue.Enqueue(ctx, &task); err != nil {
		// Enqueue failed after RevokeLease succeeded — the node is Pending with no
		// in-flight task AND no active lease, so the sweeper cannot rediscover it
		// (ListExpiredLeases only returns leased non-terminal nodes). This legacy
		// (non-AtomicStateStore) path has no automatic recovery; the atomic path
		// above avoids the window via the durable requeue outbox. Surface the
		// error rather than implying the node self-heals.
		return true, fmt.Errorf("re-enqueue reclaimed task %q/%q (node left pending, not auto-recoverable): %w", lease.ExecutionID, lease.NodeName, err)
	}
	return true, nil
}

// reclaimGroupLease is ReclaimLease's group-unit path. It mirrors the node path
// exactly: the preferred backend persists the token-fenced expiry and the
// redelivery intent in one transition, and legacy backends fall back to
// expire-then-enqueue with the same non-recoverable window the node path
// documents.
func (e *Engine) reclaimGroupLease(ctx context.Context, lease ExpiredLease, task Task) (bool, error) {
	entry := OutboxEntry{
		ID:   requeueGroupOutboxID(lease.ExecutionID, lease.UnitIdx, lease.LeaseID),
		Task: task,
	}
	if state, ok := e.state.(GroupLeaseReclaimer); ok {
		revoked, err := state.RevokeGroupLeaseWithOutbox(ctx, lease.ExecutionID, lease.UnitIdx, lease.LeaseToken, entry)
		if err != nil {
			return false, fmt.Errorf("revoke group lease %q/#%d: %w", lease.ExecutionID, lease.UnitIdx, err)
		}
		if !revoked {
			return false, nil
		}
		if err := e.FlushOutbox(ctx, lease.ExecutionID); err != nil {
			return true, fmt.Errorf("deliver reclaimed group task %q/#%d: %w", lease.ExecutionID, lease.UnitIdx, err)
		}
		return true, nil
	}

	expirer, ok := e.state.(GroupLeaseExpirer)
	if !ok {
		return false, fmt.Errorf("reclaim group lease %q/#%d: state store does not support group lease expiry", lease.ExecutionID, lease.UnitIdx)
	}
	expired, err := expirer.ExpireGroupLease(ctx, lease.ExecutionID, lease.UnitIdx, lease.LeaseToken)
	if err != nil {
		return false, fmt.Errorf("expire group lease %q/#%d: %w", lease.ExecutionID, lease.UnitIdx, err)
	}
	if !expired {
		return false, nil
	}
	if err := e.queue.Enqueue(ctx, &task); err != nil {
		// Same window the node path documents: the unit is now pending with no
		// queued task and no lease, so ListExpiredLeases can no longer see it.
		// Backends implementing GroupLeaseReclaimer avoid the window entirely.
		return true, fmt.Errorf("re-enqueue reclaimed group task %q/#%d (unit left pending, not auto-recoverable): %w", lease.ExecutionID, lease.UnitIdx, err)
	}
	return true, nil
}

// NodeLeaseRenewer is the node-level analogue of
// GroupStateStore.RenewGroupLease: it extends a live lease's deadline under a
// token fence, without touching the token, attempt counter, or status.
//
// Implementations MUST move whatever the expired-lease scan reads, not just the
// stored metadata. On backends that keep a separate expiry index, a renewal
// that updates only the metadata leaves the sweeper reclaiming a node whose
// runner is healthily working.
type NodeLeaseRenewer interface {
	RenewTaskLease(ctx context.Context, id types.ExecutionID, name string, token LeaseToken, deadline time.Time) (renewed bool, err error)
}

// RenewTaskLease extends the deadline of a live node lease so a handler that
// legitimately runs longer than the engine's default lease TTL is not reclaimed
// mid-flight.
//
// It exists because BuildTaskLease stamps every lease with defaultLeaseTTL and
// nothing clamps a node's own timeout against it: an action whose timeout
// exceeds the TTL would otherwise be swept and redelivered to a second runner
// while the first is still executing it.
//
// A batch lease renews its parent, for the same reason CommitTaskResult routes
// a batch through the expansion barrier: the batch's node name is synthetic
// ("loop/_batch/0"), BuildSubgraphLease deliberately never writes it to state,
// and the lease it carries is the parent's. Renewing under the synthetic name
// would address a node that does not exist — the store refuses, the runner
// reads the refusal as "you lost the lease" and cancels a healthy batch, and
// the parent whose deadline is actually ticking down never gets extended.
func (e *Engine) RenewTaskLease(ctx context.Context, lease *TaskLease, extend time.Duration) (bool, error) {
	if lease == nil || lease.LeaseToken == "" {
		return false, ErrInvalidLeaseToken
	}
	name := lease.Task.NodeName
	if lease.SubgraphPayload != nil && lease.SubgraphPayload.ParentNode != "" {
		name = lease.SubgraphPayload.ParentNode
	}
	if extend <= 0 {
		return false, fmt.Errorf("renew node lease %q/%q: extend must be positive", lease.Task.ExecutionID, name)
	}
	renewer, ok := e.state.(NodeLeaseRenewer)
	if !ok {
		return false, fmt.Errorf("renew node lease %q/%q: state store does not support node lease renewal",
			lease.Task.ExecutionID, name)
	}
	deadline := time.Now().UTC().Add(extend)
	renewed, err := renewer.RenewTaskLease(ctx, lease.Task.ExecutionID, name, lease.LeaseToken, deadline)
	if err != nil {
		return false, fmt.Errorf("renew node lease %q/%q: %w", lease.Task.ExecutionID, name, err)
	}
	return renewed, nil
}
