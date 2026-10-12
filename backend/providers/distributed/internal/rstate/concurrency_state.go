package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Workflow concurrency (engine.ConcurrencyLimitedStateStore)
//
// The slot set lives in a workflow-tagged hash slot while every execution key
// lives in its own {id} slot, and Redis Cluster will not let one Lua script
// touch both. Admission and release are therefore separate transitions from the
// execution writes they bracket:
//
//	charge slot (workflow slot)  ->  create execution + tag (execution slot)
//	execution terminal (execution slot)  ->  release slot (workflow slot)
//
// Neither window is closed by atomicity; both are closed by ordering and made
// recoverable. A slot charged for an execution that never landed leaves no
// execution record, and a terminal execution whose release was lost still names
// the set it belongs to — so the reconciler can sweep both. See
// concurrency_reconcile.go.
// ---------------------------------------------------------------------------

// concurrencySlotTTL bounds how long a slot set outlives its last write. It
// tracks the execution retention, floored well above the reconciler's cadence
// so a leaked slot is always observable before the set can expire underneath
// the sweep that is supposed to find it.
func (s *Store) concurrencySlotTTL() time.Duration {
	ttl := s.execTTL
	if s.transientTTL > ttl {
		ttl = s.transientTTL
	}
	if ttl < time.Hour {
		ttl = time.Hour
	}
	return ttl
}

// concurrencySlotMember names one execution's slot inside its workflow's ZSET.
// The score is admission time, which is what a QUEUE-mode promotion orders by;
// the member is the execution ID alone because the set belongs to exactly one
// workflow.
func concurrencySlotMember(id types.ExecutionID) string {
	return string(id)
}

// concurrencyTagKey records which workflow a slot-bearing execution was
// admitted under, in the execution's own hash slot. Release has only the
// execution ID in hand while the slot key is derived from the workflow, so
// without this back-reference a terminal execution could not find the set it
// has to leave.
func concurrencyTagKey(t namespace.Namespace, id types.ExecutionID) string {
	return execKey(t, id, "conc_wf")
}

var _ engine.ConcurrencyLimitedStateStore = (*Store)(nil)

// CreateExecutionWithConcurrency admits one submission against its workflow's
// slot set, then persists it.
//
// QUEUE mode is refused before anything is written. That refusal lives here
// rather than in the engine because only the store knows whether it can park an
// execution; a store that admitted an over-limit submission into a queue it
// does not have would silently break the limit the definition declared.
func (s *Store) CreateExecutionWithConcurrency(ctx context.Context, execution *engine.ExecutionSnapshot, entries []engine.OutboxEntry, admission engine.ConcurrencyAdmission) (engine.ConcurrencyAdmissionResult, error) {
	if admission.Mode == types.ConcurrencyModeQueue {
		return engine.ConcurrencyAdmissionResult{}, engine.ErrConcurrencyQueueUnsupported
	}

	// The namespace is taken from the context, like every other scoped write in
	// this store. The admission's own copy is not consulted: two sources for one
	// scope could disagree, and the context's is the one a later release will
	// read back.
	t := namespace.FromContext(ctx)
	workflow := admission.Workflow
	tag := workflowDigestTag(t, workflow)
	admitted, err := workflowAdmitLua.Run(ctx, s.rdb,
		[]string{concurrencySlotKey(t, workflow), concurrencyWorkflowIndexKey(t)},
		concurrencySlotMember(execution.ID), admission.Limit,
		time.Now().UTC().Unix(), tag, int(s.concurrencySlotTTL().Seconds()),
	).Int64()
	if err != nil && err != redis.Nil {
		return engine.ConcurrencyAdmissionResult{}, fmt.Errorf("admit workflow %q: %w", workflow, err)
	}
	if admitted == 0 {
		return engine.ConcurrencyAdmissionResult{}, engine.ErrConcurrencyLimit
	}

	// The slot is charged. Every failure from here has to give it back, or the
	// workflow runs one short of its limit until the reconciler sweeps; that
	// release is best effort for the same reason the sweep exists.
	if err := s.createExecutionAdmitted(ctx, execution, entries, tag); err != nil {
		s.releaseWorkflowSlotBestEffort(ctx, execution.ID, t, tag)
		return engine.ConcurrencyAdmissionResult{}, err
	}
	return engine.ConcurrencyAdmissionResult{}, nil
}

// createExecutionAdmitted persists an execution admitted under the workflow
// concurrency limit and records the slot back-reference.
//
// The ordinary create path does the work — it owns the transient decision, the
// SQL projection, the outbox and the counters, and a second copy here would be
// a second definition of what creating an execution means. The back-reference
// lives with the execution, in the execution's own namespace, and is written
// with the same retention as its other keys, so it is present exactly as long
// as the execution it describes.
func (s *Store) createExecutionAdmitted(ctx context.Context, e *engine.ExecutionSnapshot, entries []engine.OutboxEntry, tag string) error {
	if err := s.createExecution(ctx, e, entries); err != nil {
		return err
	}
	t := namespace.FromContext(ctx)
	ttl := s.getExecTTL(ctx, e.ID)
	if ttl <= 0 {
		ttl = s.execTTL
	}
	if err := s.rdb.Set(ctx, concurrencyTagKey(t, e.ID), tag, ttl).Err(); err != nil {
		// The execution exists but cannot name its slot set. The member still
		// can: the reconciler reads the execution ID straight out of the slot
		// ZSET, so this is recoverable rather than fatal.
		return fmt.Errorf("record concurrency tag for %q: %w", e.ID, err)
	}
	return nil
}

// releaseWorkflowSlotBestEffort frees the slot a terminal execution held. It
// cannot return an error: the terminal transition is already durable, so
// failing the caller would make it retry a transition that succeeded.
//
// It does not stay silent either. A release that never lands lowers the
// workflow's effective limit by one until the sweep, and a silent failure is
// indistinguishable from a healthy store — the same argument
// shortenTransientCompletionTTLBestEffort makes.
func (s *Store) releaseWorkflowSlotBestEffort(ctx context.Context, id types.ExecutionID, t namespace.Namespace, tag string) {
	err := s.releaseWorkflowSlot(ctx, id, t, tag)
	if err != nil && s.logger != nil {
		s.logger.Error("workflow_slot_release_failed", "execution_id", string(id), "err", err)
	}
}

// releaseWorkflowSlot removes the execution's slot from its workflow's set.
//
// It is idempotent in both directions. An execution that never held a slot —
// every execution of an unlimited workflow — has no back-reference and
// releases nothing, and a slot already released is a ZREM miss that also
// releases nothing.
//
// The tag may be supplied by a caller that already knows it (admission
// rollback); otherwise it is read from the execution's durable back-reference,
// which is the only way a restarted process can release a slot for an execution
// it never admitted.
func (s *Store) releaseWorkflowSlot(ctx context.Context, id types.ExecutionID, t namespace.Namespace, tag string) error {
	if tag == "" {
		var err error
		tag, err = s.workflowTagFor(ctx, id, t)
		if err != nil || tag == "" {
			return err
		}
	}
	_, err := workflowReleaseLua.Run(ctx, s.rdb,
		[]string{concurrencySlotKeyForTag(t, tag), concurrencyWorkflowIndexKey(t)},
		concurrencySlotMember(id), tag,
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("release workflow slot for %q: %w", id, err)
	}
	return nil
}

// workflowTagFor answers which workflow an execution was admitted under.
func (s *Store) workflowTagFor(ctx context.Context, id types.ExecutionID, t namespace.Namespace) (string, error) {
	tag, err := s.rdb.Get(ctx, concurrencyTagKey(t, id)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read concurrency tag for %q: %w", id, err)
	}
	return tag, nil
}

// releaseWorkflowSlotAfterTerminal is the post-terminal hook every terminal
// writer calls. It takes the status the writer just persisted rather than
// re-reading it, so it can never race the next transition into thinking a
// canceling execution finished.
func (s *Store) releaseWorkflowSlotAfterTerminal(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	if !types.IsTerminalExecutionStatus(status) {
		return
	}
	s.releaseWorkflowSlotBestEffort(ctx, id, namespace.FromContext(ctx), "")
}
