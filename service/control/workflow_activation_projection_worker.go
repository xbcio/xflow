package control

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

const (
	// DefaultWorkflowActivationProjectionPeriod bounds recovery latency after a
	// process dies between registry commit and activation projection.
	DefaultWorkflowActivationProjectionPeriod = time.Second
	// DefaultWorkflowActivationProjectionBatch bounds work per namespace and
	// sweep so projection recovery cannot monopolize the control-plane leader.
	DefaultWorkflowActivationProjectionBatch = 128
	// DefaultWorkflowActivationProjectionTimeout bounds one projection attempt.
	DefaultWorkflowActivationProjectionTimeout = 10 * time.Second
	// DefaultWorkflowActivationProjectionLease is deliberately longer than the
	// projection timeout; a timed-out owner cannot race a new claim before its
	// store calls have returned.
	DefaultWorkflowActivationProjectionLease = 30 * time.Second
)

// WorkflowActivationProjectionWorkerConfig configures durable activation
// projection recovery.
type WorkflowActivationProjectionWorkerConfig struct {
	Outbox            backend.WorkflowActivationProjectionOutbox
	Projector         *WorkflowActivationProjector
	Leader            LeaderGate
	Logger            engine.Logger
	Period            time.Duration
	Batch             int
	ProjectionTimeout time.Duration
	ClaimTTL          time.Duration
	Token             func() string
}

// WorkflowActivationProjectionWorker drains workflow-registry projection
// intents using token-fenced leases. Delivery is at least once; revision
// fencing makes duplicates and out-of-order claims safe.
type WorkflowActivationProjectionWorker struct {
	outbox            backend.WorkflowActivationProjectionOutbox
	projector         *WorkflowActivationProjector
	leader            LeaderGate
	log               engine.Logger
	period            time.Duration
	batch             int
	projectionTimeout time.Duration
	claimTTL          time.Duration
	token             func() string
	cursors           map[namespace.Namespace]uint64
}

// NewWorkflowActivationProjectionWorker constructs a recovery worker.
func NewWorkflowActivationProjectionWorker(cfg WorkflowActivationProjectionWorkerConfig) *WorkflowActivationProjectionWorker {
	period := cfg.Period
	if period <= 0 {
		period = DefaultWorkflowActivationProjectionPeriod
	}
	batch := cfg.Batch
	if batch <= 0 {
		batch = DefaultWorkflowActivationProjectionBatch
	}
	projectionTimeout := cfg.ProjectionTimeout
	if projectionTimeout <= 0 {
		projectionTimeout = DefaultWorkflowActivationProjectionTimeout
	}
	claimTTL := cfg.ClaimTTL
	if claimTTL <= projectionTimeout {
		claimTTL = DefaultWorkflowActivationProjectionLease
		if claimTTL <= projectionTimeout {
			claimTTL = projectionTimeout + time.Second
		}
	}
	token := cfg.Token
	if token == nil {
		token = uuid.NewString
	}
	return &WorkflowActivationProjectionWorker{
		outbox:            cfg.Outbox,
		projector:         cfg.Projector,
		leader:            cfg.Leader,
		log:               cfg.Logger,
		period:            period,
		batch:             batch,
		projectionTimeout: projectionTimeout,
		claimTTL:          claimTTL,
		token:             token,
		cursors:           make(map[namespace.Namespace]uint64),
	}
}

// Run performs recovery immediately at startup, then periodically until ctx is
// canceled. ReconcileOnce performs the leadership check on every pass.
func (w *WorkflowActivationProjectionWorker) Run(ctx context.Context) {
	if w == nil || w.outbox == nil {
		return
	}
	w.ReconcileOnce(ctx)
	ticker := time.NewTicker(w.period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.ReconcileOnce(ctx)
		}
	}
}

// ReconcileOnce drains at most one bounded page per known namespace. It
// returns the number of intents successfully acknowledged in this pass.
func (w *WorkflowActivationProjectionWorker) ReconcileOnce(ctx context.Context) int {
	if w == nil || w.outbox == nil || ctx.Err() != nil {
		return 0
	}
	if w.leader != nil && !w.leader.IsLeader() {
		return 0
	}
	namespaces, err := w.outbox.ListWorkflowActivationProjectionNamespaces(ctx)
	if err != nil {
		w.error("workflow activation projection: list namespaces", err)
		return 0
	}
	applied := 0
	for _, ns := range namespaces {
		if ctx.Err() != nil {
			break
		}
		cursor := w.cursors[ns]
		refs, next, listErr := w.outbox.ListPendingWorkflowActivationProjections(ctx, ns, cursor, w.batch)
		if listErr != nil {
			w.error("workflow activation projection: list pending", listErr, "namespace", string(ns))
			continue
		}
		w.cursors[ns] = next
		for _, ref := range refs {
			if ctx.Err() != nil {
				break
			}
			projected, projectErr := w.ProjectPending(ctx, ns, ref.MutationID, 1)
			if projectErr != nil {
				w.error("workflow activation projection: apply", projectErr, "namespace", string(ns), "mutation_id", ref.MutationID)
				continue
			}
			if projected {
				applied++
			}
		}
	}
	return applied
}

// ProjectPending claims, projects, and acknowledges one mutation. applied is
// false with a nil error only when another live lease owns the intent. Attempts
// retries projection under the same token-fenced claim; storage errors and an
// exhausted projection error leave the intent pending for later recovery.
func (w *WorkflowActivationProjectionWorker) ProjectPending(ctx context.Context, ns namespace.Namespace, mutationID string, attempts int) (applied bool, err error) {
	if w == nil || w.outbox == nil {
		return false, fmt.Errorf("workflow activation projection outbox is not configured")
	}
	if attempts <= 0 {
		attempts = 1
	}
	token := w.token()
	if token == "" {
		return false, fmt.Errorf("generate workflow activation projection claim token: empty token")
	}
	claim, err := w.outbox.ClaimWorkflowActivationProjection(ctx, ns, mutationID, token, w.claimTTL)
	if err != nil {
		return false, fmt.Errorf("claim workflow activation projection: %w", err)
	}
	switch claim.State {
	case backend.WorkflowProjectionClaimBusy:
		return false, nil
	case backend.WorkflowProjectionClaimApplied:
		acked, ackErr := w.outbox.AckWorkflowActivationProjection(ctx, ns, mutationID, token)
		if ackErr != nil {
			return false, fmt.Errorf("acknowledge applied workflow activation projection: %w", ackErr)
		}
		if !acked {
			return false, fmt.Errorf("acknowledge applied workflow activation projection: not acknowledged")
		}
		return true, nil
	case backend.WorkflowProjectionClaimAcquired:
		// Continue below.
	default:
		return false, fmt.Errorf("claim workflow activation projection: unexpected state %q", claim.State)
	}

	var projectionErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		projectionCtx, cancel := context.WithTimeout(namespace.WithNamespace(ctx, ns), w.projectionTimeout)
		projectionErr = w.projector.Project(projectionCtx, claim.Intent)
		cancel()
		if projectionErr == nil {
			break
		}
	}
	if projectionErr != nil {
		return false, fmt.Errorf("project workflow activation: %w", projectionErr)
	}
	acked, err := w.outbox.AckWorkflowActivationProjection(ctx, ns, mutationID, token)
	if err != nil {
		return false, fmt.Errorf("acknowledge workflow activation projection: %w", err)
	}
	if !acked {
		return false, fmt.Errorf("acknowledge workflow activation projection: claim token no longer owns intent")
	}
	return true, nil
}

func (w *WorkflowActivationProjectionWorker) error(message string, err error, args ...any) {
	if w == nil || w.log == nil {
		return
	}
	args = append(args, "err", err)
	w.log.Error(message, args...)
}
