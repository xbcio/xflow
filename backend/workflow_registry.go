package backend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

var ErrWorkflowConflict = errors.New("workflow key already exists with different definition hash")
var ErrWorkflowNotFound = errors.New("workflow not found")
var ErrWorkflowReplaceUnsupported = errors.New("workflow registry does not support atomic replacement")
var ErrWorkflowMutationIndeterminate = errors.New("workflow mutation outcome is indeterminate")

// WorkflowRecord is the authoritative persisted representation of a compiled
// workflow. RegistryRevision is assigned by the registry and increases on every
// successful mutation of the record. Callers must treat it as opaque except for
// equality and ordering; in particular, DefinitionHash alone is not an adequate
// compare-and-swap token because a definition can change away and back (ABA).
type WorkflowRecord struct {
	ID               types.WorkflowID
	Key              string
	Namespace        string
	Name             string
	Version          string
	DefinitionHash   string
	RegistryRevision uint64
	AuditFingerprint string
	Definition       *types.WorkflowDef
	Graph            *graph.Graph
}

// WorkflowRevision is the immutable identity used by atomic registry
// mutations. Version is included so callers can retire the previous activation
// projection after a successful rename or version change without retaining the
// full previous graph.
type WorkflowRevision struct {
	ID               types.WorkflowID
	Key              string
	Version          string
	DefinitionHash   string
	RegistryRevision uint64
}

// RevisionOfWorkflow returns the compare-and-swap identity of rec.
func RevisionOfWorkflow(rec WorkflowRecord) WorkflowRevision {
	return WorkflowRevision{
		ID:               rec.ID,
		Key:              rec.Key,
		Version:          rec.Version,
		DefinitionHash:   rec.DefinitionHash,
		RegistryRevision: rec.RegistryRevision,
	}
}

// WorkflowReplaceRequest describes one idempotent atomic replacement.
// MutationID is required, is scoped to the request namespace, and must be
// reused when an indeterminate storage error is retried. Reusing it for a
// different replacement is a conflict.
type WorkflowReplaceRequest struct {
	MutationID  string
	Expected    WorkflowRevision
	Replacement WorkflowRecord
}

type WorkflowReplaceStatus string

const (
	WorkflowReplaceUnchanged WorkflowReplaceStatus = "unchanged"
	WorkflowReplaceReplaced  WorkflowReplaceStatus = "replaced"
)

// WorkflowReplaceResult is returned for a committed mutation, including an
// idempotent replay. Unchanged preserves the existing record and revision;
// Replaced returns the registry-assigned revision of Current.
type WorkflowReplaceResult struct {
	Status   WorkflowReplaceStatus
	Previous WorkflowRevision
	Current  WorkflowRecord
}

// WorkflowActivationProjectionIntent is the durable instruction to reconcile
// entry-activation desired state after an authoritative registry replacement.
// Previous carries the identity that may need retiring; Current carries the
// complete committed record and its registry revision.
type WorkflowActivationProjectionIntent struct {
	Namespace  namespace.Namespace
	MutationID string
	Previous   WorkflowRevision
	Current    WorkflowRecord
}

// WorkflowActivationProjectionRef identifies one pending intent within the
// namespace supplied to WorkflowActivationProjectionOutbox methods.
type WorkflowActivationProjectionRef struct {
	MutationID string
}

// WorkflowActivationProjectionClaimState describes the result of Claim.
type WorkflowActivationProjectionClaimState string

const (
	// WorkflowActivationProjectionClaimAcquired means the caller owns the
	// returned intent until LeaseDeadline.
	WorkflowActivationProjectionClaimAcquired WorkflowActivationProjectionClaimState = "acquired"
	// WorkflowActivationProjectionClaimBusy means another unexpired token owns
	// the pending intent.
	WorkflowActivationProjectionClaimBusy WorkflowActivationProjectionClaimState = "busy"
	// WorkflowActivationProjectionClaimApplied means the intent was already
	// acknowledged. This makes a retry after a lost Ack response conclusive.
	WorkflowActivationProjectionClaimApplied WorkflowActivationProjectionClaimState = "applied"

	// Short aliases keep claim-state switches readable at consumption sites.
	WorkflowProjectionClaimAcquired = WorkflowActivationProjectionClaimAcquired
	WorkflowProjectionClaimBusy     = WorkflowActivationProjectionClaimBusy
	WorkflowProjectionClaimApplied  = WorkflowActivationProjectionClaimApplied
)

// WorkflowActivationProjectionClaim is the token-fenced result of Claim.
// Intent is populated for Acquired and Applied results. Busy results only need
// State and LeaseDeadline.
type WorkflowActivationProjectionClaim struct {
	State         WorkflowActivationProjectionClaimState
	Intent        WorkflowActivationProjectionIntent
	LeaseDeadline time.Time
}

type WorkflowReplaceConflictKind string

const (
	WorkflowReplaceConflictStaleRevision   WorkflowReplaceConflictKind = "stale_revision"
	WorkflowReplaceConflictDestinationKey  WorkflowReplaceConflictKind = "destination_key_occupied"
	WorkflowReplaceConflictDestinationID   WorkflowReplaceConflictKind = "destination_id_occupied"
	WorkflowReplaceConflictMutationIDReuse WorkflowReplaceConflictKind = "mutation_id_reused"
)

// WorkflowReplaceConflictError reports why an atomic replacement made no
// changes. It unwraps to ErrWorkflowConflict for compatibility with existing
// HTTP and SDK error handling.
type WorkflowReplaceConflictError struct {
	Kind WorkflowReplaceConflictKind
}

func (e *WorkflowReplaceConflictError) Error() string {
	if e == nil || e.Kind == "" {
		return ErrWorkflowConflict.Error()
	}
	return fmt.Sprintf("%s: %s", ErrWorkflowConflict, e.Kind)
}

func (e *WorkflowReplaceConflictError) Unwrap() error { return ErrWorkflowConflict }

// WorkflowMutationIndeterminateError means the registry could not prove
// whether a mutation committed (for example, the Redis connection dropped
// after EVAL). Retrying the same WorkflowReplaceRequest is the only safe
// recovery; callers must not compensate with RemoveWorkflow or AddWorkflow.
type WorkflowMutationIndeterminateError struct {
	Err error
}

func (e *WorkflowMutationIndeterminateError) Error() string {
	if e == nil || e.Err == nil {
		return ErrWorkflowMutationIndeterminate.Error()
	}
	return fmt.Sprintf("%s: %v", ErrWorkflowMutationIndeterminate, e.Err)
}

func (e *WorkflowMutationIndeterminateError) Unwrap() error {
	if e == nil || e.Err == nil {
		return ErrWorkflowMutationIndeterminate
	}
	return e.Err
}

func (e *WorkflowMutationIndeterminateError) Is(target error) bool {
	return target == ErrWorkflowMutationIndeterminate
}

// WorkflowReplaceCapability is an optional capability implemented by
// registries that can replace a record and all of its key/ID indexes in one
// atomic compare-and-swap. Replace callers must fail closed when it is absent;
// RemoveWorkflow+AddWorkflow is not a safe fallback.
type WorkflowReplaceCapability interface {
	CompareAndReplaceWorkflow(context.Context, WorkflowReplaceRequest) (WorkflowReplaceResult, error)
}

// WorkflowActivationProjectionOutbox is the durable delivery capability for
// workflow activation projections. Delivery is at least once: ListPending may
// repeat refs, an expired claim may be reclaimed, and callers must project
// idempotently using Current.RegistryRevision.
type WorkflowActivationProjectionOutbox interface {
	// ListWorkflowActivationProjectionNamespaces returns every namespace that
	// may contain a pending intent. Implementations must retain membership for
	// the registry lifetime so an unacknowledged intent stays discoverable.
	ListWorkflowActivationProjectionNamespaces(context.Context) ([]namespace.Namespace, error)

	// ListPendingWorkflowActivationProjections returns at most limit pending
	// refs after opaque cursor. A zero cursor starts a pass; nextCursor zero
	// means the pass reached its current tail. Results may repeat across passes,
	// but an unacknowledged intent must eventually be returned by passes that
	// restart at zero.
	ListPendingWorkflowActivationProjections(context.Context, namespace.Namespace, uint64, int) ([]WorkflowActivationProjectionRef, uint64, error)

	// ClaimWorkflowActivationProjection acquires one pending intent for
	// leaseToken and leaseTTL. Retrying an unexpired claim with the same
	// non-empty token is idempotent and does not extend its deadline. A
	// different unexpired token returns Busy; an expired lease may be replaced.
	// An acknowledged intent returns Applied.
	ClaimWorkflowActivationProjection(context.Context, namespace.Namespace, string, string, time.Duration) (WorkflowActivationProjectionClaim, error)

	// AckWorkflowActivationProjection marks the intent applied and removes it
	// from pending only when leaseToken owns an unexpired claim. It returns true
	// when it performs the transition or the intent was already applied, and
	// false for a stale token.
	AckWorkflowActivationProjection(context.Context, namespace.Namespace, string, string) (bool, error)
}

// DurableWorkflowReplaceCapability guarantees that each successful first
// application of CompareAndReplaceWorkflow atomically records exactly one
// pending activation projection intent. Conflicts create no intent, idempotent
// mutation replay creates no duplicate, and replay after Ack does not make the
// intent pending again.
type DurableWorkflowReplaceCapability interface {
	WorkflowReplaceCapability
	WorkflowActivationProjectionOutbox
}

type WorkflowRegistry interface {
	AddWorkflow(ctx context.Context, rec WorkflowRecord) (WorkflowRecord, error)
	GetWorkflow(ctx context.Context, id types.WorkflowID) (WorkflowRecord, error)
	// GetWorkflowByKey returns the record currently registered under key, or
	// ErrWorkflowNotFound if no record exists. It is used by Engine.AddWorkflow
	// to inspect a conflicting existing record for legacy-hash reconciliation.
	GetWorkflowByKey(ctx context.Context, key string) (WorkflowRecord, error)
	// UpdateDefinitionHash atomically updates the DefinitionHash of the record
	// with id, but only if the currently-stored hash equals expectedOldHash.
	// Returns ErrWorkflowNotFound if the id is unknown and
	// ErrWorkflowConflict if the stored hash no longer matches expectedOldHash
	// (e.g. another registrar upgraded it concurrently). It is used by
	// Engine.AddWorkflow to upgrade legacy-format hashes when semantics match.
	UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error
	RemoveWorkflow(ctx context.Context, id types.WorkflowID) error
}
