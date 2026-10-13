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

// ErrWorkflowNameConflict is returned when a mutation would create a second
// live record with the same (namespace, name). Version is a dimension of one
// workflow's history, not a way to fork the workflow: two live records sharing
// a name share an activation key space, so an entry activation derived from
// one of them keeps being hosted beside the other (and a Kafka activation
// shares its consumer GroupID with its sibling), silently splitting the
// version the name refers to.
//
// It is distinct from ErrWorkflowConflict because the caller's fix is
// different: a name conflict is resolved by addressing the existing record
// (PUT its id) or deleting it, never by changing the definition.
var ErrWorkflowNameConflict = errors.New("another live workflow already holds this namespace and name")

// ErrWorkflowVersionImmutable is returned when a mutation would publish a
// version label with a definition hash different from the one that label was
// published with. Retiring a version (installing a different one, or rolling
// back) appends it to the workflow's history, and a history entry is immutable:
// it is the payload a pinned replay or a subworkflow reference resolves to.
// Re-publishing the same label with the same hash is not a conflict.
var ErrWorkflowVersionImmutable = errors.New("workflow version was already published with a different definition")

// ErrWorkflowVersionNotFound is returned by GetWorkflowVersion when the
// addressed workflow exists but holds no such version, current or archived. It
// is deliberately distinct from ErrWorkflowNotFound so a caller can tell "this
// workflow is gone" from "this workflow never published that version".
var ErrWorkflowVersionNotFound = errors.New("workflow version not found")

// WorkflowNameConflictError reports the live record that already owns a
// (namespace, name). The holder is carried for logs and callers that want to
// point at the surviving record; it is never echoed to an unauthenticated
// caller.
//
// It satisfies errors.Is(err, ErrWorkflowConflict) so every "this registration
// cannot succeed as presented" classifier (the SDK's
// IsRetryableRegistrationError, the registration-outcome metric, the
// legacy-hash reconciliation in workflowhash.ReconcileAdd) keeps answering the
// same way it does for a key conflict.
type WorkflowNameConflictError struct {
	Holder types.WorkflowID
}

func (e *WorkflowNameConflictError) Error() string {
	if e == nil || e.Holder == "" {
		return ErrWorkflowNameConflict.Error()
	}
	return fmt.Sprintf("%s: held by workflow %q", ErrWorkflowNameConflict, e.Holder)
}

func (e *WorkflowNameConflictError) Is(target error) bool {
	return target == ErrWorkflowNameConflict || target == ErrWorkflowConflict
}

func (e *WorkflowNameConflictError) Unwrap() error { return ErrWorkflowConflict }

// WorkflowVersionImmutableError reports the version label whose published
// definition hash the mutation would change. It satisfies errors.Is(err,
// ErrWorkflowVersionImmutable) and, for the same reason as
// WorkflowNameConflictError, errors.Is(err, ErrWorkflowConflict).
type WorkflowVersionImmutableError struct {
	Version string
}

func (e *WorkflowVersionImmutableError) Error() string {
	if e == nil || e.Version == "" {
		return ErrWorkflowVersionImmutable.Error()
	}
	return fmt.Sprintf("%s: version %q", ErrWorkflowVersionImmutable, e.Version)
}

func (e *WorkflowVersionImmutableError) Is(target error) bool {
	return target == ErrWorkflowVersionImmutable || target == ErrWorkflowConflict
}

func (e *WorkflowVersionImmutableError) Unwrap() error { return ErrWorkflowConflict }

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
	// EditorMetadata is the editor-only sibling of Definition (ADR-D4 §2.2,
	// §4): visual node positions, canvas viewport, per-node UI state, and
	// author notes. It participates in AuditFingerprint but never in
	// DefinitionHash, and has no effect on compiled Graph. Nil means no
	// editor metadata has ever been stored for this record.
	EditorMetadata *types.WorkflowEditorMetadata
	Graph          *graph.Graph
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

// DefaultWorkflowVersionRetention is how many retired versions a registry keeps
// per workflow when no retention limit is configured. Retention bounds the
// registry's growth (every retired version is a full record payload) and is
// otherwise unobservable: no execution reads history after it is submitted, so
// dropping the oldest entries only shortens how far back a rollback can reach.
const DefaultWorkflowVersionRetention = 50

// WorkflowVersionInfo is one row of a workflow's version list: the current
// version (Current) or a retired one (Archived). It carries no definition —
// a version list is a page of labels, and the definition is read one version
// at a time via GetWorkflowVersion.
//
// RegistryRevision is the revision the record carried while it was live: for
// the current row it is the live record's compare-and-swap token, for an
// archived row it is the revision it was retired at. It orders retired rows
// newest-first, which is also the order they were retired in.
type WorkflowVersionInfo struct {
	Version          string
	DefinitionHash   string
	RegistryRevision uint64
	Current          bool
	Archived         bool
}

// WorkflowVersionCapability is an optional capability implemented by registries
// that keep a workflow's retired versions as an immutable history keyed by
// workflow id. Version ownership follows the id (a rename preserves it), which
// is why the history is addressed by id and never by (namespace, name).
//
// A registry without this capability serves no version history; callers that
// need one must fail closed rather than treat "no such version" as "no
// history".
type WorkflowVersionCapability interface {
	// ListWorkflowVersions returns every version of the addressed workflow:
	// the current version first, then retired versions newest-retirement
	// first. It returns ErrWorkflowNotFound when the workflow itself does not
	// exist. Archived payloads are returned by GetWorkflowVersion, not here.
	ListWorkflowVersions(ctx context.Context, id types.WorkflowID) ([]WorkflowVersionInfo, error)

	// GetWorkflowVersion returns the addressed version, current or archived.
	// The current version resolves to the same record GetWorkflow returns. An
	// archived version resolves to the payload captured when it was retired,
	// which is why its Key/Name may name the workflow as it was called then
	// (a later rename does not rewrite history).
	//
	// ErrWorkflowNotFound means the workflow has no live record;
	// ErrWorkflowVersionNotFound means it has no such version.
	GetWorkflowVersion(ctx context.Context, id types.WorkflowID, version string) (WorkflowRecord, error)
}

// WorkflowListOptions bounds one enumeration page. It mirrors
// store.ListOptions (zero Limit means unbounded, non-positive Offset means
// zero) so a backend registry can be listed with the same vocabulary as the
// durable stores.
type WorkflowListOptions struct {
	// Limit is the maximum number of ids to return. Zero means unbounded —
	// implementations must return every match rather than none, which is not
	// the natural reading of a SQL LIMIT.
	Limit int
	// Offset is the number of leading ids to skip in the namespace's stable
	// enumeration order.
	Offset int
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

	// ListWorkflows returns the ids of the workflows registered in ns, newest
	// registry revision first, then by id ascending so the order is total and
	// stable across pages.
	//
	// ns is a required scope, not an optional filter: it is matched exactly, so
	// an empty or unknown namespace yields an empty result and never stands in
	// for "every namespace". There is deliberately no variant that enumerates
	// across namespaces — a registry-wide enumeration would be a cross-tenant
	// listing endpoint, and the only registry-wide index that exists is the
	// append-only projection-discovery set, which is not an authority for
	// "which workflows exist".
	//
	// The result contains ids only. Callers that need definitions read them
	// back with GetWorkflow, which keeps a page of ids O(page) rather than
	// O(page × definition size) on the wire.
	ListWorkflows(ctx context.Context, ns namespace.Namespace, opts WorkflowListOptions) ([]types.WorkflowID, error)
}
