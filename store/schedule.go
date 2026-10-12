package store

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ScheduleOverride is the runtime overlay an operator has placed on one trigger
// node: what was changed about a schedule WITHOUT changing the workflow
// definition, and therefore without registering a new version.
//
// This is the design's answer to "schedules need to change faster than
// definitions do". A cron expression lives in a workflow definition, and a
// definition is versioned, hashed, exported and reviewed; retiming a schedule
// from "0 2 * * *" to "0 3 * * *" through the definition would create a new
// version and re-register every one of the workflow's entry units for a change
// that touches exactly one cron node. The overlay is the small, reversible,
// audited surface for that, and it is deliberately NOT a schedule entity: a
// schedule still has no existence apart from the trigger node that declares it.
//
// Every overridable field is a POINTER, and nil means "not overridden" rather
// than "empty". The difference is load-bearing: nil follows the definition when
// the definition changes, while a pointer to the definition's current value
// pins it. A store that flattened the two could not answer DELETE
// .../override, whose whole job is to return one field to following the
// definition.
type ScheduleOverride struct {
	Namespace  namespace.Namespace
	WorkflowID types.WorkflowID
	// Node is the trigger node's name — the same identity the workflow
	// definition and the entry unit use. It is not a schedule id: nothing
	// addresses an override except (namespace, workflow, node).
	Node string

	// Expression is the cron expression, overriding the node's own
	// `expression` parameter.
	Expression *string
	// Timezone is an IANA zone name, overriding the node's `timezone`.
	Timezone *string
	// Misfire overrides the node's `misfire` policy.
	Misfire *string
	// Overlap overrides the node's `overlap` policy.
	Overlap *string
	// Paused suspends the schedule. The watermark keeps advancing while paused,
	// because resuming is not a catch-up: what was skipped is skipped, unless an
	// operator asks for it explicitly (backfill).
	Paused *bool

	// Revision is monotonic and bumped on EVERY write, even one that changes
	// nothing, mirroring SupplyResource: it is the write-side CAS token
	// (If-Match), not a content version.
	Revision  uint64
	UpdatedAt time.Time
	// UpdatedBy is the principal that made the change, for the audit trail the
	// API also writes. It is not the authorization: what a caller may change is
	// decided from its scope, never from this field.
	UpdatedBy string
}

// ScheduleOverrideStore persists ScheduleOverride rows, one per
// (namespace, workflow, node).
//
// It is deliberately NOT part of store.Store. Every backend that can run a
// workflow can run a schedule, but only a deployment that serves the schedules
// API needs to persist overlays — and widening store.Store would force every
// implementation and test double to grow five methods for a control-plane
// feature they may never see. A deployment without the capability answers the
// schedules API with a refusal rather than failing to compile, exactly like the
// other optional store capabilities (AuditReconciler, ReceiptAuditAppender).
type ScheduleOverrideStore interface {
	// GetScheduleOverride reads one overlay. Returns ErrNotFound when absent,
	// and ErrInvalidNamespace when ns is not a valid scope.
	GetScheduleOverride(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) (*ScheduleOverride, error)

	// ListScheduleOverrides returns every overlay of one workflow, ordered by
	// node name so a listing is stable across calls and across backends. A
	// workflow with no overlays returns an empty slice, not an error: the
	// question "which of my schedules are overridden" is answered by an empty
	// answer, not by a missing row.
	ListScheduleOverrides(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) ([]*ScheduleOverride, error)

	// PutScheduleOverride writes one overlay and returns the stored result.
	//
	// ifMatch is the optimistic-concurrency token, with the same three cases
	// PutSupply defines:
	//   - nil           → unconditional write
	//   - *ifMatch == 0 → create only if absent
	//   - *ifMatch == N → write only if the current Revision is exactly N
	//
	// A mismatch returns ErrRevisionConflict and leaves the stored row
	// untouched. On success Revision is bumped by one and the caller's rec is
	// not mutated.
	PutScheduleOverride(ctx context.Context, rec *ScheduleOverride, ifMatch *uint64) (*ScheduleOverride, error)

	// DeleteScheduleOverride removes one overlay, after which the schedule
	// follows its definition again. Deleting an absent overlay is not an error:
	// the caller asked for a state, and the state holds. ifMatch applies as on
	// write, and a mismatch returns ErrRevisionConflict.
	DeleteScheduleOverride(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string, ifMatch *uint64) error

	// DeleteScheduleOverridesForWorkflow removes every overlay of one workflow
	// and reports how many rows it removed. It exists for the cascade: an
	// overlay has no meaning without the node it overlays, and a node has no
	// meaning without its workflow, so deleting a workflow must not leave rows
	// behind that a later re-registration of the same id would silently inherit.
	DeleteScheduleOverridesForWorkflow(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID) (int, error)
}
