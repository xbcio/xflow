package engine

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// EntryActivation is the durable desired-state record that lets a remote runner
// host a trigger entry unit — a single node OR a group node, which is externally
// one node. The control-plane leader reconciles these records: for each desired
// activation it assigns a capable runner session and fences stale generations so
// exactly one runner drives the entry unit at a time.
//
// The desired-state fields (Namespace, WorkflowID, WorkflowVersion, EntryUnitID,
// NodeType, Params, PackageHash, Selector, Requirements, Desired) are owned by
// Upsert. The assignment fields (RunnerID, SessionID, Generation, LeaseDeadline,
// AssignedPackageHash) are owned by Assign/Fence/Renew and are guarded by
// monotonic generation fencing: an Assign only wins when its generation strictly
// exceeds the currently-stored generation, so a stale caller can never overwrite
// a newer owner.
type EntryActivation struct {
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	// NodeType is the trigger node type the hosting runner must construct (e.g.
	// "kafka.source"). For a group entry unit it is the reserved group node type.
	// It is desired-state, owned by Upsert. Absent on records written before this
	// field existed (decodes to "").
	NodeType string
	// Params are the trigger parameters the hosting runner uses to construct the
	// trigger — the SAME parameters the WorkflowDef already holds (no new secret
	// surface). Desired-state, owned by Upsert. Absent on older records (decodes
	// to nil).
	Params map[string]any
	// PackageHash is the desired content fingerprint of the entry unit — for a
	// group it is the projected group package hash; for a single trigger node it
	// fingerprints (NodeType, Version, Params). A within-version change to the
	// trigger's params changes this hash, which is how the reconciler detects a
	// material content change to an already-assigned activation. Desired-state,
	// owned by Upsert.
	PackageHash string
	Selector    *types.RunnerSelector
	// Requirements are the capability requirements the hosting runner must
	// satisfy to drive this entry unit (derived from the entry unit's node
	// type(s)). The reconciler assigns only a runner whose advertised
	// capabilities cover these. It is desired-state, owned by Upsert. Absent on
	// records written before this field existed (decodes to nil).
	Requirements []CapabilityRequirement
	// Supplies are the supply contents this entry unit's nodes depend on, derived
	// from the graph's dependency edges. The hosting runner uses them as its
	// activation-time readiness gate: it fetches each one before taking over, and
	// with RequireReady it declines the activation rather than serving traffic
	// with no content.
	//
	// This is a DIFFERENT dimension from Requirements: Requirements asks "can this
	// runner run this node type" and is matched by the reconciler when choosing a
	// runner; Supplies asks "does this runner have the content yet" and is decided
	// by the runner itself. Merging them would put a dynamic, per-runner condition
	// into static placement matching.
	//
	// Desired-state, owned by Upsert. Absent on records written before this field
	// existed (decodes to nil).
	Supplies []SupplyRequirement
	Desired  bool
	RunnerID      string
	SessionID     string
	Generation    uint64
	LeaseDeadline time.Time
	// AssignedPackageHash is the PackageHash that was in effect when the current
	// owner was assigned (snapshotted by Assign). The reconciler compares it
	// against the desired PackageHash to detect a within-version content change:
	// when they differ, the current owner is running stale params and must be
	// fenced + reassigned so the new params reach a runner at a new generation. It
	// is an assignment field (owned by Assign/Fence). Absent on records written
	// before this field existed (decodes to "").
	AssignedPackageHash string
}

// EntryActivationKey uniquely identifies an entry activation record.
type EntryActivationKey struct {
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	EntryUnitID     string
}

// EntryActivationStore is the durable desired-state store for entry activations.
// Both a single-process memory implementation and a Redis implementation must
// satisfy it identically (see the shared contract test).
type EntryActivationStore interface {
	// Upsert creates or updates the desired-state fields of an activation. It
	// never touches the assignment fields (RunnerID/SessionID/Generation/
	// LeaseDeadline) of an existing record — those are owned by Assign/Fence.
	Upsert(ctx context.Context, act EntryActivation) error

	// Get returns the activation for key. The bool is false when absent.
	Get(ctx context.Context, key EntryActivationKey) (EntryActivation, bool, error)

	// List returns all activations in the namespace.
	List(ctx context.Context, ns namespace.Namespace) ([]EntryActivation, error)

	// Assign atomically claims the activation for runnerID/sessionID at the given
	// generation. It is first-writer-wins and monotonic: the claim succeeds
	// (returns true) only when gen strictly exceeds the stored generation;
	// otherwise it returns false and leaves the existing owner untouched. On a
	// successful claim it snapshots the record's current desired PackageHash into
	// AssignedPackageHash so the reconciler can later detect a within-version
	// content change (desired PackageHash drifting from the assigned one).
	Assign(ctx context.Context, key EntryActivationKey, runnerID, sessionID string, gen uint64, deadline time.Time) (bool, error)

	// Renew extends the lease deadline of the current owner WITHOUT advancing the
	// generation. It is generation-gated: the renewal succeeds (returns true) only
	// when gen EQUALS the stored generation, so a stale caller can never extend a
	// lease it no longer owns. It is a no-op (returns false) when the activation
	// does not exist or the generation does not match.
	Renew(ctx context.Context, key EntryActivationKey, gen uint64, deadline time.Time) (bool, error)

	// Fence invalidates the current owner and raises the generation floor to at
	// least gen, so any subsequent Assign must use a strictly higher generation.
	// It is a no-op when the activation does not exist.
	Fence(ctx context.Context, key EntryActivationKey, gen uint64) error
}

// SupplyRequirement is one supply content dependency of an entry unit.
//
// Node is the supply NODE's name in the graph (the identity dependency edges and
// $supplies expressions use). Resource is the SupplyResource name to fetch — it
// defaults to Node but may differ, so two workflows can consume one shared
// resource under their own local node names.
type SupplyRequirement struct {
	Node     string `json:"node"`
	Resource string `json:"resource"`
	// RequireReady false means the runner takes the activation even with no
	// content and the consumer runs with empty semantics. True (the default in
	// the DSL) means it declines instead — traffic stays in Kafka, the offset does
	// not advance, and consumer-group lag is the operator-visible signal.
	RequireReady bool `json:"require_ready"`
}
