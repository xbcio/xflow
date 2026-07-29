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
// Upsert. The assignment fields (RunnerID, SessionID, Generation, LeaseDeadline)
// are owned by Assign/Fence/Renew and are guarded by monotonic generation
// fencing: an Assign only wins when its generation strictly exceeds the
// currently-stored generation, so a stale caller can never overwrite a newer
// owner.
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
	Params      map[string]any
	PackageHash string
	Selector    *types.RunnerSelector
	// Requirements are the capability requirements the hosting runner must
	// satisfy to drive this entry unit (derived from the entry unit's node
	// type(s)). The reconciler assigns only a runner whose advertised
	// capabilities cover these. It is desired-state, owned by Upsert. Absent on
	// records written before this field existed (decodes to nil).
	Requirements  []CapabilityRequirement
	Desired       bool
	RunnerID      string
	SessionID     string
	Generation    uint64
	LeaseDeadline time.Time
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
	// otherwise it returns false and leaves the existing owner untouched.
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
