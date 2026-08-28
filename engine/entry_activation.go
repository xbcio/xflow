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
// ReplicaIndex, NodeType, Params, PackageHash, Selector, Requirements, Supplies,
// SupplyConsumers, Desired, RegistryRevision) are owned by Upsert. The assignment
// fields (RunnerID, SessionID, Generation, LeaseDeadline, AssignedPackageHash)
// are owned by Assign/Fence/Renew and are guarded by monotonic generation
// fencing: an Assign only wins when its generation strictly exceeds the
// currently-stored generation, so a stale caller can never overwrite a newer
// owner.
type EntryActivation struct {
	Namespace       namespace.Namespace
	WorkflowID      types.WorkflowID
	WorkflowVersion string
	EntryUnitID     string
	// ReplicaIndex distinguishes sibling hosts of one logical trigger entry.
	// Replica zero is the legacy activation identity and keeps its historical
	// storage key. Siblings share workflow params (including Kafka GroupID).
	ReplicaIndex uint32
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
	// SupplyConsumers pair each wasm module in this entry unit with the supply
	// nodes it consumes. Supplies answers "must this runner have the content
	// before taking over"; SupplyConsumers answers "once it has the content, who
	// receives it". The runner needs both because neither the Supplies list nor a
	// projected group package retains which member consumes which supply — the
	// pairing exists only in the graph's dependency edges, so it is derived here.
	//
	// Desired-state, owned by Upsert. Absent on records written before this field
	// existed (decodes to nil), which leaves the runner registering no consumers —
	// exactly the behaviour that preceded this field.
	SupplyConsumers []SupplyConsumerBinding
	Desired         bool
	// RegistryRevision fences desired-state projection writes. A non-zero value
	// is the authoritative workflow registry revision that produced this record.
	// Upsert ignores a lower revision, and revision zero is the legacy mode: it
	// cannot overwrite a record or workflow watermark that is already non-zero.
	// Equal revisions may be written again to support idempotent projection.
	RegistryRevision uint64
	RunnerID         string
	SessionID        string
	Generation       uint64
	LeaseDeadline    time.Time
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
	ReplicaIndex    uint32
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

// EntryActivationRevisionStore is the optional workflow-wide desired-state
// revision fence. A manager advances the watermark before listing or upserting
// any activation for that workflow. Implementations must advance monotonically
// and must make Upsert reject a revision below the watermark even when the
// activation key does not exist. Revision zero is legacy-compatible only while
// the watermark is zero.
//
// Keeping this capability separate preserves compatibility with existing
// EntryActivationStore implementations. Callers that project a non-zero
// registry revision must fail closed when the capability is unavailable.
type EntryActivationRevisionStore interface {
	AdvanceWorkflowRevision(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, revision uint64) error
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

// SupplyConsumerBinding tells a runner that one wasm script node consumes one
// supply node's content. It has two shapes, and the runner branches on
// IsDeclaration:
//
//	DECLARATION (WorkflowName + NodeName set, ModuleDigest empty) -- the shape
//	  every current control plane emits. It names the CONSUMER, not the module,
//	  because artifact_digest may be an expression that only boundary evaluation
//	  can resolve (spec §4.2). The runner records it and registers the real
//	  {digest, supplyNode} consumer at execution time.
//
//	LEGACY (ModuleDigest set, names empty) -- what a pre-declaration control
//	  plane wrote. The digest was a literal, so the runner compiles and registers
//	  at activation time exactly as before. Kept so an upgraded runner keeps
//	  working against a control plane that has not been upgraded yet.
//
// Every field is a string so the struct stays comparable: it is used as a map
// key in both SupplyConsumerBindingsForEntryUnit's dedup set and the runner's
// removedBindings diff.
//
// The registration key is deliberately NOT derived from these names. It stays
// {digest, supplyNode} in node/internal/code/script/wasm/supply_consumer.go --
// see spec §4.2.1 candidate 2 for what keying by node identity does to a
// rollback (the old engine's consumer gets overwritten, and because the
// source-driven marker is never cleared it keeps serving frozen rules with no
// diagnostic at all).
type SupplyConsumerBinding struct {
	// ModuleDigest is the artifact digest ("sha256:<64 hex>") on legacy records
	// only -- never the module bytes, which run to several MB. Empty on a
	// declaration.
	ModuleDigest string `json:"module_digest,omitempty"`
	// SupplyNode is the supply resource whose content this consumer reads.
	SupplyNode string `json:"supply_node"`
	// WorkflowName is the graph name the consuming node runs under AT RUNTIME.
	// For a map body member that is the map node's name, not the workflow's
	// (types.Input.WorkflowName's contract); for a grouped node it is the group
	// name (graph.SubgraphPackage sets Def.Name = GroupMeta.Name).
	WorkflowName string `json:"workflow_name,omitempty"`
	// NodeName is the consuming node's name within that graph.
	NodeName string `json:"node_name,omitempty"`
	// DigestExpr is the node's artifact_digest parameter VERBATIM, before
	// evaluation. The warm-up consumer renders it against $supplies alone so it
	// can pre-compile the new module the moment the pointer changes. It is a
	// template over supply content, never a credential -- but it is still node
	// configuration, so it must not be echoed in an error.
	DigestExpr string `json:"digest_expr,omitempty"`
}

// IsDeclaration reports whether this binding names its consumer rather than its
// module. Both names are required: the runner's declaration table is keyed by
// the pair, so a half-filled key would collide with an unrelated node under the
// empty string.
func (b SupplyConsumerBinding) IsDeclaration() bool {
	return b.WorkflowName != "" && b.NodeName != ""
}
