package protocol

import (
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
)

// --- Path constants ---
//
// Activation is NOT driven over its own HTTP routes. The reconciler attaches
// ActivateDirective/DeactivateDirective to the heartbeat RESPONSE, and the
// runner acts on them locally through TriggerActivationHandler. The only
// activation route that exists is the ack the runner posts back.
//
// There were once ActivatePath, DeactivatePath and ActivationListPath
// constants here. Nothing ever registered a handler for them or called them —
// they described a protocol that was never built, and anyone coding against
// them got a 404. Do not reintroduce a path constant before its route.
const (
	ActivationAckPath = "/v1/runners/activation/ack"
)

// --- Activate directive (server → runner) ---

// ActivateDirective is sent by the activation reconciler to a runner,
// instructing it to start hosting a trigger entry unit (a single trigger node
// OR a group node, which is externally one node). It is node-generic: the entry
// unit is identified by EntryUnitID (formerly GroupID) and carries the trigger's
// NodeType and Params so the runner can construct the trigger without a separate
// fetch. Params carry the SAME trigger parameters the WorkflowDef already holds —
// no new secret surface.
type ActivateDirective struct {
	Namespace       string         `json:"namespace"`
	WorkflowID      string         `json:"workflow_id"`
	WorkflowVersion string         `json:"workflow_version"`
	EntryUnitID     string         `json:"entry_unit_id"` // was GroupID
	NodeType        string         `json:"node_type"`     // trigger node type, e.g. "kafka.source"
	Params          map[string]any `json:"params,omitempty"`
	Generation      uint64         `json:"generation"`
	PackageHash     string         `json:"package_hash,omitempty"`
	// Package is the projected group SubgraphPackage, present only when
	// NodeType == engine.GroupNodeType. It is re-projected and attached fresh
	// on every Activate (never cached server-side against a runner-reported
	// hash — see spec 2026-08-07 §3.3 for why that "optimization" is not
	// worth the consistency burden). A runner that does not recognize this
	// field (old binary) silently ignores it and falls through to the
	// pre-existing fail-closed behavior for "xflow.group" — no behavior
	// regression, because that path never worked before this feature.
	Package *graph.SubgraphPackage `json:"package,omitempty"`
	// Kind selects the runner-side handler: "" or "trigger" means the trigger
	// handler (so an old runner that never sees this field behaves as before),
	// "supply" means the supply handler. Dispatching on an explicit kind rather
	// than sniffing a NodeType prefix keeps a third source mode from silently
	// landing on the wrong handler.
	Kind string `json:"kind,omitempty"`
	// Supplies are the supply contents the receiving runner must have before it
	// takes over this entry unit (see engine.SupplyRequirement). Empty means no
	// gate.
	Supplies []engine.SupplyRequirement `json:"supplies,omitempty"`
	// SupplyConsumers pair each wasm module in this entry unit with the supply
	// nodes it consumes, so the runner can register the module as a supply
	// consumer and have a content change rebuild its instance pool. Supplies is
	// the readiness gate; this is the delivery route. A runner that does not
	// recognize this field (old binary) registers nothing — byte-identical to
	// the behavior before this field existed, where nothing registered either.
	SupplyConsumers []engine.SupplyConsumerBinding `json:"supply_consumers,omitempty"`
}

// ActivationKind values for ActivateDirective.Kind.
const (
	ActivationKindTrigger = "trigger"
	ActivationKindSupply  = "supply"
)

// --- Deactivate directive (server → runner) ---

// DeactivateDirective is sent by the activation reconciler to a runner,
// instructing it to stop hosting a trigger entry unit.
type DeactivateDirective struct {
	Namespace   string `json:"namespace"`
	WorkflowID  string `json:"workflow_id"`
	EntryUnitID string `json:"entry_unit_id"` // was GroupID
	Generation  uint64 `json:"generation"`
}

// --- Activation acknowledgment (runner → server) ---

type ActivationStatus string

const (
	ActivationStatusActive      ActivationStatus = "active"
	ActivationStatusDeactivated ActivationStatus = "deactivated"
	ActivationStatusFailed      ActivationStatus = "failed"
)

// ActivationAck is sent by a runner to acknowledge an activate/deactivate directive.
type ActivationAck struct {
	RunnerID   string `json:"runner_id"`
	SessionID  string `json:"session_id"`
	AuthToken  string `json:"auth_token,omitempty"`
	WorkflowID string `json:"workflow_id"`
	// WorkflowVersion is REQUIRED: the server needs it to build the full
	// EntryActivationKey and Get the record directly instead of scanning the
	// namespace. An ack that omits it is rejected (ErrMissingWorkflowVersion,
	// HTTP 400) rather than treated as an old-runner case — the ack send path
	// and this field ship in the same feature, so no runner that can send an
	// ack lacks the value.
	WorkflowVersion string           `json:"workflow_version,omitempty"`
	GroupID         string           `json:"group_id"`
	Generation      uint64           `json:"generation"`
	Status          ActivationStatus `json:"status"`
	Error           string           `json:"error,omitempty"`
}

// --- Heartbeat response extension ---

// HeartbeatActivations is the activation directive payload piggybacked on
// heartbeat responses. The runner processes these directives after each
// heartbeat ACK.
type HeartbeatActivations struct {
	Activate   []ActivateDirective   `json:"activate,omitempty"`
	Deactivate []DeactivateDirective `json:"deactivate,omitempty"`
}

// --- Runner inventory (runner → server on reconnect) ---

// ActivationInventoryItem reports a single activation the runner is currently hosting.
// Sent during Register to allow the controller to reconcile on reconnect.
type ActivationInventoryItem struct {
	WorkflowID      string `json:"workflow_id"`
	WorkflowVersion string `json:"workflow_version,omitempty"` // empty for old runners (backward-compat)
	EntryUnitID     string `json:"entry_unit_id"`              // was GroupID
	Generation      uint64 `json:"generation"`
}
