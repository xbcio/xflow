package protocol

// --- Path constants ---
const (
	ActivatePath       = "/v1/runners/activate"
	DeactivatePath     = "/v1/runners/deactivate"
	ActivationAckPath  = "/v1/runners/activation/ack"
	ActivationListPath = "/v1/activations"
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
}

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
	RunnerID   string           `json:"runner_id"`
	SessionID  string           `json:"session_id"`
	WorkflowID string           `json:"workflow_id"`
	GroupID    string           `json:"group_id"`
	Generation uint64           `json:"generation"`
	Status     ActivationStatus `json:"status"`
	Error      string           `json:"error,omitempty"`
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
	WorkflowID  string `json:"workflow_id"`
	EntryUnitID string `json:"entry_unit_id"` // was GroupID
	Generation  uint64 `json:"generation"`
}
