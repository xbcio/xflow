package protocol

// EntrySeedProtocolVersion is the wire protocol version for the entry-seed
// endpoint (POST /v1/executions). A runner stamps this into every
// SeedExecutionRequest so the control plane can detect and reject mismatched
// wire formats.
const EntrySeedProtocolVersion = 1

// BoundaryExit is the on-wire representation of a single entry-unit boundary
// exit port output (mirrors engine.BoundaryExit). It carries the node name,
// exit port, and the emitted data payload.
type BoundaryExit struct {
	NodeName string         `json:"node_name"`
	Port     string         `json:"port"`
	Data     map[string]any `json:"data,omitempty"`
}

// SeedExecutionRequest is the on-wire request a runner sends to seed an
// execution from an entry unit (single node or group node). The control plane
// resolves the authoritative namespace server-side from the authenticated
// principal — the namespace is deliberately NOT part of this body, so a forged
// or cross-namespace admission key fails closed.
type SeedExecutionRequest struct {
	ProtocolVersion int            `json:"protocol_version"`
	WorkflowID      string         `json:"workflow_id"`
	WorkflowVersion string         `json:"workflow_version,omitempty"`
	EntryUnitID     string         `json:"entry_unit_id"`
	AdmissionKey    string         `json:"admission_key"`
	Outcome         string         `json:"outcome"`
	Exits           []BoundaryExit `json:"exits,omitempty"`
	Error           string         `json:"error,omitempty"`
}

// SeedExecutionResponse is the on-wire response for a seed request. State is
// "accepted" or "conflict"; Duplicate is true when the same admission key +
// result hash was already accepted (idempotent retry returns the same
// ExecutionID).
type SeedExecutionResponse struct {
	State       string `json:"state"`
	ExecutionID string `json:"execution_id"`
	Duplicate   bool   `json:"duplicate,omitempty"`
}
