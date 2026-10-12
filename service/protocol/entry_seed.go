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
	// Generation is the entry-activation generation the seeding runner believes
	// it currently owns. The control plane fences by EXACT generation: when an
	// activation record governs this entry unit, a seed must carry precisely the
	// currently-assigned generation to seed a NEW execution; any other value
	// (below = superseded runner, above = forged) is treated as stale and
	// rejected fail-closed, EXCEPT an already-accepted admission key is still
	// duplicate-accepted so the runner can commit its Kafka offset (spec §11.6).
	// Zero is NOT a bypass: it is fenced like any other mismatch when an
	// activation governs the unit. Unfenced admission happens only when NO
	// activation record governs the unit (e.g. a locally-hosted single trigger
	// not driven by an EntryActivation) — that path is keyed on the absence of
	// an activation record, never on a magic zero value.
	Generation uint64 `json:"generation,omitempty"`
	// ReplicaIndex selects the sibling activation whose generation must own this
	// seed. It is not part of the admission key or result hash.
	ReplicaIndex uint32 `json:"replica_index,omitempty"`
}

// SeedExecutionResponse is the on-wire response for a seed request. State is
// "accepted" or "conflict" on a 2xx; a refused seed answers 429 with
// SeedExecutionRefusal instead (see its doc).
//
// Duplicate is true when the same admission key + result hash was already
// accepted (idempotent retry returns the same ExecutionID).
type SeedExecutionResponse struct {
	State       string `json:"state"`
	ExecutionID string `json:"execution_id"`
	Duplicate   bool   `json:"duplicate,omitempty"`
	// Queued is true when the admitted execution was parked in its workflow's
	// queue-mode line, and QueuePosition is its 1-based place (0 when it is not
	// queued). A parked seed is still a complete admission — the execution is
	// durable and the caller commits the offset either way — so these fields are
	// informational, and they mirror the execute face's queued/queue_position so
	// both faces describe a queued execution the same way.
	//
	// The runner shipped in this repository does not consume them: a batch being
	// HELD BACK is the Refused path, which withholds the offset and has its own
	// state on the batch-admission metric, whereas a batch that was parked was
	// admitted and moves on. They are on the wire for a caller that records them
	// and for the symmetry above, not because an offset decision depends on them.
	Queued        bool `json:"queued,omitempty"`
	QueuePosition int  `json:"queue_position,omitempty"`
}

// Refusal states a seed admission can be answered with. They are the engine's
// own admission states carried verbatim, so a state the control plane reports
// and a state a runner classifies are the same string; the values are stable
// wire vocabulary, not an implementation detail.
const (
	// SeedStateOverLimit: the workflow is at its concurrency limit and the
	// refusal is REJECT-mode — nothing was persisted.
	SeedStateOverLimit = "over_limit"
	// SeedStateQueueFull: the workflow's queue-mode backlog is at max_queued —
	// nothing was persisted either.
	SeedStateQueueFull = "queue_full"
)

// SeedExecutionRefusal is the 429 body for a refused seed admission.
//
// It is a separate body shape from SeedExecutionResponse because the HTTP status
// is the contract: a refusal is a decision the runner acts on — it withholds the
// Kafka offset and retries after a backoff (decision 5) — and it must not be
// mistakable for an admission. Error carries the same stable code the execute
// path uses for the same two refusals (workflow_concurrency_limit /
// workflow_concurrency_queue_full), so one vocabulary covers both faces.
type SeedExecutionRefusal struct {
	State string `json:"state"`
	Error string `json:"error"`
}
