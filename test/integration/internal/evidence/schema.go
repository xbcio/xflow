package evidence

import (
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
)

// SchemaVersion is the current envelope schema version.
const SchemaVersion = 2

// Envelope is the versioned artifact envelope that bundles raw observations,
// source provenance, environment metadata, and independent verification.
//
// It matches spec §8.1 and is produced by tests as a raw ledger and by the
// verifier as a finalized artifact.
type Envelope struct {
	SchemaVersion       int                  `json:"schema_version"`
	RunID               string               `json:"run_id"`
	StartedAt           time.Time            `json:"started_at"`
	FinishedAt          time.Time            `json:"finished_at"`
	Source              SourceProvenance     `json:"source"`
	Environment         Environment          `json:"environment"`
	Suite               SuiteSummary         `json:"suite"`
	Raw                 RawLedger            `json:"raw"`
	DerivedObservations []DerivedObservation `json:"derived_observations"`
	Verification        Verification         `json:"verification"`
}

// SourceProvenance records the exact source state that produced the evidence.
// All fields are recomputed independently by the verifier; tests must not
// pre-compute or hard-code them.
type SourceProvenance struct {
	CommitSHA          string `json:"commit_sha"`
	RelevantTreeClean  bool   `json:"relevant_tree_clean"`
	RelevantDiffSHA256 string `json:"relevant_diff_sha256"`
	TestBinarySHA256   string `json:"test_binary_sha256"`
	GoVersion          string `json:"go_version"`
}

// Environment records runtime dependency versions, not Docker image tags.
type Environment struct {
	RedisVersion string `json:"redis_version"`
	MySQLVersion string `json:"mysql_version"`
}

// SuiteSummary records the test suite outcome as recomputed from go test -json.
type SuiteSummary struct {
	ExitCode             int `json:"exit_code"`
	SkipCount            int `json:"skip_count"`
	DroppedRuntimeEvents int `json:"dropped_runtime_events"`
	RequiredRows         int `json:"required_rows"`
	ObservedRows         int `json:"observed_rows"`
}

// RawLedger holds all raw observations. Tests write only these records; they
// never write derived_observations or pre-aggregated gate rows.
type RawLedger struct {
	RuntimeEvents        []engine.RuntimeEvidenceEvent `json:"runtime_events"`
	CounterSnapshots     []CounterSnapshot             `json:"counter_snapshots"`
	ProtocolObservations []ProtocolObservation         `json:"protocol_observations"`
	StateSnapshots       []StateSnapshot               `json:"state_snapshots"`
	SuiteRecords         []SuiteRecord                 `json:"suite_records"`
}

// CounterSnapshot records a handler counter value bound to a specific instance.
type CounterSnapshot struct {
	RunID       string            `json:"run_id"`
	Topology    string            `json:"topology"`
	ExecutionID types.ExecutionID `json:"execution_id"`
	NodeName    string            `json:"node_name"`
	CounterID   string            `json:"counter_id"`
	HandlerName string            `json:"handler_name"`
	Value       int               `json:"value"`
	ObservedAt  time.Time         `json:"observed_at"`
}

// ProtocolObservation records protocol-level events such as loss injections,
// authority rejections, and directory/sweeper outcomes.
type ProtocolObservation struct {
	RunID       string            `json:"run_id"`
	Topology    string            `json:"topology"`
	ExecutionID types.ExecutionID `json:"execution_id"`
	Type        string            `json:"type"`
	ObservedAt  time.Time         `json:"observed_at"`
	Detail      map[string]any    `json:"detail"`
}

// StateSnapshot records authoritative state/outbox reads at defined moments.
type StateSnapshot struct {
	RunID       string            `json:"run_id"`
	Topology    string            `json:"topology"`
	ExecutionID types.ExecutionID `json:"execution_id"`
	Type        string            `json:"type"`
	ObservedAt  time.Time         `json:"observed_at"`
	State       map[string]any    `json:"state"`
}

// SuiteRecord mirrors one go test -json record bound to the run.
type SuiteRecord struct {
	RunID          string  `json:"run_id"`
	TestName       string  `json:"test_name"`
	Package        string  `json:"package"`
	Action         string  `json:"action"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	Output         string  `json:"output,omitempty"`
}

// DerivedObservation is produced mechanically by the verifier from raw records.
// Every observation references the raw event IDs or counter snapshot IDs that
// justify it.
type DerivedObservation struct {
	Kind                string                   `json:"kind"`
	Scenario            string                   `json:"scenario,omitempty"`
	Fixture             string                   `json:"fixture,omitempty"`
	Topology            string                   `json:"topology,omitempty"`
	ExecutionID         types.ExecutionID        `json:"execution_id,omitempty"`
	CommitEventID       string                   `json:"commit_event_id,omitempty"`
	AdvanceEventID      string                   `json:"advance_event_id,omitempty"`
	CounterSnapshotID   string                   `json:"counter_snapshot_id,omitempty"`
	RetryEventID        string                   `json:"retry_event_id,omitempty"`
	AcceptedCommit      bool                     `json:"accepted_commit"`
	AppliedAdvance      bool                     `json:"applied_advance"`
	Classification      *EffectiveClassification `json:"classification,omitempty"`
	HandlerInvocations  int                      `json:"handler_invocations"`
	SourceEventIDs      []string                 `json:"source_event_ids"`
	EvidenceSource      string                   `json:"evidence_source"`
	Reason              string                   `json:"reason,omitempty"`
}

// EffectiveClassification is a structured projection of a classified error.
// It intentionally does not carry error full text, credentials, or tenant data.
type EffectiveClassification struct {
	Source    engine.ErrorSource `json:"source"`
	Classified bool              `json:"classified"`
	Kind      types.ErrorKind    `json:"kind,omitempty"`
	Retryable *bool              `json:"retryable,omitempty"`
	Permanent *bool              `json:"permanent,omitempty"`
	Code      string             `json:"code,omitempty"`
}

// Verification records the independent verifier outcome.
type Verification struct {
	Passed           bool     `json:"passed"`
	Errors           []string `json:"errors,omitempty"`
	SourceRecomputed bool     `json:"source_recomputed"`
	SuiteRecomputed  bool     `json:"suite_recomputed"`
}

// NewEnvelope creates an envelope with a fresh run ID and started timestamp.
func NewEnvelope() *Envelope {
	return &Envelope{
		SchemaVersion: SchemaVersion,
		RunID:         uuid.New().String(),
		StartedAt:     time.Now().UTC(),
	}
}

// EffectiveClassificationFromEvent projects an engine event into the envelope
// classification struct without copying full error text or tenant payload.
//
// Business and explicit error-port outputs are not wrapped in a ClassifiedError
// (Classified==false), but they still carry a stable kind by matrix convention,
// so they are projected here. Unclassified successful commits return nil.
func EffectiveClassificationFromEvent(ev engine.RuntimeEvidenceEvent) *EffectiveClassification {
	switch {
	case ev.Classified:
		// keep projection below
	case ev.ErrorSource == engine.ErrorSourceBusiness, ev.ErrorSource == engine.ErrorSourceErrorPort:
		// recognized categories with a stable kind despite Classified==false
	default:
		return nil
	}

	kind := ev.ErrorKind
	if kind == "" {
		switch ev.ErrorSource {
		case engine.ErrorSourceBusiness:
			kind = types.ErrorKindBusiness
		case engine.ErrorSourceErrorPort:
			kind = types.ErrorKindErrorPort
		}
	}

	return &EffectiveClassification{
		Source:     ev.ErrorSource,
		Classified: ev.Classified,
		Kind:       kind,
		Retryable:  ev.Retryable,
		Permanent:  ev.Permanent,
		Code:       ev.ErrorCode,
	}
}
