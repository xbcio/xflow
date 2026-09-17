package evidence

import (
	"time"

	"github.com/google/uuid"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// SchemaVersion is the current envelope schema version.
//
// Version history:
//
//	2 — git SHA + relevant-tree cleanliness + Go version + suite/raw ledgers.
//	3 — adds the `release` block: tag, Node/pnpm pins, GOOS/GOARCH, container
//	    image identity, human attestation, declared unverified scope, and the
//	    harness gate identity. A v3 artifact is the only shape the verifier
//	    publishes; v2 remains readable so historical artifacts stay parseable
//	    (MergeRawEnvelopes takes the fragment's version, and the G0 artifact
//	    validator accepts both versions — see VersionMinSupported).
//
// The bump is a bump, not a documentation change: at v3 the `release` block is
// required by the verifier and by the G0 artifact validator, so a v3 artifact
// that omits it fails loudly instead of passing as a v2 artifact would.
const SchemaVersion = 3

// VersionMinSupported is the oldest envelope schema version the current
// verifier and validators still understand. It exists so the "accept older
// artifacts" branch is named once instead of being spelled as a bare literal
// in each validator.
const VersionMinSupported = 2

// Envelope is the versioned artifact envelope that bundles raw observations,
// source provenance, environment metadata, release provenance, and independent
// verification.
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
	Release             ReleaseProvenance    `json:"release"`
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

// ReleaseProvenance is the schema-v3 release block: the release identity of the
// candidate that this artifact is evidence for.
//
// It is a VERIFIER OUTPUT. Nothing in it may be self-reported by the test
// binary or the recorder: the verifier recomputes the whole block from
// authoritative sources (git for the tag, the pinned toolchain files and the Go
// runtime for the versions and platform) and from the release-harness inputs
// (ReleaseInput) that no repository read can recover — the container images the
// gate ran against, the human attestation, the declared unverified scope, and
// the gate's own identity. MergeRawEnvelopes deliberately does not carry a
// fragment's Release block, so a fragment that stamps one cannot influence the
// finalized artifact.
type ReleaseProvenance struct {
	Gate            GateIdentity     `json:"gate"`
	Tag             string           `json:"tag"`
	TagKind         string           `json:"tag_kind"`
	GoVersion       string           `json:"go_version"`
	NodeVersion     string           `json:"node_version"`
	PnpmVersion     string           `json:"pnpm_version"`
	OS              string           `json:"os"`
	Arch            string           `json:"arch"`
	ContainerImages []ContainerImage `json:"container_images"`
	Attestation     Attestation      `json:"attestation"`
	UnverifiedScope []string         `json:"unverified_scope"`
}

// TagKind values for ReleaseProvenance.TagKind. The kind is recorded rather
// than inferred so "no tag points at this candidate" is distinguishable from
// "the tag was not recorded".
const (
	TagKindNone        = "none"
	TagKindLightweight = "lightweight"
	TagKindAnnotated   = "annotated"
)

// GateIdentity identifies the release-harness gate that produced this artifact.
// Name, Command and StartedAt are supplied by the harness, because only the
// harness knows which target was invoked and when it started. ExitCode is taken
// from the independently recomputed suite (the verifier fails the artifact on
// any mismatch, so a self-reported exit code cannot survive), and
// FinishedAt/DurationSeconds are derived by the verifier when it finalizes the
// artifact, so the recorded duration cannot be self-reported.
type GateIdentity struct {
	Name            string    `json:"name"`
	Command         string    `json:"command"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	ExitCode        int       `json:"exit_code"`
	DurationSeconds float64   `json:"duration_seconds"`
}

// ContainerImage is one dependency image the gate ran against. Digest is the
// registry digest (sha256:...) when the harness could resolve it; Resolved
// carries that fact explicitly so an unresolved digest can never be read as a
// pinned one.
type ContainerImage struct {
	Component string `json:"component"`
	Reference string `json:"reference"`
	Digest    string `json:"digest"`
	Resolved  bool   `json:"resolved"`
}

// Attestation records who vouches for this evidence. It is recorded, not
// recomputed: no repository read can establish who reviewed an artifact.
// SignedOff is required to equal "both names are present" so the boolean can
// never claim a sign-off that the names do not support.
type Attestation struct {
	Reviewer  string `json:"reviewer"`
	ReRunner  string `json:"re_runner"`
	SignedOff bool   `json:"signed_off"`
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
	RuntimeEvents           []CollectedRuntimeEvidenceEvent `json:"runtime_events"`
	CounterSnapshots        []CounterSnapshot               `json:"counter_snapshots"`
	ProtocolObservations    []ProtocolObservation           `json:"protocol_observations"`
	StateSnapshots          []StateSnapshot                 `json:"state_snapshots"`
	SuiteRecords            []SuiteRecord                   `json:"suite_records"`
	RunIdentities           []RunIdentity                   `json:"run_identities"`
	EnvironmentObservations []EnvironmentObservation        `json:"environment_observations"`
}

// EvidenceRecordMeta is the envelope metadata for a collected runtime evidence
// event. It records who produced the observation, when it was observed, and
// how it maps back to the run and execution.
type EvidenceRecordMeta struct {
	RunID            string            `json:"run_id"`
	Topology         string            `json:"topology,omitempty"`
	ProducerID       string            `json:"producer_id"`
	ExecutionID      types.ExecutionID `json:"execution_id"`
	ObservedAt       time.Time         `json:"observed_at"`
	SourceDigest     string            `json:"source_digest,omitempty"`
	TestBinaryDigest string            `json:"test_binary_digest,omitempty"`
}

// CollectedRuntimeEvidenceEvent wraps a raw engine.RuntimeEvidenceEvent with
// collector-side metadata. This is the element type stored in RawLedger.RuntimeEvents.
type CollectedRuntimeEvidenceEvent struct {
	Meta  EvidenceRecordMeta          `json:"meta"`
	Event engine.RuntimeEvidenceEvent `json:"event"`
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

// RunIdentity identifies the run and binds the test binary and manifest digests.
// It is emitted by the test binary into the raw ledger and checked by the
// verifier for consistency with the orchestrated run.
type RunIdentity struct {
	RunID            string    `json:"run_id"`
	TestBinaryDigest string    `json:"test_binary_digest"`
	ManifestDigest   string    `json:"manifest_digest"`
	ProducerID       string    `json:"producer_id,omitempty"`
	ObservedAt       time.Time `json:"observed_at"`
}

// EnvironmentObservation records a runtime environment version query.
type EnvironmentObservation struct {
	RunID      string    `json:"run_id"`
	Topology   string    `json:"topology"`
	Component  string    `json:"component"` // "redis" or "mysql"
	Query      string    `json:"query"`     // exact query string
	Result     string    `json:"result"`    // parsed version
	ObservedAt time.Time `json:"observed_at"`
}

// DerivedObservation is produced mechanically by the verifier from raw records.
// Every observation references the raw event IDs or counter snapshot IDs that
// justify it.
type DerivedObservation struct {
	Kind               string                   `json:"kind"`
	Scenario           string                   `json:"scenario,omitempty"`
	Fixture            string                   `json:"fixture,omitempty"`
	Topology           string                   `json:"topology,omitempty"`
	ExecutionID        types.ExecutionID        `json:"execution_id,omitempty"`
	CommitEventID      string                   `json:"commit_event_id,omitempty"`
	AdvanceEventID     string                   `json:"advance_event_id,omitempty"`
	CounterSnapshotID  string                   `json:"counter_snapshot_id,omitempty"`
	RetryEventID       string                   `json:"retry_event_id,omitempty"`
	AcceptedCommit     bool                     `json:"accepted_commit"`
	AppliedAdvance     bool                     `json:"applied_advance"`
	Classification     *EffectiveClassification `json:"classification,omitempty"`
	HandlerInvocations int                      `json:"handler_invocations"`
	HandlerName        string                   `json:"handler_name,omitempty"`
	SourceEventIDs     []string                 `json:"source_event_ids"`
	EvidenceSource     string                   `json:"evidence_source"`
	Reason             string                   `json:"reason,omitempty"`
}

// EffectiveClassification is a structured projection of a classified error.
// It intentionally does not carry error full text, credentials, or namespace data.
type EffectiveClassification struct {
	Source     engine.ErrorSource `json:"source"`
	Classified bool               `json:"classified"`
	Kind       types.ErrorKind    `json:"kind,omitempty"`
	Retryable  *bool              `json:"retryable,omitempty"`
	Permanent  *bool              `json:"permanent,omitempty"`
	Code       string             `json:"code,omitempty"`
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
// classification struct without copying full error text or namespace payload.
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
