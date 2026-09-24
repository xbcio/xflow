package protocol

import (
	"encoding/json"
	"errors"
	"runtime/debug"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Capability is what a runner advertises it can execute. It is transmitted on
// register/poll/hello, stored as a JSON blob in the runner directory, and
// echoed verbatim by GET /v1/management/runners/{id} — the type declares no
// MarshalJSON and nothing on that path redacts, so every field placed here is
// readable by any caller holding runner-read scope.
type Capability struct {
	NodeType    string   `json:"node_type"`
	NodeVersion int      `json:"node_version,omitempty"`
	Runtimes    []string `json:"runtimes,omitempty"`
	Features    []string `json:"features,omitempty"`
	Resources   []string `json:"resources,omitempty"`
	// Credentials holds credential reference NAMES, never credential material.
	// Given the echo path above, a value placed here is disclosed, not stored.
	//
	// No production constructor writes this field today: runnerCapabilities in
	// sdk/xflow sets only NodeType/Features, and the matching side
	// (control.MatchCapabilities, canRunRouting) never reads it — so
	// credential-aware routing is declared here but not implemented. A node's
	// Definition.Credential(name) declaration reaches types.Descriptor and stops
	// there; graph.Requirement.Credentials is left zero by both
	// buildPackageRequirements and buildBodyRequirements.
	//
	// Before wiring it up, note that execution/subgraph validates a package's
	// required credentials against registryInventory.Credentials(), which is
	// hardcoded to return nil — so the first requirement that arrives non-empty
	// is rejected as "credential not declared" rather than routed.
	Credentials []string `json:"credentials,omitempty"`
}

type RegisterRunnerRequest struct {
	RunnerID     string            `json:"runner_id"`
	Concurrency  int               `json:"concurrency"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities []Capability      `json:"capabilities"`
	Namespaces   []string          `json:"namespaces,omitempty"`
	// AuthToken is the runner's bearer token. Preferred: Authorization
	// header. This body field is a fallback for transports that can't set
	// headers.
	AuthToken string `json:"auth_token,omitempty"`
	// Activations reports the trigger entry-unit activations the runner is
	// currently hosting, sent on (re)register so the control plane can reconcile
	// its assignments on reconnect: still-hosted activations have their lease
	// renewed (generation unchanged) rather than being orphaned, and assignments
	// the reconnected runner no longer hosts are revoked for reassignment. Empty
	// on a fresh runner or one that hosts no triggers. Carries no secret values
	// (only workflow/entry-unit identity + generation).
	Activations []ActivationInventoryItem `json:"activations,omitempty"`
	// SupportsEncryption indicates the runner can receive and decrypt $enc
	// supply envelopes. When true the server includes a SupplyKey in the
	// registration response and encrypts supply GET responses for this runner.
	SupportsEncryption bool `json:"supports_encryption,omitempty"`
}

// RunnerXflowVersionLabel is the registration label a runner uses to report the
// xflow revision its binary links.
//
// It exists because the capability guard cannot see a version. A control plane
// admits a runner by comparing node TYPES, and the capabilities an SDK declares
// are identical across releases in which a node's parameters gained a mode: the
// capability says "xflow.http", never "xflow.http with mode=raw". Two deployments
// one release apart therefore register successfully and then disagree about what
// a parameter means — the older one ignoring it — with no signal on either side.
//
// A LABEL carries it because that is the part of the registration a control plane
// already compares against exact expected values, and because labels are the only
// free-form map on the registration wire that every transport (HTTP, gRPC,
// in-process, stream) already carries. A new protocol field would be the more
// honest home and costs a protobuf regeneration plus a matching change on every
// side that constructs the DTO; this costs one map entry.
//
// It is a RESERVED key: a runner's own labels must not use it, and the SDK
// overrides any value a caller sets for it.
const RunnerXflowVersionLabel = "xflow_version"

// xflowModule names the module whose version is reported. It is spelled out
// rather than read from BuildInfo().Main.Path because a RUNNER is its own module
// and links xflow as a dependency — the main module's path is the runner's.
const xflowModule = "github.com/xbcio/xflow"

// LinkedXflowVersion reports the version of this module the running binary was
// built against, or "" when that cannot be established.
//
// Empty is a distinct answer from a version, and callers must treat it as "not
// reported" rather than as agreement: a build that cannot name its own version
// cannot be compared against anything.
func LinkedXflowVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	// Built FROM this module — cmd/runner, cmd/server, or a test binary in this
	// repo. There is then no dependency entry to read, and the main module's
	// version describes a working tree rather than a release, so it is reported
	// as unknown instead of as a version two deployments might match on.
	if info.Main.Path == xflowModule {
		return ""
	}
	for _, dep := range info.Deps {
		if dep == nil || dep.Path != xflowModule {
			continue
		}
		// The replacement is what is actually linked. A module replacement
		// answers honestly; a filesystem replacement has no comparable version
		// and is rejected below rather than allowed to claim the version it
		// replaced — which would let a side running unreleased code assert a
		// released version.
		if dep.Replace != nil {
			return comparableModuleVersion(dep.Replace.Version)
		}
		return comparableModuleVersion(dep.Version)
	}
	return ""
}

// comparableModuleVersion reports a version only when it is one two deployments
// can meaningfully compare: a release tag or a pseudo-version, both of which
// start with "v". "(devel)" is a working tree and "" is a filesystem
// replacement; neither names a release, so both become "unknown".
func comparableModuleVersion(version string) string {
	version = strings.TrimSpace(version)
	if !strings.HasPrefix(version, "v") {
		return ""
	}
	return version
}

// RunnerControlDirective is the server's current desired scheduling state for
// a runner session. It is a cooperative convergence signal only: the control
// plane always applies the authoritative DRAINING gate before creating a queue
// claim, including for old runners that do not understand this DTO.
type RunnerControlDirective struct {
	DesiredState string `json:"desired_state"`
	Generation   uint64 `json:"generation"`
	// RecoveryOnly tells a capable runner to stop ordinary polling and request
	// only already-finalized handoff replay. It is true while desired_state is
	// draining and intentionally may be ignored by older peers.
	RecoveryOnly bool `json:"recovery_only,omitempty"`
}

// RunnerDrainObservation is a runner-local observation made only after the
// runner has applied a DRAINING control directive. InFlight remains on the
// enclosing heartbeat so older peers retain their existing capacity payload.
// The control plane fences this observation by the live session and control
// generation before using it in a drain-completion projection.
type RunnerDrainObservation struct {
	Generation        uint64 `json:"generation"`
	RecoveryOnly      bool   `json:"recovery_only"`
	ActiveActivations uint32 `json:"active_activations"`
}

type RegisterRunnerResponse struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	// Control is included by servers whose runner directory supports graceful
	// drain. Nil preserves compatibility with older servers/directories.
	Control *RunnerControlDirective `json:"control,omitempty"`
	// SupplyKey is a base64-encoded 32-byte AES-256 key for decrypting supply
	// content. Included only when the runner declared supports_encryption=true
	// and the server has encryption enabled. The runner stores it in its
	// keyring and sends Accept: application/x-xflow-encrypted on supply fetches.
	SupplyKey string `json:"supply_key,omitempty"`
}

type HeartbeatRequest struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	Capacity  int    `json:"capacity"`
	InFlight  int    `json:"in_flight"`
	Timestamp int64  `json:"timestamp"`
	AuthToken string `json:"auth_token,omitempty"`
	// DrainObservation is sent only while this runner has applied a draining
	// directive. It is advisory evidence, never the server-side admission
	// authority: the directory verifies session and generation atomically.
	DrainObservation *RunnerDrainObservation `json:"drain_observation,omitempty"`
	// SupplyObserved reports the content hash currently in effect for each supply
	// this runner hosts a consumer for. Aggregated server-side it answers "are all
	// runners on revision N yet" — the direct analogue of Kubernetes'
	// observedGeneration, which the platform otherwise has no equivalent of.
	//
	// It carries hashes, never content.
	SupplyObserved map[string]string `json:"supply_observed,omitempty"`
	// SupplyKeyID is the ID of the supply encryption key this runner currently
	// holds. The server compares it against its own and returns the key in
	// SupplyKeyRotation only when they differ, which makes rotation delivery
	// convergent: a runner that was down during a rotation, restarted, or
	// joined afterwards all catch up on their next heartbeat, with no
	// per-runner bookkeeping on the server.
	//
	// The ID is a 4-byte fingerprint of the key, not key material. Empty when
	// this runner holds no key or does not use supply encryption.
	SupplyKeyID string `json:"supply_key_id,omitempty"`
}

type HeartbeatResponse struct {
	ServerTime  int64                 `json:"server_time"`
	Activations *HeartbeatActivations `json:"activations,omitempty"`
	// Control lets a runner promptly converge its local poll gate after an
	// operator changes desired state.
	Control *RunnerControlDirective `json:"control,omitempty"`
	// SupplyHints carries "supply node name → current content hash" for the
	// supplies this runner hosts a consumer for. A differing hash tells the runner
	// to fetch once; an equal hash costs nothing.
	//
	// This is a latency optimization, NOT the correctness channel: hints are lost
	// to partitions, restarts and leader changes. Convergence is guaranteed by the
	// activation-time fetch plus TTL polling. Only hashes travel here, so the
	// heartbeat body does not grow with content size.
	SupplyHints map[string]string `json:"supply_hints,omitempty"`
	// SupplyKeyRotation carries a base64-encoded new AES-256 key when the server
	// rotates the supply encryption key. The runner installs it as current and
	// demotes the old current to previous. Absent when the runner's reported
	// SupplyKeyID already matches the server's current key — so this is sent
	// once per rotation per runner, never repeatedly. Redelivery would be
	// actively harmful: installSupplyKey calls Keyring.Rotate on each delivery,
	// so a second delivery of the same key demotes the key it just promoted and
	// evicts the previous one, breaking decryption of content encrypted before
	// the rotation.
	SupplyKeyRotation string `json:"supply_key_rotation,omitempty"`
	// MetricsReportIntervalSeconds, when non-zero, tells the runner how often to
	// ship its metrics to the server. It is the scrape cadence expressed to the
	// runner, so raising Prometheus' scrape_interval does not require touching
	// every runner's flags.
	//
	// Three states, with omitempty making an old server land on "no opinion":
	//   absent / 0 → keep whatever the runner is using (its local default)
	//   > 0        → adopt this many seconds (the server has already clamped it)
	//   < 0        → suspend reporting; the reporter stays alive and resumes on
	//                the next positive value
	//
	// The negative state is the operational valve: when the server or Redis is
	// under pressure, the whole fleet can be told to stop reporting without
	// restarting a single runner.
	MetricsReportIntervalSeconds int `json:"metrics_report_interval_seconds,omitempty"`
}

type PollTaskRequest struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	// Deprecated: retained for compatibility with older control planes. Current
	// control planes use Register and Heartbeat as the capacity authority.
	Capacity int `json:"capacity"`
	// Deprecated: retained for compatibility with older control planes. Current
	// control planes route with labels accepted at Register, never poll input.
	Labels map[string]string `json:"labels,omitempty"`
	// Deprecated: retained for compatibility with older control planes. Current
	// control planes route with capabilities accepted at Register, never poll input.
	Capabilities []Capability `json:"capabilities"`
	AuthToken    string       `json:"auth_token,omitempty"`
	// ActiveLeaseIDs lists the leases this runner's workers are executing at
	// the moment of the poll. The control plane replays a finalized lease that
	// never reached its runner (lost response, restarted process), and it
	// cannot tell that case apart from a lease the runner is mid-handler on —
	// both are "leased to this runner, this session". Only the runner knows,
	// so it says, and the server replays everything it does not name.
	//
	// Without this a runner with Concurrency > 1 had each of its idle workers
	// handed the lease its busy sibling was running, so every node executed
	// once per unit of concurrency.
	//
	// Lease IDs are opaque server-issued identifiers, not secrets, and carry no
	// task input. An old runner omits the field, which reads as "nothing in
	// flight" and restores the previous behaviour for it alone.
	ActiveLeaseIDs []string `json:"active_lease_ids,omitempty"`
	// RecoveryOnly is sent after a capable runner receives a DRAINING directive.
	// It requests lease replay only and can never grant extra work; server-side
	// desired-state gating remains authoritative.
	RecoveryOnly bool `json:"recovery_only,omitempty"`
}

type PollTaskResponse struct {
	Lease   *engine.TaskLease       `json:"lease,omitempty"`
	Wait    time.Duration           `json:"wait"`
	Control *RunnerControlDirective `json:"control,omitempty"`
}

type ReportResultRequest struct {
	RunnerID  string            `json:"runner_id"`
	SessionID string            `json:"session_id"`
	Lease     *engine.TaskLease `json:"lease"`
	Result    engine.TaskResult `json:"result"`
	AuthToken string            `json:"auth_token,omitempty"`
	// TraceCarrier holds W3C traceparent/tracestate headers set by the runner
	// after executing the task. The control plane extracts these to create a
	// commit span parented to the runner's execute span.
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
	// GroupResult is set (instead of Result) when the runner reports a group
	// execution outcome. The control plane uses it to commit via
	// CommitGroupResult rather than CommitTaskResultWithOutcome.
	GroupResult *engine.GroupResult `json:"group_result,omitempty"`
}

type reportResultRequestJSON struct {
	RunnerID     string              `json:"runner_id"`
	SessionID    string              `json:"session_id"`
	Lease        *engine.TaskLease   `json:"lease"`
	Result       json.RawMessage     `json:"result"`
	AuthToken    string              `json:"auth_token,omitempty"`
	TraceCarrier map[string]string   `json:"trace_carrier,omitempty"`
	GroupResult  *engine.GroupResult `json:"group_result,omitempty"`
}

type taskResultJSON struct {
	Output  *types.Output      `json:"output,omitempty"`
	Suspend *types.SuspendSpec `json:"suspend,omitempty"`
	// Error is the legacy string-only error representation. It is always
	// populated when result.Error is non-nil so older peers that only read this
	// field continue to function (without classification).
	Error string `json:"error,omitempty"`
	// ErrorDetail carries the structured wire error DTO. Newer peers read it
	// to recover retry/permanent classification; older peers ignore it. It is
	// set when result.Error is a *types.ClassifiedError or is marked permanent
	// via types.ErrPermanent.
	ErrorDetail *types.ClassifiedError `json:"error_detail,omitempty"`
}

func (r ReportResultRequest) MarshalJSON() ([]byte, error) {
	resultJSON, err := MarshalTaskResult(r.Result)
	if err != nil {
		return nil, err
	}
	return json.Marshal(reportResultRequestJSON{
		RunnerID:     r.RunnerID,
		SessionID:    r.SessionID,
		Lease:        r.Lease,
		Result:       resultJSON,
		AuthToken:    r.AuthToken,
		TraceCarrier: r.TraceCarrier,
		GroupResult:  r.GroupResult,
	})
}

func (r *ReportResultRequest) UnmarshalJSON(data []byte) error {
	var in reportResultRequestJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	r.RunnerID = in.RunnerID
	r.SessionID = in.SessionID
	r.Lease = in.Lease
	r.AuthToken = in.AuthToken
	r.TraceCarrier = in.TraceCarrier
	r.GroupResult = in.GroupResult
	result, err := UnmarshalTaskResult(in.Result)
	if err != nil {
		return err
	}
	r.Result = result
	return nil
}

type ReportResultResponse struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
}

// MarshalTaskResult encodes a task result to JSON. It is the single source of
// truth for result serialization shared by the HTTP and gRPC transports.
//
// Error classification is preserved across the wire: when result.Error is a
// *types.ClassifiedError (or is marked permanent via types.ErrPermanent), the
// structured DTO is emitted in error_detail alongside the legacy error string.
// This lets new servers apply retry/on-error policy without parsing error text
// while old peers still read the string. Unmarked errors serialize as the
// legacy string only, preserving the pre-DTO behavior.
func MarshalTaskResult(result engine.TaskResult) ([]byte, error) {
	out := taskResultJSON{
		Output:  result.Output,
		Suspend: result.Suspend,
	}
	if result.Error != nil {
		out.Error = result.Error.Error()
		switch err := result.Error.(type) {
		case *types.ClassifiedError:
			out.ErrorDetail = err
		default:
			// An error stamped permanent via errors.Join(ErrPermanent, ...)
			// (e.g. the dispatcher's PermanentConfiguration failure) is not a
			// *ClassifiedError, but its classification must still survive the
			// wire: synthesize a permanent DTO from it.
			if types.IsPermanent(err) {
				out.ErrorDetail = &types.ClassifiedError{
					Kind:      types.ErrorKindPermanent,
					Message:   err.Error(),
					Permanent: true,
				}
			}
		}
	}
	return json.Marshal(out)
}

// UnmarshalTaskResult decodes a task result produced by MarshalTaskResult.
//
// When error_detail is present the structured ClassifiedError is recovered so
// types.IsPermanent(result.Error) reflects the runner's classification. When
// only the legacy error string is present (old runner), a plain error is
// reconstructed — equivalent to the pre-DTO behavior, treated as transient.
func UnmarshalTaskResult(data []byte) (engine.TaskResult, error) {
	var in taskResultJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return engine.TaskResult{}, err
	}
	result := engine.TaskResult{
		Output:  in.Output,
		Suspend: in.Suspend,
	}
	switch {
	case in.ErrorDetail != nil:
		result.Error = in.ErrorDetail
	case in.Error != "":
		result.Error = errors.New(in.Error)
	}
	return result, nil
}

// RunnerFrame is the transport-agnostic runner→server frame (mirrors runnerpb.RunnerFrame.oneof).
type RunnerFrame struct {
	Hello              *HelloFrame
	Result             *ResultFrame
	ControlObservation *ControlObservationFrame
	Bye                *ByeFrame
}

type HelloFrame struct {
	RunnerID     string
	Concurrency  int
	Capabilities []Capability
	Labels       map[string]string
	Namespaces   []string
}

type ResultFrame struct {
	LeaseID string
	Lease   *engine.TaskLease
	Result  engine.TaskResult
}

// ControlObservationFrame reports the runner's locally applied control state
// on an established Connect stream. RecoveryOnly is the stream's recovery-only
// request: it asks for pre-drain debt only and never authorizes new admission.
// The server still decides every claim against its authoritative control state.
type ControlObservationFrame struct {
	Generation        uint64
	RecoveryOnly      bool
	ActiveWorkers     uint32
	ActiveActivations uint32
}

type ByeFrame struct{}

// ServerFrame is the transport-agnostic server→runner frame.
type ServerFrame struct {
	Welcome   *WelcomeFrame
	Control   *ControlFrame
	Task      *TaskFrame
	Ack       *AckFrame
	Backoff   *BackoffFrame
	Keepalive *KeepaliveFrame
}

type WelcomeFrame struct {
	RunnerID   string
	ServerTime int64
	// Control is the initial desired state for this stream. Nil means an older
	// server did not project control; callers retain the active/generation-zero
	// compatibility default in that case.
	Control *RunnerControlDirective
}

// ControlFrame carries a desired-state update after WelcomeFrame.
type ControlFrame struct {
	Directive *RunnerControlDirective
}

type TaskFrame struct {
	Lease *engine.TaskLease
}

type AckFrame struct {
	LeaseID  string
	Accepted bool
	Error    string
}

type BackoffFrame struct {
	Wait time.Duration
}

type KeepaliveFrame struct{}

// FrameStream is the runner-facing bidirectional stream abstraction. gRPC
// wraps the generated bidi stream; HTTP simulates it with long-poll. runner.Run
// speaks only this interface.
type FrameStream interface {
	Send(RunnerFrame) error
	Recv() (ServerFrame, error)
	Close() error
}
