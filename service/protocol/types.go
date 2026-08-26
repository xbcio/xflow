package protocol

import (
	"encoding/json"
	"errors"
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

type RegisterRunnerResponse struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
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
	RunnerID     string            `json:"runner_id"`
	SessionID    string            `json:"session_id"`
	Capacity     int               `json:"capacity"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities []Capability      `json:"capabilities"`
	AuthToken    string            `json:"auth_token,omitempty"`
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
}

type PollTaskResponse struct {
	Lease *engine.TaskLease `json:"lease,omitempty"`
	Wait  time.Duration     `json:"wait"`
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
	Hello  *HelloFrame
	Result *ResultFrame
	Bye    *ByeFrame
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

type ByeFrame struct{}

// ServerFrame is the transport-agnostic server→runner frame.
type ServerFrame struct {
	Welcome   *WelcomeFrame
	Task      *TaskFrame
	Ack       *AckFrame
	Backoff   *BackoffFrame
	Keepalive *KeepaliveFrame
}

type WelcomeFrame struct {
	RunnerID   string
	ServerTime int64
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
