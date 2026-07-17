package protocol

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type Capability struct {
	NodeType    string `json:"node_type"`
	NodeVersion int    `json:"node_version,omitempty"`
}

type RegisterRunnerRequest struct {
	RunnerID     string            `json:"runner_id"`
	Concurrency  int               `json:"concurrency"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities []Capability      `json:"capabilities"`
	// AuthToken is the runner's bearer token. Preferred: Authorization
	// header. This body field is a fallback for transports that can't set
	// headers.
	AuthToken string `json:"auth_token,omitempty"`
}

type RegisterRunnerResponse struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
}

type HeartbeatRequest struct {
	RunnerID  string `json:"runner_id"`
	SessionID string `json:"session_id"`
	Capacity  int    `json:"capacity"`
	InFlight  int    `json:"in_flight"`
	Timestamp int64  `json:"timestamp"`
	AuthToken string `json:"auth_token,omitempty"`
}

type HeartbeatResponse struct {
	ServerTime int64 `json:"server_time"`
}

type PollTaskRequest struct {
	RunnerID     string            `json:"runner_id"`
	SessionID    string            `json:"session_id"`
	Capacity     int               `json:"capacity"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities []Capability      `json:"capabilities"`
	AuthToken    string            `json:"auth_token,omitempty"`
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
}

type reportResultRequestJSON struct {
	RunnerID  string            `json:"runner_id"`
	SessionID string            `json:"session_id"`
	Lease     *engine.TaskLease `json:"lease"`
	Result    json.RawMessage   `json:"result"`
	AuthToken string            `json:"auth_token,omitempty"`
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
		RunnerID:  r.RunnerID,
		SessionID: r.SessionID,
		Lease:     r.Lease,
		Result:    resultJSON,
		AuthToken: r.AuthToken,
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
