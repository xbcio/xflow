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
	RunnerID string `json:"runner_id"`
}

type HeartbeatRequest struct {
	RunnerID  string `json:"runner_id"`
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
	Lease     *engine.TaskLease `json:"lease"`
	Result    engine.TaskResult `json:"result"`
	AuthToken string            `json:"auth_token,omitempty"`
}

type reportResultRequestJSON struct {
	RunnerID  string            `json:"runner_id"`
	Lease     *engine.TaskLease `json:"lease"`
	Result    json.RawMessage   `json:"result"`
	AuthToken string            `json:"auth_token,omitempty"`
}

type taskResultJSON struct {
	Output  *types.Output      `json:"output,omitempty"`
	Suspend *types.SuspendSpec `json:"suspend,omitempty"`
	Error   string             `json:"error,omitempty"`
}

func (r ReportResultRequest) MarshalJSON() ([]byte, error) {
	resultJSON, err := MarshalTaskResult(r.Result)
	if err != nil {
		return nil, err
	}
	return json.Marshal(reportResultRequestJSON{
		RunnerID:  r.RunnerID,
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

// MarshalTaskResult encodes a task result to JSON using the protocol's
// error-as-string convention. It is the single source of truth for result
// serialization shared by the HTTP and gRPC transports.
func MarshalTaskResult(result engine.TaskResult) ([]byte, error) {
	out := taskResultJSON{
		Output:  result.Output,
		Suspend: result.Suspend,
	}
	if result.Error != nil {
		out.Error = result.Error.Error()
	}
	return json.Marshal(out)
}

// UnmarshalTaskResult decodes a task result produced by MarshalTaskResult.
func UnmarshalTaskResult(data []byte) (engine.TaskResult, error) {
	var in taskResultJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return engine.TaskResult{}, err
	}
	result := engine.TaskResult{
		Output:  in.Output,
		Suspend: in.Suspend,
	}
	if in.Error != "" {
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
