package protocol

import (
	"time"

	"github.com/xbcio/xflow/engine"
)

type Capability struct {
	NodeType    string `json:"node_type"`
	NodeVersion int    `json:"node_version,omitempty"`
}

type RegisterRunnerRequest struct {
	RunnerID     string       `json:"runner_id"`
	Concurrency  int          `json:"concurrency"`
	Capabilities []Capability `json:"capabilities"`
}

type RegisterRunnerResponse struct {
	RunnerID string `json:"runner_id"`
}

type HeartbeatRequest struct {
	RunnerID  string `json:"runner_id"`
	Capacity  int    `json:"capacity"`
	InFlight  int    `json:"in_flight"`
	Timestamp int64  `json:"timestamp"`
}

type HeartbeatResponse struct {
	ServerTime int64 `json:"server_time"`
}

type PollTaskRequest struct {
	RunnerID     string       `json:"runner_id"`
	Capacity     int          `json:"capacity"`
	Capabilities []Capability `json:"capabilities"`
}

type PollTaskResponse struct {
	Lease *engine.TaskLease `json:"lease,omitempty"`
	Wait  time.Duration     `json:"wait"`
}

type ReportResultRequest struct {
	RunnerID string            `json:"runner_id"`
	Lease    *engine.TaskLease `json:"lease"`
	Result   engine.TaskResult `json:"result"`
}

type ReportResultResponse struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
}
