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

type reportResultRequestJSON struct {
	RunnerID string            `json:"runner_id"`
	Lease    *engine.TaskLease `json:"lease"`
	Result   taskResultJSON    `json:"result"`
}

type taskResultJSON struct {
	Output  *types.Output      `json:"output,omitempty"`
	Suspend *types.SuspendSpec `json:"suspend,omitempty"`
	Error   string             `json:"error,omitempty"`
}

func (r ReportResultRequest) MarshalJSON() ([]byte, error) {
	out := reportResultRequestJSON{
		RunnerID: r.RunnerID,
		Lease:    r.Lease,
		Result: taskResultJSON{
			Output:  r.Result.Output,
			Suspend: r.Result.Suspend,
		},
	}
	if r.Result.Error != nil {
		out.Result.Error = r.Result.Error.Error()
	}
	return json.Marshal(out)
}

func (r *ReportResultRequest) UnmarshalJSON(data []byte) error {
	var in reportResultRequestJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	r.RunnerID = in.RunnerID
	r.Lease = in.Lease
	r.Result = engine.TaskResult{
		Output:  in.Result.Output,
		Suspend: in.Result.Suspend,
	}
	if in.Result.Error != "" {
		r.Result.Error = errors.New(in.Result.Error)
	}
	return nil
}

type ReportResultResponse struct {
	Accepted bool   `json:"accepted"`
	Error    string `json:"error,omitempty"`
}
