package protocol

import (
	"encoding/json"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// Converters between protocol DTOs and generated gRPC messages. Rich domain
// payloads (TaskLease, TaskResult) travel as JSON bytes; scalar control fields
// map field-by-field. Shared by the gRPC client (this package) and the gRPC
// server (service/control).

func CapabilitiesToProto(capabilities []Capability) []*runnerpb.Capability {
	if len(capabilities) == 0 {
		return nil
	}
	out := make([]*runnerpb.Capability, len(capabilities))
	for i, c := range capabilities {
		out[i] = &runnerpb.Capability{
			NodeType:    c.NodeType,
			NodeVersion: int32(c.NodeVersion),
		}
	}
	return out
}

func CapabilitiesFromProto(capabilities []*runnerpb.Capability) []Capability {
	if len(capabilities) == 0 {
		return nil
	}
	out := make([]Capability, len(capabilities))
	for i, c := range capabilities {
		out[i] = Capability{
			NodeType:    c.GetNodeType(),
			NodeVersion: int(c.GetNodeVersion()),
		}
	}
	return out
}

func marshalLease(lease *engine.TaskLease) ([]byte, error) {
	if lease == nil {
		return nil, nil
	}
	return json.Marshal(lease)
}

func unmarshalLease(data []byte) (*engine.TaskLease, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var lease engine.TaskLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

// RegisterRequestToProto and the From/To pairs below convert whole request and
// response DTOs. They keep the JSON-bytes boundary in one place so transports
// never re-implement payload encoding.

func RegisterRequestToProto(req RegisterRunnerRequest) *runnerpb.RegisterRequest {
	return &runnerpb.RegisterRequest{
		RunnerId:     req.RunnerID,
		Concurrency:  int32(req.Concurrency),
		Capabilities: CapabilitiesToProto(req.Capabilities),
		Labels:       cloneLabels(req.Labels),
	}
}

func RegisterRequestFromProto(req *runnerpb.RegisterRequest) RegisterRunnerRequest {
	return RegisterRunnerRequest{
		RunnerID:     req.GetRunnerId(),
		Concurrency:  int(req.GetConcurrency()),
		Capabilities: CapabilitiesFromProto(req.GetCapabilities()),
		Labels:       cloneLabels(req.GetLabels()),
	}
}

func HeartbeatRequestToProto(req HeartbeatRequest) *runnerpb.HeartbeatRequest {
	return &runnerpb.HeartbeatRequest{
		RunnerId:  req.RunnerID,
		Capacity:  int32(req.Capacity),
		InFlight:  int32(req.InFlight),
		Timestamp: req.Timestamp,
	}
}

func HeartbeatRequestFromProto(req *runnerpb.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID:  req.GetRunnerId(),
		Capacity:  int(req.GetCapacity()),
		InFlight:  int(req.GetInFlight()),
		Timestamp: req.GetTimestamp(),
	}
}

func PollTaskRequestToProto(req PollTaskRequest) *runnerpb.PollTaskRequest {
	return &runnerpb.PollTaskRequest{
		RunnerId:     req.RunnerID,
		Capacity:     int32(req.Capacity),
		Capabilities: CapabilitiesToProto(req.Capabilities),
		Labels:       cloneLabels(req.Labels),
	}
}

func PollTaskRequestFromProto(req *runnerpb.PollTaskRequest) PollTaskRequest {
	return PollTaskRequest{
		RunnerID:     req.GetRunnerId(),
		Capacity:     int(req.GetCapacity()),
		Capabilities: CapabilitiesFromProto(req.GetCapabilities()),
		Labels:       cloneLabels(req.GetLabels()),
	}
}

func PollTaskResponseToProto(resp PollTaskResponse) (*runnerpb.PollTaskResponse, error) {
	leaseJSON, err := marshalLease(resp.Lease)
	if err != nil {
		return nil, err
	}
	return &runnerpb.PollTaskResponse{
		LeaseJson: leaseJSON,
		WaitNanos: int64(resp.Wait),
	}, nil
}

func PollTaskResponseFromProto(resp *runnerpb.PollTaskResponse) (PollTaskResponse, error) {
	lease, err := unmarshalLease(resp.GetLeaseJson())
	if err != nil {
		return PollTaskResponse{}, err
	}
	return PollTaskResponse{
		Lease: lease,
		Wait:  time.Duration(resp.GetWaitNanos()),
	}, nil
}

func ReportResultRequestToProto(req ReportResultRequest) (*runnerpb.ReportResultRequest, error) {
	leaseJSON, err := marshalLease(req.Lease)
	if err != nil {
		return nil, err
	}
	resultJSON, err := MarshalTaskResult(req.Result)
	if err != nil {
		return nil, err
	}
	return &runnerpb.ReportResultRequest{
		RunnerId:   req.RunnerID,
		LeaseJson:  leaseJSON,
		ResultJson: resultJSON,
	}, nil
}

func ReportResultRequestFromProto(req *runnerpb.ReportResultRequest) (ReportResultRequest, error) {
	lease, err := unmarshalLease(req.GetLeaseJson())
	if err != nil {
		return ReportResultRequest{}, err
	}
	result, err := UnmarshalTaskResult(req.GetResultJson())
	if err != nil {
		return ReportResultRequest{}, err
	}
	return ReportResultRequest{
		RunnerID: req.GetRunnerId(),
		Lease:    lease,
		Result:   result,
	}, nil
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}
	return out
}
