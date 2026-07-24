package protocol

import (
	"encoding/json"
	"errors"
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
		Tenants:      cloneStrings(req.Tenants),
	}
}

func RegisterRequestFromProto(req *runnerpb.RegisterRequest) RegisterRunnerRequest {
	return RegisterRunnerRequest{
		RunnerID:     req.GetRunnerId(),
		Concurrency:  int(req.GetConcurrency()),
		Capabilities: CapabilitiesFromProto(req.GetCapabilities()),
		Labels:       cloneLabels(req.GetLabels()),
		Tenants:      req.GetTenants(),
	}
}

func RegisterResponseToProto(resp RegisterRunnerResponse) *runnerpb.RegisterResponse {
	return &runnerpb.RegisterResponse{
		RunnerId:  resp.RunnerID,
		SessionId: resp.SessionID,
	}
}

func RegisterResponseFromProto(resp *runnerpb.RegisterResponse) RegisterRunnerResponse {
	return RegisterRunnerResponse{
		RunnerID:  resp.GetRunnerId(),
		SessionID: resp.GetSessionId(),
	}
}

func HeartbeatRequestToProto(req HeartbeatRequest) *runnerpb.HeartbeatRequest {
	return &runnerpb.HeartbeatRequest{
		RunnerId:  req.RunnerID,
		Capacity:  int32(req.Capacity),
		InFlight:  int32(req.InFlight),
		Timestamp: req.Timestamp,
		SessionId: req.SessionID,
	}
}

func HeartbeatRequestFromProto(req *runnerpb.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID:  req.GetRunnerId(),
		SessionID: req.GetSessionId(),
		Capacity:  int(req.GetCapacity()),
		InFlight:  int(req.GetInFlight()),
		Timestamp: req.GetTimestamp(),
	}
}

func PollTaskRequestToProto(req PollTaskRequest) *runnerpb.PollTaskRequest {
	return &runnerpb.PollTaskRequest{
		RunnerId:     req.RunnerID,
		SessionId:    req.SessionID,
		Capacity:     int32(req.Capacity),
		Capabilities: CapabilitiesToProto(req.Capabilities),
		Labels:       cloneLabels(req.Labels),
	}
}

func PollTaskRequestFromProto(req *runnerpb.PollTaskRequest) PollTaskRequest {
	return PollTaskRequest{
		RunnerID:     req.GetRunnerId(),
		SessionID:    req.GetSessionId(),
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
		SessionId:  req.SessionID,
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
		RunnerID:  req.GetRunnerId(),
		SessionID: req.GetSessionId(),
		Lease:     lease,
		Result:    result,
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

func cloneStrings(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

func RunnerFrameToProto(f RunnerFrame) (*runnerpb.RunnerFrame, error) {
	switch {
	case f.Hello != nil:
		return &runnerpb.RunnerFrame{Frame: &runnerpb.RunnerFrame_Hello{
			Hello: &runnerpb.HelloFrame{
				RunnerId:     f.Hello.RunnerID,
				Concurrency:  int32(f.Hello.Concurrency),
				Capabilities: CapabilitiesToProto(f.Hello.Capabilities),
				Labels:       cloneLabels(f.Hello.Labels),
				Tenants:      cloneStrings(f.Hello.Tenants),
			},
		}}, nil
	case f.Result != nil:
		leaseJSON, err := marshalLease(f.Result.Lease)
		if err != nil {
			return nil, err
		}
		resultJSON, err := MarshalTaskResult(f.Result.Result)
		if err != nil {
			return nil, err
		}
		return &runnerpb.RunnerFrame{Frame: &runnerpb.RunnerFrame_Result{
			Result: &runnerpb.ResultFrame{
				LeaseId:    f.Result.LeaseID,
				LeaseJson:  leaseJSON,
				ResultJson: resultJSON,
			},
		}}, nil
	case f.Bye != nil:
		return &runnerpb.RunnerFrame{Frame: &runnerpb.RunnerFrame_Bye{Bye: &runnerpb.ByeFrame{}}}, nil
	}
	return nil, errors.New("runner frame: no sub-frame set")
}

func RunnerFrameFromProto(pb *runnerpb.RunnerFrame) (RunnerFrame, error) {
	switch f := pb.GetFrame().(type) {
	case *runnerpb.RunnerFrame_Hello:
		return RunnerFrame{Hello: &HelloFrame{
			RunnerID:     f.Hello.GetRunnerId(),
			Concurrency:  int(f.Hello.GetConcurrency()),
			Capabilities: CapabilitiesFromProto(f.Hello.GetCapabilities()),
			Labels:       cloneLabels(f.Hello.GetLabels()),
			Tenants:      f.Hello.GetTenants(),
		}}, nil
	case *runnerpb.RunnerFrame_Result:
		lease, err := unmarshalLease(f.Result.GetLeaseJson())
		if err != nil {
			return RunnerFrame{}, err
		}
		result, err := UnmarshalTaskResult(f.Result.GetResultJson())
		if err != nil {
			return RunnerFrame{}, err
		}
		return RunnerFrame{Result: &ResultFrame{
			LeaseID: f.Result.GetLeaseId(),
			Lease:   lease,
			Result:  result,
		}}, nil
	case *runnerpb.RunnerFrame_Bye:
		return RunnerFrame{Bye: &ByeFrame{}}, nil
	}
	return RunnerFrame{}, errors.New("runner frame: empty oneof")
}

func ServerFrameToProto(f ServerFrame) (*runnerpb.ServerFrame, error) {
	switch {
	case f.Welcome != nil:
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Welcome{
			Welcome: &runnerpb.WelcomeFrame{RunnerId: f.Welcome.RunnerID, ServerTime: f.Welcome.ServerTime},
		}}, nil
	case f.Task != nil:
		leaseJSON, err := marshalLease(f.Task.Lease)
		if err != nil {
			return nil, err
		}
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Task{
			Task: &runnerpb.TaskFrame{LeaseJson: leaseJSON},
		}}, nil
	case f.Ack != nil:
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Ack{
			Ack: &runnerpb.AckFrame{LeaseId: f.Ack.LeaseID, Accepted: f.Ack.Accepted, Error: f.Ack.Error},
		}}, nil
	case f.Backoff != nil:
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Backoff{
			Backoff: &runnerpb.BackoffFrame{WaitNanos: int64(f.Backoff.Wait)},
		}}, nil
	case f.Keepalive != nil:
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Keepalive{Keepalive: &runnerpb.KeepaliveFrame{}}}, nil
	}
	return nil, errors.New("server frame: no sub-frame set")
}

func ServerFrameFromProto(pb *runnerpb.ServerFrame) (ServerFrame, error) {
	switch f := pb.GetFrame().(type) {
	case *runnerpb.ServerFrame_Welcome:
		return ServerFrame{Welcome: &WelcomeFrame{RunnerID: f.Welcome.GetRunnerId(), ServerTime: f.Welcome.GetServerTime()}}, nil
	case *runnerpb.ServerFrame_Task:
		lease, err := unmarshalLease(f.Task.GetLeaseJson())
		if err != nil {
			return ServerFrame{}, err
		}
		return ServerFrame{Task: &TaskFrame{Lease: lease}}, nil
	case *runnerpb.ServerFrame_Ack:
		return ServerFrame{Ack: &AckFrame{LeaseID: f.Ack.GetLeaseId(), Accepted: f.Ack.GetAccepted(), Error: f.Ack.GetError()}}, nil
	case *runnerpb.ServerFrame_Backoff:
		return ServerFrame{Backoff: &BackoffFrame{Wait: time.Duration(f.Backoff.GetWaitNanos())}}, nil
	case *runnerpb.ServerFrame_Keepalive:
		return ServerFrame{Keepalive: &KeepaliveFrame{}}, nil
	}
	return ServerFrame{}, errors.New("server frame: empty oneof")
}
