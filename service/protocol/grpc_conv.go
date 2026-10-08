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
			Features:    cloneStrings(c.Features),
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
			Features:    cloneStrings(c.GetFeatures()),
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
		Namespaces:   cloneStrings(req.Namespaces),
		Activations:  ActivationInventoryToProto(req.Activations),
		InstanceUid:  req.InstanceUID,
		// Verbatim: control, not the transport, decodes the envelope.
		DescriptorsJson: cloneBytes(req.DescriptorsJSON),
	}
}

func RegisterRequestFromProto(req *runnerpb.RegisterRequest) RegisterRunnerRequest {
	return RegisterRunnerRequest{
		RunnerID:        req.GetRunnerId(),
		Concurrency:     int(req.GetConcurrency()),
		Capabilities:    CapabilitiesFromProto(req.GetCapabilities()),
		Labels:          cloneLabels(req.GetLabels()),
		Namespaces:      req.GetNamespaces(),
		Activations:     ActivationInventoryFromProto(req.GetActivations()),
		InstanceUID:     req.GetInstanceUid(),
		DescriptorsJSON: cloneBytes(req.GetDescriptorsJson()),
	}
}

func ActivationInventoryToProto(items []ActivationInventoryItem) []*runnerpb.ActivationInventoryItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]*runnerpb.ActivationInventoryItem, len(items))
	for i, item := range items {
		out[i] = &runnerpb.ActivationInventoryItem{
			WorkflowId:      item.WorkflowID,
			EntryUnitId:     item.EntryUnitID,
			Generation:      item.Generation,
			WorkflowVersion: item.WorkflowVersion,
			ReplicaIndex:    item.ReplicaIndex,
		}
	}
	return out
}

func ActivationInventoryFromProto(items []*runnerpb.ActivationInventoryItem) []ActivationInventoryItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]ActivationInventoryItem, len(items))
	for i, item := range items {
		out[i] = ActivationInventoryItem{
			WorkflowID:      item.GetWorkflowId(),
			WorkflowVersion: item.GetWorkflowVersion(),
			EntryUnitID:     item.GetEntryUnitId(),
			Generation:      item.GetGeneration(),
			ReplicaIndex:    item.GetReplicaIndex(),
		}
	}
	return out
}

func RunnerControlDirectiveToProto(directive *RunnerControlDirective) *runnerpb.RunnerControlDirective {
	if directive == nil {
		return nil
	}
	return &runnerpb.RunnerControlDirective{
		DesiredState: directive.DesiredState,
		Generation:   directive.Generation,
		RecoveryOnly: directive.RecoveryOnly,
	}
}

func RunnerControlDirectiveFromProto(directive *runnerpb.RunnerControlDirective) *RunnerControlDirective {
	if directive == nil {
		return nil
	}
	return &RunnerControlDirective{
		DesiredState: directive.GetDesiredState(),
		Generation:   directive.GetGeneration(),
		RecoveryOnly: directive.GetRecoveryOnly(),
	}
}

func RunnerDrainObservationToProto(observation *RunnerDrainObservation) *runnerpb.RunnerDrainObservation {
	if observation == nil {
		return nil
	}
	return &runnerpb.RunnerDrainObservation{
		Generation:        observation.Generation,
		RecoveryOnly:      observation.RecoveryOnly,
		ActiveActivations: observation.ActiveActivations,
	}
}

func RunnerDrainObservationFromProto(observation *runnerpb.RunnerDrainObservation) *RunnerDrainObservation {
	if observation == nil {
		return nil
	}
	return &RunnerDrainObservation{
		Generation:        observation.GetGeneration(),
		RecoveryOnly:      observation.GetRecoveryOnly(),
		ActiveActivations: observation.GetActiveActivations(),
	}
}

func RegisterResponseToProto(resp RegisterRunnerResponse) *runnerpb.RegisterResponse {
	return &runnerpb.RegisterResponse{
		RunnerId:  resp.RunnerID,
		SessionId: resp.SessionID,
		Control:   RunnerControlDirectiveToProto(resp.Control),
	}
}

func RegisterResponseFromProto(resp *runnerpb.RegisterResponse) RegisterRunnerResponse {
	return RegisterRunnerResponse{
		RunnerID:  resp.GetRunnerId(),
		SessionID: resp.GetSessionId(),
		Control:   RunnerControlDirectiveFromProto(resp.GetControl()),
	}
}

// HostedActivationsToProto converts the heartbeat carrier. nil stays nil (the
// runner does not report); a non-nil report with an empty list converts to a
// present message with no items, which is the distinct "hosts nothing" signal.
func HostedActivationsToProto(report *HostedActivationsReport) *runnerpb.HostedActivationsReport {
	if report == nil {
		return nil
	}
	return &runnerpb.HostedActivationsReport{
		Activations: ActivationInventoryToProto(report.Activations),
	}
}

func HostedActivationsFromProto(report *runnerpb.HostedActivationsReport) *HostedActivationsReport {
	if report == nil {
		return nil
	}
	return &HostedActivationsReport{
		Activations: ActivationInventoryFromProto(report.GetActivations()),
	}
}

func HeartbeatRequestToProto(req HeartbeatRequest) *runnerpb.HeartbeatRequest {
	return &runnerpb.HeartbeatRequest{
		RunnerId:          req.RunnerID,
		Capacity:          int32(req.Capacity),
		InFlight:          int32(req.InFlight),
		Timestamp:         req.Timestamp,
		SessionId:         req.SessionID,
		SupplyObserved:    cloneLabels(req.SupplyObserved),
		SupplyKeyId:       req.SupplyKeyID,
		DrainObservation:  RunnerDrainObservationToProto(req.DrainObservation),
		HostedActivations: HostedActivationsToProto(req.HostedActivations),
	}
}

func HeartbeatRequestFromProto(req *runnerpb.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		RunnerID:          req.GetRunnerId(),
		SessionID:         req.GetSessionId(),
		Capacity:          int(req.GetCapacity()),
		InFlight:          int(req.GetInFlight()),
		Timestamp:         req.GetTimestamp(),
		SupplyObserved:    cloneLabels(req.GetSupplyObserved()),
		SupplyKeyID:       req.GetSupplyKeyId(),
		DrainObservation:  RunnerDrainObservationFromProto(req.GetDrainObservation()),
		HostedActivations: HostedActivationsFromProto(req.GetHostedActivations()),
	}
}

// HeartbeatResponseToProto converts the Go struct to the proto message.
// Activations are carried as JSON bytes (same pattern as lease_json) because
// ActivateDirective.Params is map[string]any which has no clean proto mapping.
func HeartbeatResponseToProto(resp HeartbeatResponse) (*runnerpb.HeartbeatResponse, error) {
	out := &runnerpb.HeartbeatResponse{
		ServerTime:        resp.ServerTime,
		SupplyHints:       cloneLabels(resp.SupplyHints),
		SupplyKeyRotation: resp.SupplyKeyRotation,
		Control:           RunnerControlDirectiveToProto(resp.Control),
	}
	if resp.Activations != nil {
		data, err := json.Marshal(resp.Activations)
		if err != nil {
			return nil, err
		}
		out.ActivationsJson = data
	}
	return out, nil
}

// HeartbeatResponseFromProto converts the proto message to the Go struct.
func HeartbeatResponseFromProto(resp *runnerpb.HeartbeatResponse) (HeartbeatResponse, error) {
	out := HeartbeatResponse{
		ServerTime:        resp.GetServerTime(),
		SupplyHints:       cloneLabels(resp.GetSupplyHints()),
		SupplyKeyRotation: resp.GetSupplyKeyRotation(),
		Control:           RunnerControlDirectiveFromProto(resp.GetControl()),
	}
	if data := resp.GetActivationsJson(); len(data) > 0 {
		var acts HeartbeatActivations
		if err := json.Unmarshal(data, &acts); err != nil {
			return HeartbeatResponse{}, err
		}
		out.Activations = &acts
	}
	return out, nil
}

func PollTaskRequestToProto(req PollTaskRequest) *runnerpb.PollTaskRequest {
	return &runnerpb.PollTaskRequest{
		RunnerId:       req.RunnerID,
		SessionId:      req.SessionID,
		Capacity:       int32(req.Capacity),
		Capabilities:   CapabilitiesToProto(req.Capabilities),
		Labels:         cloneLabels(req.Labels),
		ActiveLeaseIds: append([]string(nil), req.ActiveLeaseIDs...),
		RecoveryOnly:   req.RecoveryOnly,
	}
}

func PollTaskRequestFromProto(req *runnerpb.PollTaskRequest) PollTaskRequest {
	return PollTaskRequest{
		RunnerID:       req.GetRunnerId(),
		SessionID:      req.GetSessionId(),
		Capacity:       int(req.GetCapacity()),
		Capabilities:   CapabilitiesFromProto(req.GetCapabilities()),
		Labels:         cloneLabels(req.GetLabels()),
		ActiveLeaseIDs: append([]string(nil), req.GetActiveLeaseIds()...),
		RecoveryOnly:   req.GetRecoveryOnly(),
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
		Control:   RunnerControlDirectiveToProto(resp.Control),
	}, nil
}

func PollTaskResponseFromProto(resp *runnerpb.PollTaskResponse) (PollTaskResponse, error) {
	lease, err := unmarshalLease(resp.GetLeaseJson())
	if err != nil {
		return PollTaskResponse{}, err
	}
	return PollTaskResponse{
		Lease:   lease,
		Wait:    time.Duration(resp.GetWaitNanos()),
		Control: RunnerControlDirectiveFromProto(resp.GetControl()),
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
		RunnerId:     req.RunnerID,
		LeaseJson:    leaseJSON,
		ResultJson:   resultJSON,
		SessionId:    req.SessionID,
		TraceCarrier: cloneLabels(req.TraceCarrier),
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
		RunnerID:     req.GetRunnerId(),
		SessionID:    req.GetSessionId(),
		Lease:        lease,
		Result:       result,
		TraceCarrier: cloneLabels(req.GetTraceCarrier()),
	}, nil
}

// ActivationAckRequestToProto converts the HTTP-shaped ActivationAck DTO to its
// gRPC wire message. AuthToken is deliberately NOT carried onto the proto
// message: like every other request, the token travels in gRPC metadata (see
// GRPCClient.withAuth / overrideTokenFromMetadata), never in the message body.
func ActivationAckRequestToProto(ack ActivationAck) *runnerpb.ActivationAckRequest {
	return &runnerpb.ActivationAckRequest{
		RunnerId:        ack.RunnerID,
		SessionId:       ack.SessionID,
		WorkflowId:      ack.WorkflowID,
		WorkflowVersion: ack.WorkflowVersion,
		GroupId:         ack.GroupID,
		ReplicaIndex:    ack.ReplicaIndex,
		Generation:      ack.Generation,
		Status:          string(ack.Status),
		Error:           ack.Error,
	}
}

// ActivationAckRequestFromProto converts a gRPC ActivationAckRequest back to
// the transport-agnostic ActivationAck DTO Core.activationAck consumes.
// AuthToken is left empty; the gRPC server fills it from metadata the same way
// the HTTP handler fills it from the Authorization header.
func ActivationAckRequestFromProto(req *runnerpb.ActivationAckRequest) ActivationAck {
	return ActivationAck{
		RunnerID:        req.GetRunnerId(),
		SessionID:       req.GetSessionId(),
		WorkflowID:      req.GetWorkflowId(),
		WorkflowVersion: req.GetWorkflowVersion(),
		GroupID:         req.GetGroupId(),
		ReplicaIndex:    req.GetReplicaIndex(),
		Generation:      req.GetGeneration(),
		Status:          ActivationStatus(req.GetStatus()),
		Error:           req.GetError(),
	}
}

// RenewLeaseRequestToProto converts the HTTP-shaped RenewLeaseRequest DTO to
// its gRPC wire message. AuthToken travels on the message for DTO parity, but
// (like every other request) the gRPC server ignores it in favor of the
// Authorization metadata set by GRPCClient.withAuth.
func RenewLeaseRequestToProto(req RenewLeaseRequest) *runnerpb.RenewLeaseRequest {
	return &runnerpb.RenewLeaseRequest{
		RunnerId:   req.RunnerID,
		SessionId:  req.SessionID,
		LeaseId:    req.LeaseID,
		LeaseToken: req.LeaseToken,
		ExtendMs:   req.Extend,
		AuthToken:  req.AuthToken,
	}
}

// RenewLeaseRequestFromProto converts a gRPC RenewLeaseRequest back to the
// transport-agnostic DTO Core.renewLease consumes. AuthToken is left empty;
// the gRPC server fills it from metadata the same way the HTTP handler fills
// it from the Authorization header.
func RenewLeaseRequestFromProto(req *runnerpb.RenewLeaseRequest) RenewLeaseRequest {
	return RenewLeaseRequest{
		RunnerID:   req.GetRunnerId(),
		SessionID:  req.GetSessionId(),
		LeaseID:    req.GetLeaseId(),
		LeaseToken: req.GetLeaseToken(),
		Extend:     req.GetExtendMs(),
	}
}

// RenewLeaseResponseToProto converts the HTTP-shaped RenewLeaseResponse DTO
// to its gRPC wire message. Deadline travels as UnixNano (same pattern as
// HeartbeatRequest.Timestamp) to avoid a proto Timestamp import; a zero
// time.Time (the refusal case) encodes as 0.
func RenewLeaseResponseToProto(resp RenewLeaseResponse) *runnerpb.RenewLeaseResponse {
	var deadline int64
	if !resp.Deadline.IsZero() {
		deadline = resp.Deadline.UnixNano()
	}
	return &runnerpb.RenewLeaseResponse{
		Renewed:          resp.Renewed,
		DeadlineUnixNano: deadline,
		Error:            resp.Error,
	}
}

// RenewLeaseResponseFromProto converts a gRPC RenewLeaseResponse back to the
// transport-agnostic DTO. A zero deadline_unix_nano decodes to the zero
// time.Time, matching the HTTP transport's refusal shape.
func RenewLeaseResponseFromProto(resp *runnerpb.RenewLeaseResponse) RenewLeaseResponse {
	out := RenewLeaseResponse{
		Renewed: resp.GetRenewed(),
		Error:   resp.GetError(),
	}
	if n := resp.GetDeadlineUnixNano(); n != 0 {
		out.Deadline = time.Unix(0, n).UTC()
	}
	return out
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

// cloneBytes copies src so a converted DTO does not alias the source message's
// buffer. Empty input stays empty (nil) in both directions.
func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	return append([]byte(nil), src...)
}

func RunnerFrameToProto(f RunnerFrame) (*runnerpb.RunnerFrame, error) {
	switch {
	case f.Hello != nil:
		return &runnerpb.RunnerFrame{Frame: &runnerpb.RunnerFrame_Hello{
			Hello: &runnerpb.HelloFrame{
				RunnerId:        f.Hello.RunnerID,
				Concurrency:     int32(f.Hello.Concurrency),
				Capabilities:    CapabilitiesToProto(f.Hello.Capabilities),
				Labels:          cloneLabels(f.Hello.Labels),
				Namespaces:      cloneStrings(f.Hello.Namespaces),
				InstanceUid:     f.Hello.InstanceUID,
				DescriptorsJson: cloneBytes(f.Hello.DescriptorsJSON),
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
	case f.ControlObservation != nil:
		return &runnerpb.RunnerFrame{Frame: &runnerpb.RunnerFrame_ControlObservation{
			ControlObservation: &runnerpb.ControlObservationFrame{
				Generation:        f.ControlObservation.Generation,
				RecoveryOnly:      f.ControlObservation.RecoveryOnly,
				ActiveWorkers:     f.ControlObservation.ActiveWorkers,
				ActiveActivations: f.ControlObservation.ActiveActivations,
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
			RunnerID:        f.Hello.GetRunnerId(),
			Concurrency:     int(f.Hello.GetConcurrency()),
			Capabilities:    CapabilitiesFromProto(f.Hello.GetCapabilities()),
			Labels:          cloneLabels(f.Hello.GetLabels()),
			Namespaces:      f.Hello.GetNamespaces(),
			InstanceUID:     f.Hello.GetInstanceUid(),
			DescriptorsJSON: cloneBytes(f.Hello.GetDescriptorsJson()),
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
	case *runnerpb.RunnerFrame_ControlObservation:
		return RunnerFrame{ControlObservation: &ControlObservationFrame{
			Generation:        f.ControlObservation.GetGeneration(),
			RecoveryOnly:      f.ControlObservation.GetRecoveryOnly(),
			ActiveWorkers:     f.ControlObservation.GetActiveWorkers(),
			ActiveActivations: f.ControlObservation.GetActiveActivations(),
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
			Welcome: &runnerpb.WelcomeFrame{
				RunnerId:   f.Welcome.RunnerID,
				ServerTime: f.Welcome.ServerTime,
				Control:    RunnerControlDirectiveToProto(f.Welcome.Control),
			},
		}}, nil
	case f.Control != nil:
		return &runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Control{
			Control: &runnerpb.ControlFrame{Directive: RunnerControlDirectiveToProto(f.Control.Directive)},
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
		return ServerFrame{Welcome: &WelcomeFrame{
			RunnerID:   f.Welcome.GetRunnerId(),
			ServerTime: f.Welcome.GetServerTime(),
			Control:    RunnerControlDirectiveFromProto(f.Welcome.GetControl()),
		}}, nil
	case *runnerpb.ServerFrame_Control:
		return ServerFrame{Control: &ControlFrame{
			Directive: RunnerControlDirectiveFromProto(f.Control.GetDirective()),
		}}, nil
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
