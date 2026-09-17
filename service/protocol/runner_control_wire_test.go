package protocol

import "testing"

func TestRunnerControlDirectiveGRPCRoundTrips(t *testing.T) {
	want := &RunnerControlDirective{DesiredState: "draining", Generation: 9, RecoveryOnly: true}
	wantObservation := &RunnerDrainObservation{Generation: 9, RecoveryOnly: true, ActiveActivations: 2}

	request := HeartbeatRequest{RunnerID: "runner-a", SessionID: "session-a", DrainObservation: wantObservation}
	if got := HeartbeatRequestFromProto(HeartbeatRequestToProto(request)); got.DrainObservation == nil || *got.DrainObservation != *wantObservation {
		t.Fatalf("heartbeat drain observation = %#v, want %#v", got.DrainObservation, wantObservation)
	}
	if got := HeartbeatRequestFromProto(HeartbeatRequestToProto(HeartbeatRequest{RunnerID: "runner-a"})); got.DrainObservation != nil {
		t.Fatalf("nil heartbeat drain observation round trip = %#v, want nil", got.DrainObservation)
	}

	register := RegisterResponseFromProto(RegisterResponseToProto(RegisterRunnerResponse{
		RunnerID: "runner-a", SessionID: "session-a", Control: want,
	}))
	if register.Control == nil || *register.Control != *want {
		t.Fatalf("register control = %#v, want %#v", register.Control, want)
	}

	heartbeatPB, err := HeartbeatResponseToProto(HeartbeatResponse{Control: want})
	if err != nil {
		t.Fatalf("HeartbeatResponseToProto: %v", err)
	}
	heartbeat, err := HeartbeatResponseFromProto(heartbeatPB)
	if err != nil {
		t.Fatalf("HeartbeatResponseFromProto: %v", err)
	}
	if heartbeat.Control == nil || *heartbeat.Control != *want {
		t.Fatalf("heartbeat control = %#v, want %#v", heartbeat.Control, want)
	}

	pollRequest := PollTaskRequest{RunnerID: "runner-a", SessionID: "session-a", RecoveryOnly: true}
	if got := PollTaskRequestFromProto(PollTaskRequestToProto(pollRequest)); !got.RecoveryOnly {
		t.Fatalf("poll recovery_only = false, want true")
	}
	pollPB, err := PollTaskResponseToProto(PollTaskResponse{Control: want})
	if err != nil {
		t.Fatalf("PollTaskResponseToProto: %v", err)
	}
	poll, err := PollTaskResponseFromProto(pollPB)
	if err != nil {
		t.Fatalf("PollTaskResponseFromProto: %v", err)
	}
	if poll.Control == nil || *poll.Control != *want {
		t.Fatalf("poll control = %#v, want %#v", poll.Control, want)
	}
}
