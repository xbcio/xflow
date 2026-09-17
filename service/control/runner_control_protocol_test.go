package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestCoreProjectsRunnerControlOnRegisterHeartbeatAndPoll(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	core := &Core{runners: directory, auth: DisabledAuthenticator{}}

	registered, err := core.register(ctx, protocol.RegisterRunnerRequest{RunnerID: "runner-a", Concurrency: 1}, TransportInfo{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if registered.Control == nil || registered.Control.DesiredState != string(RunnerDesiredStateActive) {
		t.Fatalf("register control = %#v, want active", registered.Control)
	}
	drain, err := directory.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-a", "hash-a", RunnerDesiredStateDraining))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}

	heartbeat, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: "runner-a", SessionID: registered.SessionID, Capacity: 1, InFlight: 0,
		DrainObservation: &protocol.RunnerDrainObservation{
			Generation: drain.Generation, RecoveryOnly: true, ActiveActivations: 0,
		},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if heartbeat.Control == nil || heartbeat.Control.DesiredState != string(RunnerDesiredStateDraining) || !heartbeat.Control.RecoveryOnly {
		t.Fatalf("heartbeat control = %#v, want draining recovery-only", heartbeat.Control)
	}
	current, found, err := directory.RunnerControl(ctx, "runner-a")
	if err != nil || !found || current.Drain == nil || !current.Drain.ServerQuiescent || !current.Drain.RunnerQuiescent || current.Drain.Phase != RunnerDrainPhaseComplete {
		t.Fatalf("control after core heartbeat = %+v, found=%v, err=%v; want complete", current, found, err)
	}

	poll, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID: "runner-a", SessionID: registered.SessionID, RecoveryOnly: true,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if poll.Lease != nil || poll.Control == nil || poll.Control.DesiredState != string(RunnerDesiredStateDraining) || !poll.Control.RecoveryOnly {
		t.Fatalf("poll = %#v, want no lease plus draining recovery-only control", poll)
	}
}
