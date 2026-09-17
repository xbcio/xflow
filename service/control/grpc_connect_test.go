package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

func TestGRPCConnectProjectsControlAndObservations(t *testing.T) {
	directory := NewMemoryRunnerDirectory()
	client := startGRPCTestServer(t, &fakeControlEngine{}, directory)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := stream.Send(protocol.RunnerFrame{Hello: &protocol.HelloFrame{
		RunnerID:    "runner-connect",
		Concurrency: 2,
	}}); err != nil {
		t.Fatalf("send HELLO: %v", err)
	}

	welcome, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive WELCOME: %v", err)
	}
	if welcome.Welcome == nil {
		t.Fatalf("first server frame = %#v, want WELCOME", welcome)
	}
	if welcome.Welcome.RunnerID != "runner-connect" {
		t.Fatalf("welcome runner id = %q, want runner-connect", welcome.Welcome.RunnerID)
	}
	if directive := welcome.Welcome.Control; directive == nil ||
		directive.DesiredState != string(RunnerDesiredStateActive) || directive.Generation != 0 || directive.RecoveryOnly {
		t.Fatalf("welcome directive = %#v, want active generation 0", directive)
	}
	if registered, found := directory.Runner(ctx, "runner-connect"); !found || registered.Capacity != 2 {
		t.Fatalf("HELLO registration = %#v, found=%v, want capacity 2", registered, found)
	}

	drain, err := directory.SetRunnerControl(ctx, RunnerControlRequest{
		RunnerID:     "runner-connect",
		DesiredState: RunnerDesiredStateDraining,
		Actor:        "operator-a",
		Action:       "drain",
		Reason:       "maintenance",
		RequestID:    "connect-drain",
		RequestHash:  "connect-drain-hash",
		Now:          time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SetRunnerControl(drain) error = %v", err)
	}

	control, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive CONTROL: %v", err)
	}
	if control.Control == nil || control.Control.Directive == nil {
		t.Fatalf("server frame = %#v, want CONTROL", control)
	}
	if directive := control.Control.Directive; directive.DesiredState != string(RunnerDesiredStateDraining) ||
		directive.Generation != drain.Generation || !directive.RecoveryOnly {
		t.Fatalf("control directive = %#v, want draining generation %d", directive, drain.Generation)
	}

	if err := stream.Send(protocol.RunnerFrame{ControlObservation: &protocol.ControlObservationFrame{
		Generation:    drain.Generation,
		RecoveryOnly:  true,
		ActiveWorkers: 2,
	}}); err != nil {
		t.Fatalf("send active-worker control observation: %v", err)
	}
	waitForGRPCConnectObservation(t, ctx, directory, "runner-connect", 2, false)

	if err := stream.Send(protocol.RunnerFrame{ControlObservation: &protocol.ControlObservationFrame{
		Generation:        drain.Generation,
		RecoveryOnly:      true,
		ActiveActivations: 3,
	}}); err != nil {
		t.Fatalf("send active-activation control observation: %v", err)
	}
	waitForGRPCConnectObservation(t, ctx, directory, "runner-connect", 0, false)

	if err := stream.Send(protocol.RunnerFrame{ControlObservation: &protocol.ControlObservationFrame{
		Generation:   drain.Generation,
		RecoveryOnly: true,
	}}); err != nil {
		t.Fatalf("send quiescent control observation: %v", err)
	}
	waitForGRPCConnectObservation(t, ctx, directory, "runner-connect", 0, true)

	if err := stream.Send(protocol.RunnerFrame{Bye: &protocol.ByeFrame{}}); err != nil {
		t.Fatalf("send BYE: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

func waitForGRPCConnectObservation(t *testing.T, ctx context.Context, directory *MemoryRunnerDirectory, runnerID string, wantInFlight int, wantQuiescent bool) {
	t.Helper()
	for {
		runner, registered := directory.Runner(ctx, runnerID)
		control, found, err := directory.RunnerControl(ctx, runnerID)
		if err != nil {
			t.Fatalf("RunnerControl() error = %v", err)
		}
		if registered && found && runner.InFlight == wantInFlight && control.Drain != nil &&
			control.Drain.RunnerQuiescent == wantQuiescent {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("control observation was not reflected before deadline: runner=%#v registered=%v control=%#v found=%v", runner, registered, control, found)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
