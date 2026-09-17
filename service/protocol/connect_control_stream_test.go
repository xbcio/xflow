package protocol

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

func TestConnectControlFrameConversions(t *testing.T) {
	directive := &RunnerControlDirective{DesiredState: "draining", Generation: 7, RecoveryOnly: true}

	welcomePB, err := ServerFrameToProto(ServerFrame{Welcome: &WelcomeFrame{
		RunnerID: "runner-a", ServerTime: 42, Control: directive,
	}})
	if err != nil {
		t.Fatalf("WelcomeFrame to proto: %v", err)
	}
	welcome, err := ServerFrameFromProto(welcomePB)
	if err != nil {
		t.Fatalf("WelcomeFrame from proto: %v", err)
	}
	if welcome.Welcome == nil || welcome.Welcome.Control == nil || *welcome.Welcome.Control != *directive {
		t.Fatalf("welcome control = %#v, want %#v", welcome.Welcome, directive)
	}

	controlPB, err := ServerFrameToProto(ServerFrame{Control: &ControlFrame{Directive: directive}})
	if err != nil {
		t.Fatalf("ControlFrame to proto: %v", err)
	}
	control, err := ServerFrameFromProto(controlPB)
	if err != nil {
		t.Fatalf("ControlFrame from proto: %v", err)
	}
	if control.Control == nil || control.Control.Directive == nil || *control.Control.Directive != *directive {
		t.Fatalf("control frame = %#v, want %#v", control.Control, directive)
	}

	observation := &ControlObservationFrame{
		Generation: 7, RecoveryOnly: true, ActiveWorkers: 2, ActiveActivations: 3,
	}
	runnerPB, err := RunnerFrameToProto(RunnerFrame{ControlObservation: observation})
	if err != nil {
		t.Fatalf("ControlObservationFrame to proto: %v", err)
	}
	if got := runnerPB.GetControlObservation(); got == nil || got.GetGeneration() != observation.Generation ||
		!got.GetRecoveryOnly() || got.GetActiveWorkers() != observation.ActiveWorkers ||
		got.GetActiveActivations() != observation.ActiveActivations {
		t.Fatalf("control observation proto = %#v, want %#v", got, observation)
	}
	runner, err := RunnerFrameFromProto(runnerPB)
	if err != nil {
		t.Fatalf("ControlObservationFrame from proto: %v", err)
	}
	if runner.ControlObservation == nil || *runner.ControlObservation != *observation {
		t.Fatalf("control observation = %#v, want %#v", runner.ControlObservation, observation)
	}

	// A pre-control server sends the historical two-field WelcomeFrame. Its
	// absent control projection must remain distinguishable from a directive.
	legacy, err := ServerFrameFromProto(&runnerpb.ServerFrame{Frame: &runnerpb.ServerFrame_Welcome{
		Welcome: &runnerpb.WelcomeFrame{RunnerId: "runner-a", ServerTime: 42},
	}})
	if err != nil {
		t.Fatalf("legacy WelcomeFrame from proto: %v", err)
	}
	if legacy.Welcome == nil || legacy.Welcome.Control != nil {
		t.Fatalf("legacy welcome = %#v, want nil control", legacy.Welcome)
	}
}

func TestGRPCConnectCarriesRunnerControlFrames(t *testing.T) {
	const bufferSize = 1 << 20
	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	defer server.Stop()
	defer func() { _ = listener.Close() }()

	observations := make(chan ControlObservationFrame, 1)
	handler := RunnerConnectHandlerFunc(func(stream RunnerConnectStream) error {
		first, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive hello: %w", err)
		}
		if first.Hello == nil || first.Hello.RunnerID != "runner-a" {
			return fmt.Errorf("first Connect frame = %#v, want runner-a hello", first)
		}

		initial := &RunnerControlDirective{DesiredState: "draining", Generation: 8, RecoveryOnly: true}
		if err := stream.Send(ServerFrame{Welcome: &WelcomeFrame{
			RunnerID: "runner-a", ServerTime: 100, Control: initial,
		}}); err != nil {
			return fmt.Errorf("send welcome: %w", err)
		}
		if err := stream.Send(ServerFrame{Control: &ControlFrame{Directive: &RunnerControlDirective{
			DesiredState: "active", Generation: 9,
		}}}); err != nil {
			return fmt.Errorf("send control: %w", err)
		}

		next, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive control observation: %w", err)
		}
		if next.ControlObservation == nil {
			return fmt.Errorf("second Connect frame = %#v, want control observation", next)
		}
		observations <- *next.ControlObservation
		return nil
	})
	runnerpb.RegisterRunnerProtocolServer(server, &connectControlTestServer{handler: handler})
	go func() { _ = server.Serve(listener) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := NewGRPCClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(RunnerFrame{Hello: &HelloFrame{RunnerID: "runner-a", Concurrency: 1}}); err != nil {
		t.Fatalf("send hello: %v", err)
	}

	welcome, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive welcome: %v", err)
	}
	if welcome.Welcome == nil || welcome.Welcome.Control == nil ||
		welcome.Welcome.Control.DesiredState != "draining" || welcome.Welcome.Control.Generation != 8 ||
		!welcome.Welcome.Control.RecoveryOnly {
		t.Fatalf("welcome = %#v, want draining generation 8", welcome)
	}

	control, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive control: %v", err)
	}
	if control.Control == nil || control.Control.Directive == nil ||
		control.Control.Directive.DesiredState != "active" || control.Control.Directive.Generation != 9 ||
		control.Control.Directive.RecoveryOnly {
		t.Fatalf("control = %#v, want active generation 9", control)
	}

	wantObservation := ControlObservationFrame{
		Generation: 8, RecoveryOnly: true, ActiveWorkers: 2, ActiveActivations: 3,
	}
	if err := stream.Send(RunnerFrame{ControlObservation: &wantObservation}); err != nil {
		t.Fatalf("send control observation: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case got := <-observations:
		if got != wantObservation {
			t.Fatalf("control observation = %#v, want %#v", got, wantObservation)
		}
	case <-ctx.Done():
		t.Fatal("server did not receive control observation")
	}
}

type connectControlTestServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	handler RunnerConnectHandler
}

func (s *connectControlTestServer) Connect(stream runnerpb.RunnerProtocol_ConnectServer) error {
	return ServeGRPCConnect(stream, s.handler)
}
