package control

import (
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// grpcConnectControlPollInterval is a best-effort delivery backstop for runner
// directories. RunnerControlDirectory deliberately has no transport-specific
// subscription API, so an established Connect stream observes changes here.
const grpcConnectControlPollInterval = 100 * time.Millisecond

// Connect serves the runner's bidirectional protocol through the protocol
// package's transport-neutral gRPC adapter. The control plane retains the
// registration, authentication, session-fencing, and directory policy here.
func (s *GRPCServer) Connect(stream runnerpb.RunnerProtocol_ConnectServer) error {
	return protocol.ServeGRPCConnect(stream, protocol.RunnerConnectHandlerFunc(s.connect))
}

// connect performs the control-plane side of a Connect stream. After the
// initial synchronous HELLO handshake, the receive goroutine never calls Send:
// the loop below is the sole server-to-runner sender, which keeps gRPC Send
// calls serialized while still allowing directive polling to progress.
func (s *GRPCServer) connect(stream protocol.RunnerConnectStream) error {
	ctx := stream.Context()
	var authToken string
	overrideTokenFromMetadata(ctx, &authToken)
	transport := grpcTransportInfo(ctx)

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "first Connect frame must be HELLO")
		}
		return err
	}
	if first.Hello == nil {
		return status.Error(codes.InvalidArgument, "first Connect frame must be HELLO")
	}

	hello := first.Hello
	registered, err := s.core.register(ctx, protocol.RegisterRunnerRequest{
		RunnerID:     hello.RunnerID,
		Concurrency:  hello.Concurrency,
		Capabilities: hello.Capabilities,
		Labels:       hello.Labels,
		Namespaces:   hello.Namespaces,
		AuthToken:    authToken,
	}, transport)
	if err != nil {
		return runnerStatus(err)
	}
	if err := stream.Send(protocol.ServerFrame{Welcome: &protocol.WelcomeFrame{
		RunnerID:   registered.RunnerID,
		ServerTime: time.Now().Unix(),
		Control:    registered.Control,
	}}); err != nil {
		return err
	}

	incoming := make(chan grpcConnectInbound, 1)
	done := make(chan struct{})
	defer close(done)
	go receiveGRPCConnectFrames(stream, incoming, done)

	lastDirective := registered.Control
	sendDirective := func(directive *protocol.RunnerControlDirective) error {
		if directive == nil || sameRunnerControlDirective(lastDirective, directive) {
			return nil
		}
		if err := stream.Send(protocol.ServerFrame{Control: &protocol.ControlFrame{Directive: directive}}); err != nil {
			return err
		}
		lastDirective = directive
		return nil
	}

	ticker := time.NewTicker(grpcConnectControlPollInterval)
	defer ticker.Stop()
	for {
		select {
		case received := <-incoming:
			if received.err != nil {
				if errors.Is(received.err, io.EOF) {
					return nil
				}
				return received.err
			}

			switch frame := received.frame; {
			case frame.Bye != nil:
				return nil
			case frame.Hello != nil:
				return status.Error(codes.InvalidArgument, "HELLO is only valid as the first Connect frame")
			case frame.ControlObservation != nil:
				activeWorkers, err := connectActiveWorkers(frame.ControlObservation.ActiveWorkers)
				if err != nil {
					return err
				}
				response, err := s.core.heartbeat(ctx, protocol.HeartbeatRequest{
					RunnerID:  registered.RunnerID,
					SessionID: registered.SessionID,
					Capacity:  hello.Concurrency,
					InFlight:  activeWorkers,
					Timestamp: time.Now().Unix(),
					AuthToken: authToken,
					DrainObservation: &protocol.RunnerDrainObservation{
						Generation:        frame.ControlObservation.Generation,
						RecoveryOnly:      frame.ControlObservation.RecoveryOnly,
						ActiveActivations: frame.ControlObservation.ActiveActivations,
					},
				}, transport)
				if err != nil {
					return runnerStatus(err)
				}
				if err := sendDirective(response.Control); err != nil {
					return err
				}
			case frame.Result != nil:
				response, err := s.core.reportResult(ctx, protocol.ReportResultRequest{
					RunnerID:  registered.RunnerID,
					SessionID: registered.SessionID,
					Lease:     frame.Result.Lease,
					Result:    frame.Result.Result,
					AuthToken: authToken,
				}, transport)
				if err != nil && !errors.Is(err, engine.ErrInvalidLeaseToken) {
					return runnerStatus(err)
				}
				if err := stream.Send(protocol.ServerFrame{Ack: &protocol.AckFrame{
					LeaseID:  frame.Result.LeaseID,
					Accepted: response.Accepted,
					Error:    response.Error,
				}}); err != nil {
					return err
				}
			default:
				return status.Error(codes.InvalidArgument, "Connect frame has no supported payload")
			}
		case <-ticker.C:
			if err := sendDirective(s.core.runnerControlDirective(ctx, registered.RunnerID)); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// connectActiveWorkers converts the unsigned wire count into the directory's
// int-valued observation without allowing a 32-bit build to wrap a large
// runner count into a negative or apparently idle value.
func connectActiveWorkers(activeWorkers uint32) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if uint64(activeWorkers) > maxInt {
		return 0, status.Error(codes.InvalidArgument, "control observation active_workers exceeds platform integer range")
	}
	return int(activeWorkers), nil
}

type grpcConnectInbound struct {
	frame protocol.RunnerFrame
	err   error
}

func receiveGRPCConnectFrames(stream protocol.RunnerConnectStream, incoming chan<- grpcConnectInbound, done <-chan struct{}) {
	for {
		frame, err := stream.Recv()
		select {
		case incoming <- grpcConnectInbound{frame: frame, err: err}:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}

func sameRunnerControlDirective(left, right *protocol.RunnerControlDirective) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.DesiredState == right.DesiredState &&
		left.Generation == right.Generation &&
		left.RecoveryOnly == right.RecoveryOnly
}
