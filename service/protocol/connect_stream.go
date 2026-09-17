package protocol

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// RunnerConnectStream is the server-side, transport-neutral view of Connect.
// A control-plane implementation receives already-converted frames and can send
// WelcomeFrame, ControlFrame, and normal task frames without depending on gRPC
// generated types.
type RunnerConnectStream interface {
	Context() context.Context
	Recv() (RunnerFrame, error)
	Send(ServerFrame) error
}

// RunnerConnectHandler is the minimal callback a process-level server supplies
// to serve Connect. It deliberately belongs to protocol rather than importing a
// control-plane type, so service/control can consume it without reversing the
// protocol dependency direction.
type RunnerConnectHandler interface {
	Connect(RunnerConnectStream) error
}

// RunnerConnectHandlerFunc adapts a function to RunnerConnectHandler.
type RunnerConnectHandlerFunc func(RunnerConnectStream) error

func (f RunnerConnectHandlerFunc) Connect(stream RunnerConnectStream) error {
	return f(stream)
}

// ServeGRPCConnect adapts a generated gRPC Connect stream to a transport-neutral
// RunnerConnectHandler. A service/control GRPCServer can call this directly from
// its generated Connect method while retaining ownership of authentication,
// registration, task dispatch, and control-directory policy.
func ServeGRPCConnect(stream runnerpb.RunnerProtocol_ConnectServer, handler RunnerConnectHandler) error {
	if handler == nil {
		return status.Error(codes.Unimplemented, "runner Connect handler is not configured")
	}
	return handler.Connect(&grpcRunnerConnectStream{stream: stream})
}

type grpcRunnerConnectStream struct {
	stream runnerpb.RunnerProtocol_ConnectServer
}

func (s *grpcRunnerConnectStream) Context() context.Context {
	return s.stream.Context()
}

func (s *grpcRunnerConnectStream) Recv() (RunnerFrame, error) {
	frame, err := s.stream.Recv()
	if err != nil {
		return RunnerFrame{}, err
	}
	out, err := RunnerFrameFromProto(frame)
	if err != nil {
		return RunnerFrame{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return out, nil
}

func (s *grpcRunnerConnectStream) Send(frame ServerFrame) error {
	out, err := ServerFrameToProto(frame)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return s.stream.Send(out)
}
