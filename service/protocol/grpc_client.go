package protocol

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// GRPCClient speaks the Runner Protocol over gRPC. It implements the same method
// set as the HTTP Client (and the runner.ProtocolClient interface), so a runner
// switches transports purely by injecting a different client.
type GRPCClient struct {
	grpc  runnerpb.RunnerProtocolClient
	token string
}

// NewGRPCClient wraps an established gRPC connection. The caller owns the
// connection lifecycle (Dial / Close).
func NewGRPCClient(conn grpc.ClientConnInterface) *GRPCClient {
	return &GRPCClient{grpc: runnerpb.NewRunnerProtocolClient(conn)}
}

// WithToken returns a client that attaches Authorization: Bearer <token> to
// every outgoing RPC via gRPC metadata. Mirrors HTTP Client.WithToken.
func (c *GRPCClient) WithToken(token string) *GRPCClient {
	cp := *c
	cp.token = token
	return &cp
}

// withAuth appends authorization metadata to the outgoing context. Uses
// AppendToOutgoingContext so callers who already set metadata (test doubles,
// interceptors) do not lose their values.
func (c *GRPCClient) withAuth(ctx context.Context) context.Context {
	if c.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

func (c *GRPCClient) Register(ctx context.Context, req RegisterRunnerRequest) (RegisterRunnerResponse, error) {
	resp, err := c.grpc.Register(c.withAuth(ctx), RegisterRequestToProto(req))
	if err != nil {
		return RegisterRunnerResponse{}, err
	}
	return RegisterResponseFromProto(resp), nil
}

func (c *GRPCClient) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	resp, err := c.grpc.Heartbeat(c.withAuth(ctx), HeartbeatRequestToProto(req))
	if err != nil {
		return HeartbeatResponse{}, err
	}
	return HeartbeatResponseFromProto(resp)
}

func (c *GRPCClient) Poll(ctx context.Context, req PollTaskRequest) (PollTaskResponse, error) {
	resp, err := c.grpc.PollTask(c.withAuth(ctx), PollTaskRequestToProto(req))
	if err != nil {
		return PollTaskResponse{}, err
	}
	return PollTaskResponseFromProto(resp)
}

func (c *GRPCClient) ReportResult(ctx context.Context, req ReportResultRequest) (ReportResultResponse, error) {
	in, err := ReportResultRequestToProto(req)
	if err != nil {
		return ReportResultResponse{}, err
	}
	resp, err := c.grpc.ReportResult(c.withAuth(ctx), in)
	if err != nil {
		return ReportResultResponse{}, err
	}
	return ReportResultResponse{Accepted: resp.GetAccepted(), Error: resp.GetError()}, nil
}

// Connect opens the bidi Runner Protocol stream. Token (if set) is attached
// via metadata on the stream context.
func (c *GRPCClient) Connect(ctx context.Context) (FrameStream, error) {
	stream, err := c.grpc.Connect(c.withAuth(ctx))
	if err != nil {
		return nil, err
	}
	return &grpcFrameStream{stream: stream}, nil
}

type grpcFrameStream struct {
	stream grpc.BidiStreamingClient[runnerpb.RunnerFrame, runnerpb.ServerFrame]
}

func (g *grpcFrameStream) Send(fr RunnerFrame) error {
	pb, err := RunnerFrameToProto(fr)
	if err != nil {
		return err
	}
	return g.stream.Send(pb)
}

func (g *grpcFrameStream) Recv() (ServerFrame, error) {
	pb, err := g.stream.Recv()
	if err != nil {
		return ServerFrame{}, err
	}
	return ServerFrameFromProto(pb)
}

func (g *grpcFrameStream) Close() error { return g.stream.CloseSend() }
