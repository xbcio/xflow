package protocol

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
	in := RegisterRequestToProto(req)
	resp, err := c.grpc.Register(c.withAuth(ctx), in)
	if err != nil {
		return RegisterRunnerResponse{}, requestSizeError("Register", in, err)
	}
	return RegisterResponseFromProto(resp), nil
}

func (c *GRPCClient) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	in := HeartbeatRequestToProto(req)
	resp, err := c.grpc.Heartbeat(c.withAuth(ctx), in)
	if err != nil {
		return HeartbeatResponse{}, requestSizeError("Heartbeat", in, err)
	}
	return HeartbeatResponseFromProto(resp)
}

func (c *GRPCClient) Poll(ctx context.Context, req PollTaskRequest) (PollTaskResponse, error) {
	in := PollTaskRequestToProto(req)
	resp, err := c.grpc.PollTask(c.withAuth(ctx), in)
	if err != nil {
		return PollTaskResponse{}, requestSizeError("PollTask", in, err)
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
		return ReportResultResponse{}, requestSizeError("ReportResult", in, err)
	}
	return ReportResultResponse{Accepted: resp.GetAccepted(), Error: resp.GetError()}, nil
}

// grpcMessageSizePattern matches the "(N vs. M)" message-size statuses grpc-go
// raises with codes.ResourceExhausted, capturing the offending message size N:
// the client's own send limit ("trying to send message larger than max"), and
// a peer's receive limit ("grpc: received message larger than max", "grpc:
// message after decompression larger than max"), which the server returns as
// the RPC status. grpc-go exposes no structured detail for these, so the
// status message is the only signal; TestGRPCClientMapsRequestSizeErrors pins
// the wording against the grpc-go version in go.mod.
var grpcMessageSizePattern = regexp.MustCompile(`larger than max(?: length allowed on current machine)? \((\d+) vs\. \d+\)`)

// requestSizeError wraps err in ErrRunnerRequestTooLarge when it is a gRPC
// message-size rejection of the request in itself, so a runner treats it as
// the HTTP Client's oversize body: resending the same request cannot succeed.
//
// ResourceExhausted alone does not identify the request. The same code covers
// a response over the client's receive limit or the server's send limit, an
// RST_STREAM with FLOW_CONTROL_ERROR or ENHANCE_YOUR_CALM, and anything a
// server interceptor chooses to return (the runner protocol server returns it
// for none of its own errors). Only a size status whose reported message size
// equals the encoded request is mapped. A response rejection reports the
// response size, which cannot equal a request that was itself sent and read
// within limits no larger than the response limit, as RunnerGRPCDialOptions
// and RunnerGRPCServerOptions configure them. Any other ResourceExhausted is
// returned unchanged and keeps its transient meaning.
func requestSizeError(method string, in proto.Message, err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return err
	}
	m := grpcMessageSizePattern.FindStringSubmatch(st.Message())
	if m == nil || m[1] != strconv.Itoa(proto.Size(in)) {
		return err
	}
	return fmt.Errorf("runner protocol %s: %w: %w", method, ErrRunnerRequestTooLarge, err)
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
