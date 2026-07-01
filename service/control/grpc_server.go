package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// GRPCServer adapts the generated RunnerProtocolServer onto the transport-agnostic
// Core. It shares the same RunnerPool and EngineFacade as the HTTP Server, so a
// single control plane can serve both transports concurrently.
type GRPCServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	core *Core
}

// GRPCServerOption configures a gRPC control-plane server.
type GRPCServerOption func(*GRPCServer)

// WithGRPCAuthenticator installs a runner-protocol authenticator on the gRPC
// server. Default is the permissive DisabledAuthenticator.
func WithGRPCAuthenticator(a Authenticator) GRPCServerOption {
	return func(s *GRPCServer) {
		if a != nil {
			s.core.auth = a
		}
	}
}

// WithGRPCLogger sets the logger used for auth decisions and diagnostics.
func WithGRPCLogger(l engine.Logger) GRPCServerOption {
	return func(s *GRPCServer) { s.core.logger = l }
}

// NewGRPCServer builds a gRPC Runner Protocol server backed by the given engine
// and runner pool. Pass the same RunnerPool used by the HTTP Server and
// Dispatcher to share runner state across transports.
func NewGRPCServer(engine EngineFacade, runners *RunnerPool, opts ...GRPCServerOption) *GRPCServer {
	if runners == nil {
		runners = NewRunnerPool()
	}
	srv := &GRPCServer{
		core: &Core{
			engine:   engine,
			runners:  runners,
			pollWait: time.Second,
		},
	}
	for _, o := range opts {
		o(srv)
	}
	return srv
}

func (s *GRPCServer) Register(ctx context.Context, req *runnerpb.RegisterRequest) (*runnerpb.RegisterResponse, error) {
	in := protocol.RegisterRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.register(in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	return &runnerpb.RegisterResponse{RunnerId: resp.RunnerID}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *runnerpb.HeartbeatRequest) (*runnerpb.HeartbeatResponse, error) {
	in := protocol.HeartbeatRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.heartbeat(in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	return &runnerpb.HeartbeatResponse{ServerTime: resp.ServerTime}, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *runnerpb.PollTaskRequest) (*runnerpb.PollTaskResponse, error) {
	in := protocol.PollTaskRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.pollTask(in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	out, err := protocol.PollTaskResponseToProto(resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return out, nil
}

func (s *GRPCServer) ReportResult(ctx context.Context, req *runnerpb.ReportResultRequest) (*runnerpb.ReportResultResponse, error) {
	in, err := protocol.ReportResultRequestFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.reportResult(ctx, in, grpcTransportInfo(ctx))
	if err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			// Carry the rejection in-band so the runner sees Accepted=false with
			// a reason, mirroring the HTTP 409 contract.
			return &runnerpb.ReportResultResponse{Accepted: false, Error: resp.Error}, nil
		}
		return nil, runnerStatus(err)
	}
	return &runnerpb.ReportResultResponse{Accepted: resp.Accepted, Error: resp.Error}, nil
}

// overrideTokenFromMetadata pulls the Authorization: Bearer <token> value out
// of gRPC metadata and, if present, overrides whatever the request payload
// carried. Matches the HTTP contract: header transport is authoritative.
func overrideTokenFromMetadata(ctx context.Context, dst *string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return
	}
	for _, v := range md.Get("authorization") {
		if strings.HasPrefix(v, "Bearer ") {
			*dst = strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
			return
		}
	}
}

// grpcTransportInfo extracts TLS peer identity for authenticators that want to
// enforce mTLS. Returns an empty struct on plaintext connections.
func grpcTransportInfo(ctx context.Context) TransportInfo {
	info := TransportInfo{}
	pr, ok := peer.FromContext(ctx)
	if !ok || pr == nil || pr.AuthInfo == nil {
		return info
	}
	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return info
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return info
	}
	cert := tlsInfo.State.PeerCertificates[0]
	info.TLSPeerCN = cert.Subject.String()
	info.TLSPeerSAN = append(info.TLSPeerSAN, cert.DNSNames...)
	return info
}

// runnerStatus maps transport-agnostic Core sentinel errors to gRPC status codes.
func runnerStatus(err error) error {
	switch {
	case errors.Is(err, ErrRunnerIDRequired), errors.Is(err, ErrConcurrencyRequired), errors.Is(err, ErrLeaseRequired):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrRunnerNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
