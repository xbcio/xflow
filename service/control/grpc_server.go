package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
)

// GRPCServer adapts the generated RunnerProtocolServer onto the transport-agnostic
// Core. It shares the same RunnerDirectory and EngineFacade as the HTTP Server, so a
// single control plane can serve both transports concurrently.
type GRPCServer struct {
	runnerpb.UnimplementedRunnerProtocolServer
	core *Core
}

// GRPCServerOption configures a gRPC control-plane server.
type GRPCServerOption func(*GRPCServer)

// RunnerGRPCServerOptions returns the grpc.ServerOptions every grpc.Server
// hosting the runner protocol must be built with. Today that is a receive
// limit of MaxRegisterRunnerBodyBytes, so a Register the HTTP transport
// accepts is not rejected with ResourceExhausted by grpc-go's 4 MiB default,
// and a send limit of the same size, matching what a runner built with
// RunnerGRPCDialOptions accepts. Without the send limit grpc-go would send up
// to MaxInt32, and an oversize message would fail on the runner as an opaque
// ResourceExhausted instead of on the server, where it can be logged.
//
// PollTask does not rely on the send limit for leases: it measures the
// encoded response itself and fails a lease too large to deliver (see
// failUndeliverableLease), because a lease the transport refuses is not lost
// but redelivered indefinitely.
//
// Both limits are server-wide, not Register-only: Heartbeat, PollTask,
// ReportResult and every Connect stream message may now also be up to 8 MiB
// instead of 4 MiB. grpc-go enforces MaxRecvMsgSize in the transport while
// reading a message, before any handler or interceptor sees it, and offers no
// per-method override; an interceptor can only tighten after the full message
// is already buffered, so it could not be the mechanism that loosens one
// method. Any other service registered on the same grpc.Server inherits the
// limits too.
func RunnerGRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxRegisterRunnerBodyBytes),
		grpc.MaxSendMsgSize(MaxRegisterRunnerBodyBytes),
	}
}

// RunnerGRPCDialOptions returns the grpc.DialOptions every runner-protocol
// client connection must be built with: send and receive limits of
// MaxRegisterRunnerBodyBytes, the same as RunnerGRPCServerOptions. Without
// them grpc-go's 4 MiB default receive limit rejects a server message the
// server is allowed to send, and the runner sees only an opaque
// ResourceExhausted.
func RunnerGRPCDialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MaxRegisterRunnerBodyBytes),
		grpc.MaxCallSendMsgSize(MaxRegisterRunnerBodyBytes),
	)}
}

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

// WithGRPCReportRejectionObserver installs the report-rejection observer on the
// gRPC server.
//
// gRPC gets its own Core (see NewGRPCServer), so the HTTP Server's option does
// not reach here and a 409 on this transport would otherwise stay unattributable.
// The gRPC handler answers an invalid lease token in-band rather than with an
// HTTP status, but it is the same four fences underneath and the same need to
// tell them apart. nil or unset is a no-op.
func WithGRPCReportRejectionObserver(observer ReportRejectionObserver) GRPCServerOption {
	return func(s *GRPCServer) { s.core.reportRejectionObserver = observer }
}

// WithGRPCRunnerDescriptorObserver installs the runner-descriptor rejection
// observer on the gRPC server's own Core. nil or unset is a no-op.
func WithGRPCRunnerDescriptorObserver(observer RunnerDescriptorObserver) GRPCServerOption {
	return func(s *GRPCServer) { s.core.runnerDescriptorObserver = observer }
}

// WithGRPCAuthObserver installs a non-blocking observer for runner auth decisions.
func WithGRPCAuthObserver(observer AuthObserver) GRPCServerOption {
	return func(s *GRPCServer) { s.core.authObserver = observer }
}

// WithGRPCPollWait sets the long-poll wait duration returned to runners when
// no task is available. Default is one second.
func WithGRPCPollWait(d time.Duration) GRPCServerOption {
	return func(s *GRPCServer) {
		if d > 0 {
			s.core.pollWait = d
		}
	}
}

// WithGRPCTracer installs a distributed tracing implementation on the gRPC
// control-plane server. Mirrors WithTracer for the HTTP server.
func WithGRPCTracer(t tracing.Tracer) GRPCServerOption {
	return func(s *GRPCServer) {
		if t != nil {
			s.core.tracer = t
		}
	}
}

// NewGRPCServer builds a gRPC Runner Protocol server backed by the given engine
// and runner directory. Pass the same RunnerDirectory used by the HTTP Server and
// Dispatcher to share runner state across transports.
func NewGRPCServer(engine EngineFacade, runners RunnerDirectory, opts ...GRPCServerOption) *GRPCServer {
	if runners == nil {
		runners = NewMemoryRunnerDirectory()
	}
	srv := &GRPCServer{
		core: &Core{
			engine:       engine,
			runners:      runners,
			pollWait:     time.Second,
			pollWalkWait: defaultPollWalkWait,
			tracer:       tracing.NoopTracer{},
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
	resp, err := s.core.register(ctx, in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	return protocol.RegisterResponseToProto(resp), nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *runnerpb.HeartbeatRequest) (*runnerpb.HeartbeatResponse, error) {
	in := protocol.HeartbeatRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.heartbeat(ctx, in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	out, convErr := protocol.HeartbeatResponseToProto(resp)
	if convErr != nil {
		return nil, status.Error(codes.Internal, ErrInternalServer.Error())
	}
	return out, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *runnerpb.PollTaskRequest) (*runnerpb.PollTaskResponse, error) {
	in := protocol.PollTaskRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.pollTask(ctx, in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	out, err := protocol.PollTaskResponseToProto(resp)
	if err != nil {
		return nil, status.Error(codes.Internal, ErrInternalServer.Error())
	}
	if size := proto.Size(out); resp.Lease != nil && size > MaxRegisterRunnerBodyBytes {
		out, err = protocol.PollTaskResponseToProto(s.core.failOversizeLease(ctx, in, resp, size, "runner gRPC message limit"))
		if err != nil {
			return nil, status.Error(codes.Internal, ErrInternalServer.Error())
		}
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

// AckActivation reports the outcome of an activate/deactivate directive back
// to the server, mirroring HandleActivationAck exactly: both call
// Core.activationAck, which owns authz, namespace scoping, fencing/generation
// checks, and idempotency, so neither transport duplicates that logic.
func (s *GRPCServer) AckActivation(ctx context.Context, req *runnerpb.ActivationAckRequest) (*runnerpb.ActivationAckResponse, error) {
	in := protocol.ActivationAckRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	if err := s.core.activationAck(ctx, in, grpcTransportInfo(ctx)); err != nil {
		return nil, runnerStatus(err)
	}
	return &runnerpb.ActivationAckResponse{}, nil
}

// RenewLease extends an active lease's deadline, mirroring HandleRenewLease
// exactly: both call Core.renewLease, which owns session validation, lease
// lookup, execution-deadline enforcement, and the group/node renewal split,
// so neither transport duplicates that logic.
func (s *GRPCServer) RenewLease(ctx context.Context, req *runnerpb.RenewLeaseRequest) (*runnerpb.RenewLeaseResponse, error) {
	in := protocol.RenewLeaseRequestFromProto(req)
	overrideTokenFromMetadata(ctx, &in.AuthToken)
	resp, err := s.core.renewLease(ctx, in, grpcTransportInfo(ctx))
	if err != nil {
		return nil, runnerStatus(err)
	}
	return protocol.RenewLeaseResponseToProto(resp), nil
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
// enforce mTLS. It deliberately does not populate SourceIP, so a gRPC caller is
// not distinguishable from a peerless HTTP one by address; authenticators that
// must tell a local gRPC peer apart need Kind (currently the second half of the
// gRPC story is unimplemented — a loopback-gated policy denies all gRPC runners
// rather than admitting the local one). Returns an empty TLS identity on
// plaintext connections.
func grpcTransportInfo(ctx context.Context) TransportInfo {
	info := TransportInfo{Kind: TransportKindGRPC}
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
	case errors.Is(err, ErrRunnerIDRequired), errors.Is(err, ErrRunnerSessionRequired), errors.Is(err, ErrConcurrencyRequired), errors.Is(err, ErrInstanceUIDRequired), errors.Is(err, ErrInvalidNamespace), errors.Is(err, ErrLabelConflict), errors.Is(err, ErrLeaseRequired), errors.Is(err, ErrMissingWorkflowVersion):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrInvalidCapability):
		return status.Error(codes.InvalidArgument, ErrInvalidCapability.Error())
	case errors.Is(err, ErrRunnerSessionStale), errors.Is(err, ErrRunnerIDConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrRunnerNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, ErrAuthNamespaceDenied):
		return status.Error(codes.PermissionDenied, ErrAuthNamespaceDenied.Error())
	case errors.Is(err, ErrAuthCapabilityDenied):
		return status.Error(codes.PermissionDenied, ErrAuthCapabilityDenied.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
