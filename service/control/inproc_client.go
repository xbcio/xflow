package control

import (
	"context"

	"github.com/xbcio/xflow/service/protocol"
)

// InProcessRunnerClient is the runner protocol bound directly to a control
// plane, with no HTTP or gRPC hop. It is the third transport alongside the HTTP
// handler (server.go) and the gRPC server (grpc_server.go), and like them it is
// a thin adapter: every method builds the TransportInfo the wire transports
// build (stamped with TransportKindInProcess) and calls the same package-private
// Core method.
//
// Two things it deliberately does NOT do, because Core already owns them:
// request-shape validation and retries. Both live in Core, and duplicating
// either here would let the in-process path drift from the wire paths it is
// supposed to be indistinguishable from.
//
// The token is carried here rather than on each request because the wire
// transports inject it and Core reads it back:
//
//   - HTTP puts it in an Authorization header; the handler's
//     overrideTokenFromHeader copies it onto req.AuthToken before Core sees it.
//   - gRPC puts it in metadata; overrideTokenFromMetadata does the same.
//
// In process there is no header or metadata layer, so the client writes it onto
// the request itself via stamp. Without this the in-process path would
// authenticate as the empty token and be rejected everywhere the wire path is
// accepted.
type InProcessRunnerClient struct {
	core  *Core
	token string
}

// InProcessRunnerClient returns a runner-protocol client that dispatches
// straight into this server's Core, carrying token as the runner's bearer
// credential (the same value the HTTP transport would set as an Authorization
// header). The returned value satisfies the runner service's ProtocolClient and
// its optional MetricsReportClient / lease-renew / activation-ack / identity-
// renew capabilities, so an embedded runner reaches every protocol method
// without a socket.
func (s *Server) InProcessRunnerClient(token string) *InProcessRunnerClient {
	return &InProcessRunnerClient{core: s.core, token: token}
}

// stamp writes the client's token onto a request field the way the wire
// transports' server-side header/metadata lifting does. An empty client token
// leaves the field untouched, matching how an empty WithToken disables the
// Authorization header rather than clearing it — a request that already carries
// its own credential (RenewIdentityRequest does) keeps it.
func (c *InProcessRunnerClient) stamp(dst *string) {
	if c.token != "" {
		*dst = c.token
	}
}

// inProcessTransportInfo is the TransportInfo every in-process call carries. It
// names the transport kind so an authenticator can tell the embedded runner
// apart from a peerless HTTP caller, which presents the same empty SourceIP.
// SourceIP is deliberately empty: this transport has no network peer.
func inProcessTransportInfo() TransportInfo {
	return TransportInfo{Kind: TransportKindInProcess}
}

func (c *InProcessRunnerClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.register(ctx, req, inProcessTransportInfo())
}

func (c *InProcessRunnerClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.heartbeat(ctx, req, inProcessTransportInfo())
}

func (c *InProcessRunnerClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.pollTask(ctx, req, inProcessTransportInfo())
}

func (c *InProcessRunnerClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.reportResult(ctx, req, inProcessTransportInfo())
}

// RenewLease is a capability the gRPC transport deliberately omits (see
// service/runner/lease_renew.go); in process there is no such constraint, so
// this transport implements it and a long-running handler is not reclaimed
// mid-flight. This is an intentional difference from gRPC, not an oversight.
func (c *InProcessRunnerClient) RenewLease(ctx context.Context, req protocol.RenewLeaseRequest) (protocol.RenewLeaseResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.renewLease(ctx, req, inProcessTransportInfo())
}

// ActivationAck, like RenewLease, is implemented here although gRPC omits it.
func (c *InProcessRunnerClient) ActivationAck(ctx context.Context, ack protocol.ActivationAck) error {
	c.stamp(&ack.AuthToken)
	return c.core.activationAck(ctx, ack, inProcessTransportInfo())
}

// ReportMetrics hands the raw Prometheus body to Core without re-encoding: the
// wire transport frames it as gzip protobuf, but Core.reportMetrics takes the
// bytes and there is no reason to round-trip them through a codec in process.
func (c *InProcessRunnerClient) ReportMetrics(ctx context.Context, runnerID, sessionID string, body []byte) error {
	return c.core.reportMetrics(ctx, runnerID, sessionID, c.token, body, inProcessTransportInfo())
}

func (c *InProcessRunnerClient) RenewIdentity(ctx context.Context, req protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
	c.stamp(&req.AuthToken)
	return c.core.renewIdentity(ctx, req, inProcessTransportInfo())
}
