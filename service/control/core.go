package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// ConnectStream is the transport-agnostic bidirectional stream Core uses. The
// gRPC adapter wraps runnerpb.RunnerProtocol_ConnectServer onto it.
type ConnectStream interface {
	Recv() (protocol.RunnerFrame, error)
	Send(protocol.ServerFrame) error
	Context() context.Context
}

// Connect drives a runner's full lifecycle: HELLO → register+bind session →
// send/recv loops → BYE/disconnect. Returns on clean BYE, ctx cancel, or
// stream error. Single sender to stream: the send loop is the only goroutine
// calling stream.Send; ACKs and TASKs are both funneled through sess.send.
func (c *Core) Connect(stream ConnectStream, token string, info TransportInfo) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Hello == nil {
		return ErrRunnerIDRequired
	}
	hello := first.Hello
	if hello.RunnerID == "" || hello.Concurrency <= 0 {
		return ErrConcurrencyRequired
	}
	if _, authErr := c.authn().AuthenticateRegister(hello.RunnerID, token, info); authErr != nil {
		if err := c.authDeny(hello.RunnerID, token, "register", info, authErr); err != nil {
			return err
		}
	}
	c.runners.RegisterWithLabelsAndPolicy(hello.RunnerID, hello.Concurrency, hello.Capabilities, hello.Labels, RunnerPolicy{AllowedNodeTypes: []string{"*"}})

	sendCh := make(chan protocol.ServerFrame, hello.Concurrency*2+2)
	done := make(chan struct{})
	sess := newStreamSession(hello.RunnerID, sendCh, done, hello.Concurrency)
	c.runners.bindSession(hello.RunnerID, sess)

	if err := c.sendFrame(ctx, sendCh, protocol.ServerFrame{Welcome: &protocol.WelcomeFrame{RunnerID: hello.RunnerID, ServerTime: time.Now().Unix()}}); err != nil {
		c.runners.clearSession(hello.RunnerID)
		close(done)
		return err
	}

	recvErr := make(chan error, 1)
	go func() { recvErr <- c.connectRecvLoop(ctx, stream, hello.RunnerID, sess) }()
	sendErr := make(chan error, 1)
	go func() { sendErr <- c.connectSendLoop(ctx, stream, sendCh, sess, done) }()

	var firstErr error
	select {
	case err := <-recvErr:
		firstErr = err
		close(done)
		<-sendErr
	case err := <-sendErr:
		firstErr = err
		close(done)
		<-recvErr
	case <-ctx.Done():
		firstErr = ctx.Err()
		close(done)
		<-recvErr
		<-sendErr
	}
	c.runners.clearSession(hello.RunnerID)
	if errors.Is(firstErr, context.Canceled) {
		return nil
	}
	return firstErr
}

func (c *Core) connectRecvLoop(ctx context.Context, stream ConnectStream, runnerID string, sess *streamSession) error {
	for {
		fr, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case fr.Bye != nil:
			return nil
		case fr.Result != nil:
			if err := c.handleResultFrame(ctx, runnerID, fr.Result, sess); err != nil {
				return err
			}
		}
	}
}

func (c *Core) handleResultFrame(ctx context.Context, runnerID string, r *protocol.ResultFrame, sess *streamSession) error {
	if c.engine == nil {
		return ErrEngineNotConfigured
	}
	var ack protocol.ServerFrame
	if err := c.engine.CommitTaskResult(ctx, r.Lease, r.Result); err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			ack = protocol.ServerFrame{Ack: &protocol.AckFrame{LeaseID: r.LeaseID, Accepted: false, Error: err.Error()}}
		} else {
			return err
		}
	} else {
		ack = protocol.ServerFrame{Ack: &protocol.AckFrame{LeaseID: r.LeaseID, Accepted: true}}
	}
	c.runners.consumeResult(runnerID, r.LeaseID)
	return c.sendFrame(ctx, sess.send, ack)
}

func (c *Core) connectSendLoop(ctx context.Context, stream ConnectStream, sessSend <-chan protocol.ServerFrame, sess *streamSession, done <-chan struct{}) error {
	c.runners.drainInto(sess)
	notify := c.runners.notifyChan(sess.runnerID)
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case fr := <-sessSend:
			if err := stream.Send(fr); err != nil {
				return err
			}
			c.runners.drainInto(sess)
		case <-notify:
			c.runners.drainInto(sess)
		}
	}
}

func (c *Core) sendFrame(ctx context.Context, send chan<- protocol.ServerFrame, fr protocol.ServerFrame) error {
	select {
	case send <- fr:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Transport-agnostic outcome errors. Each transport (HTTP, gRPC) maps these to
// its own status representation so the core handling logic stays free of
// net/http and grpc/codes.
var (
	ErrRunnerIDRequired    = errors.New("runner_id is required")
	ErrConcurrencyRequired = errors.New("runner_id and concurrency are required")
	ErrRunnerNotFound      = errors.New("runner not found")
	ErrLeaseRequired       = errors.New("runner_id and lease are required")
	ErrEngineNotConfigured = errors.New("engine not configured")
	ErrUnauthenticated     = errors.New("unauthenticated")
)

// Core holds the transport-independent Runner Protocol logic shared by the HTTP
// and gRPC servers. Each method takes and returns protocol DTOs and signals
// outcomes through the sentinel errors above plus engine.ErrInvalidLeaseToken.
type Core struct {
	engine   EngineFacade
	runners  *RunnerPool
	pollWait time.Duration
	// auth resolves credentials to a RunnerPolicy on every call. Nil == the
	// disabled authenticator, matching legacy behavior.
	auth         Authenticator
	logger       engine.Logger
	authObserver AuthObserver
}

// AuthObserver receives auth allow/deny/dry-run decisions.
type AuthObserver interface {
	OnAuthDecision(op, result, authMode string)
}

func (c *Core) authn() Authenticator {
	if c.auth == nil {
		return DisabledAuthenticator{}
	}
	return c.auth
}

// authDeny logs an auth outcome (with fingerprinted token) and returns the
// transport-agnostic unauthenticated sentinel. Dry-run denials are logged but
// return nil so the request proceeds.
func (c *Core) authDeny(runnerID, token, op string, info TransportInfo, err error) error {
	if err == nil {
		c.observeAuth(op, "allow")
		return nil
	}
	if IsDryRunDenial(err) {
		c.observeAuth(op, "dry_run_allow")
		if c.logger != nil {
			c.logger.Error("auth_dry_run_violation",
				"op", op, "runner", runnerID, "token", TokenFingerprint(token), "cn", info.TLSPeerCN, "err", err)
		}
		return nil
	}
	c.observeAuth(op, "deny")
	if c.logger != nil {
		c.logger.Error("auth_denied",
			"op", op, "runner", runnerID, "token", TokenFingerprint(token), "cn", info.TLSPeerCN, "err", err)
	}
	return ErrUnauthenticated
}

func (c *Core) observeAuth(op, result string) {
	if c.authObserver == nil {
		return
	}
	c.authObserver.OnAuthDecision(op, result, authMode(c.authn()))
}

func authMode(auth Authenticator) string {
	switch a := auth.(type) {
	case DisabledAuthenticator:
		return "disabled"
	case *FilePolicyStore:
		if a.IsDryRun() {
			return "dry_run"
		}
		return "enforcing"
	default:
		return "custom"
	}
}

func (c *Core) register(req protocol.RegisterRunnerRequest, info TransportInfo) (protocol.RegisterRunnerResponse, error) {
	if req.RunnerID == "" || req.Concurrency <= 0 {
		return protocol.RegisterRunnerResponse{}, ErrConcurrencyRequired
	}
	policy, authErr := c.authn().AuthenticateRegister(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(req.RunnerID, req.AuthToken, "register", info, authErr); err != nil {
		return protocol.RegisterRunnerResponse{}, err
	}
	c.runners.RegisterWithLabelsAndPolicy(req.RunnerID, req.Concurrency, req.Capabilities, req.Labels, policy)
	return protocol.RegisterRunnerResponse{RunnerID: req.RunnerID}, nil
}

func (c *Core) heartbeat(req protocol.HeartbeatRequest, info TransportInfo) (protocol.HeartbeatResponse, error) {
	if req.RunnerID == "" {
		return protocol.HeartbeatResponse{}, ErrRunnerIDRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(req.RunnerID, req.AuthToken, "heartbeat", info, authErr); err != nil {
		return protocol.HeartbeatResponse{}, err
	}
	at := time.Unix(req.Timestamp, 0)
	if req.Timestamp == 0 {
		at = time.Now()
	}
	if !c.runners.Heartbeat(req.RunnerID, req.Capacity, req.InFlight, at) {
		return protocol.HeartbeatResponse{}, ErrRunnerNotFound
	}
	return protocol.HeartbeatResponse{ServerTime: time.Now().Unix()}, nil
}

func (c *Core) pollTask(req protocol.PollTaskRequest, info TransportInfo) (protocol.PollTaskResponse, error) {
	if req.RunnerID == "" {
		return protocol.PollTaskResponse{}, ErrRunnerIDRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(req.RunnerID, req.AuthToken, "poll", info, authErr); err != nil {
		return protocol.PollTaskResponse{}, err
	}
	lease, ok := c.runners.PollWithLabels(req.RunnerID, req.Capacity, req.Capabilities, req.Labels)
	if !ok {
		return protocol.PollTaskResponse{Wait: c.pollWait}, nil
	}
	return protocol.PollTaskResponse{Lease: &lease}, nil
}

func (c *Core) reportResult(ctx context.Context, req protocol.ReportResultRequest, info TransportInfo) (protocol.ReportResultResponse, error) {
	if req.RunnerID == "" || req.Lease == nil {
		return protocol.ReportResultResponse{}, ErrLeaseRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(req.RunnerID, req.AuthToken, "report_result", info, authErr); err != nil {
		return protocol.ReportResultResponse{}, err
	}
	if c.engine == nil {
		return protocol.ReportResultResponse{}, ErrEngineNotConfigured
	}
	if err := c.engine.CommitTaskResult(ctx, req.Lease, req.Result); err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			return protocol.ReportResultResponse{Accepted: false, Error: err.Error()}, err
		}
		return protocol.ReportResultResponse{}, err
	}
	return protocol.ReportResultResponse{Accepted: true}, nil
}
