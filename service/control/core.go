package control

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)


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
	runners  RunnerDirectory
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
	session, err := c.runners.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     req.RunnerID,
		Capacity:     req.Concurrency,
		Capabilities: req.Capabilities,
		Policy:       policy,
		Now:          time.Now(),
	})
	if err != nil {
		return protocol.RegisterRunnerResponse{}, normalizeRunnerError(err)
	}
	return protocol.RegisterRunnerResponse{RunnerID: req.RunnerID, SessionID: session.SessionID}, nil
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
	if err := c.runners.Heartbeat(context.Background(), HeartbeatRequest{
		RunnerID:  req.RunnerID,
		SessionID: req.SessionID,
		Capacity:  req.Capacity,
		InFlight:  req.InFlight,
		Now:       at,
	}); err != nil {
		return protocol.HeartbeatResponse{}, normalizeRunnerError(err)
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
	for {
		claim, ok, err := c.runners.ClaimForRunner(context.Background(), ClaimRequest{
			RunnerID:     req.RunnerID,
			SessionID:    req.SessionID,
			Capacity:     req.Capacity,
			Capabilities: req.Capabilities,
			Now:          time.Now(),
		})
		if err != nil {
			return protocol.PollTaskResponse{}, normalizeRunnerError(err)
		}
		if !ok {
			return protocol.PollTaskResponse{Wait: c.pollWait}, nil
		}
		if c.engine == nil {
			_ = c.runners.ReleaseClaim(context.Background(), claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, ErrEngineNotConfigured
		}
		lease, err := c.engine.BuildTaskLease(context.Background(), &claim.Assignment.Task)
		switch {
		case err == nil:
			if err := c.runners.FinalizeClaim(context.Background(), claim.ClaimID, lease); err != nil {
				_ = c.runners.ReleaseClaim(context.Background(), claim.ClaimID, ReleaseClaimRequeue)
				return protocol.PollTaskResponse{}, normalizeRunnerError(err)
			}
			return protocol.PollTaskResponse{Lease: lease}, nil
		case errors.Is(err, engine.ErrExecutionInactive):
			_ = c.runners.ReleaseClaim(context.Background(), claim.ClaimID, ReleaseClaimDrop)
		case errors.Is(err, engine.ErrLeaseAlreadyActive):
			_ = c.runners.ReleaseClaim(context.Background(), claim.ClaimID, ReleaseClaimKeepSeen)
		default:
			_ = c.runners.ReleaseClaim(context.Background(), claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, err
		}
	}
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
	outcome, err := c.engine.CommitTaskResultWithOutcome(ctx, req.Lease, req.Result)
	if err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			return protocol.ReportResultResponse{Accepted: false, Error: err.Error()}, err
		}
		return protocol.ReportResultResponse{}, err
	}
	switch outcome {
	case engine.CommitOutcomeAccepted, engine.CommitOutcomeDuplicateTerminal, engine.CommitOutcomeExecutionInactive:
		if err := c.runners.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID:     req.RunnerID,
			SessionID:    req.SessionID,
			AssignmentID: BuildAssignmentID(&req.Lease.Task),
			RemoveSeen:   true,
		}); err != nil {
			return protocol.ReportResultResponse{}, normalizeRunnerError(err)
		}
	}
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func normalizeRunnerError(err error) error {
	if errors.Is(err, ErrRunnerSessionStale) {
		return ErrRunnerNotFound
	}
	return err
}
