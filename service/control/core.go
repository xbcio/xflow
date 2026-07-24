package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
)

// Transport-agnostic outcome errors. Each transport (HTTP, gRPC) maps these to
// its own status representation so the core handling logic stays free of
// net/http and grpc/codes.
var (
	ErrRunnerIDRequired      = errors.New("runner_id is required")
	ErrRunnerSessionRequired = errors.New("runner_id and session_id are required")
	ErrConcurrencyRequired   = errors.New("runner_id and concurrency are required")
	ErrInvalidTenant         = errors.New("invalid tenant")
	ErrRunnerNotFound        = errors.New("runner not found")
	ErrLeaseRequired         = errors.New("runner_id, session_id and lease are required")
	ErrEngineNotConfigured   = errors.New("engine not configured")
	ErrUnauthenticated       = errors.New("unauthenticated")
	// ErrInternalServer is the generic message returned to clients for any
	// error that is not a recognised transport-agnostic sentinel. The full
	// error is logged server-side; clients must never see internal stack
	// traces, Redis errors, or backend paths.
	ErrInternalServer = errors.New("internal server error")
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
	// tracer instruments the runner protocol dispatch and commit path.
	// NoopTracer when tracing is disabled.
	tracer tracing.Tracer
}

// leaseRecoveryEngine is deliberately optional so custom EngineFacade test
// doubles and integrations remain source compatible. The concrete engine uses
// it to replay a lease that was committed before a control-plane crash.
type leaseRecoveryEngine interface {
	RecoverTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error)
}

// AuthObserver receives auth allow/deny/dry-run decisions.
type AuthObserver interface {
	OnAuthDecision(ctx context.Context, op, result, authMode string)
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
func (c *Core) authDeny(ctx context.Context, runnerID, token, op string, info TransportInfo, err error) error {
	if err == nil {
		c.observeAuth(ctx, op, "allow")
		return nil
	}
	if IsDryRunDenial(err) {
		c.observeAuth(ctx, op, "dry_run_allow")
		if c.logger != nil {
			c.logger.Error("auth_dry_run_violation",
				"op", op, "runner", runnerID, "token", TokenFingerprint(token), "cn", info.TLSPeerCN, "err", err)
		}
		return nil
	}
	c.observeAuth(ctx, op, "deny")
	if c.logger != nil {
		c.logger.Error("auth_denied",
			"op", op, "runner", runnerID, "token", TokenFingerprint(token), "cn", info.TLSPeerCN, "err", err)
	}
	return ErrUnauthenticated
}

func (c *Core) observeAuth(ctx context.Context, op, result string) {
	if c.authObserver == nil {
		return
	}
	c.authObserver.OnAuthDecision(ctx, op, result, authMode(c.authn()))
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

func (c *Core) register(ctx context.Context, req protocol.RegisterRunnerRequest, info TransportInfo) (protocol.RegisterRunnerResponse, error) {
	if req.RunnerID == "" || req.Concurrency <= 0 {
		return protocol.RegisterRunnerResponse{}, ErrConcurrencyRequired
	}
	for _, t := range tenantIDs(req.Tenants) {
		if err := tenant.Validate(t); err != nil {
			return protocol.RegisterRunnerResponse{}, fmt.Errorf("%w: %v", ErrInvalidTenant, err)
		}
	}
	policy, authErr := c.authn().AuthenticateRegister(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "register", info, authErr); err != nil {
		return protocol.RegisterRunnerResponse{}, err
	}
	session, err := c.runners.Register(ctx, RegisterRunnerRequest{
		RunnerID:     req.RunnerID,
		Capacity:     req.Concurrency,
		Capabilities: req.Capabilities,
		Policy:       policy,
		Tenants:      tenantIDs(req.Tenants),
		Now:          time.Now(),
	})
	if err != nil {
		return protocol.RegisterRunnerResponse{}, normalizeRunnerError(err, c.logger, "register")
	}
	return protocol.RegisterRunnerResponse{RunnerID: req.RunnerID, SessionID: session.SessionID}, nil
}

func (c *Core) heartbeat(ctx context.Context, req protocol.HeartbeatRequest, info TransportInfo) (protocol.HeartbeatResponse, error) {
	if req.RunnerID == "" || req.SessionID == "" {
		return protocol.HeartbeatResponse{}, ErrRunnerSessionRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "heartbeat", info, authErr); err != nil {
		return protocol.HeartbeatResponse{}, err
	}
	at := time.Unix(req.Timestamp, 0)
	if req.Timestamp == 0 {
		at = time.Now()
	}
	if err := c.runners.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  req.RunnerID,
		SessionID: req.SessionID,
		Capacity:  req.Capacity,
		InFlight:  req.InFlight,
		Now:       at,
	}); err != nil {
		return protocol.HeartbeatResponse{}, normalizeRunnerError(err, c.logger, "heartbeat")
	}
	return protocol.HeartbeatResponse{ServerTime: time.Now().Unix()}, nil
}

func (c *Core) pollTask(ctx context.Context, req protocol.PollTaskRequest, info TransportInfo) (protocol.PollTaskResponse, error) {
	if req.RunnerID == "" || req.SessionID == "" {
		return protocol.PollTaskResponse{}, ErrRunnerSessionRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "poll", info, authErr); err != nil {
		return protocol.PollTaskResponse{}, err
	}
	for {
		claim, ok, err := c.runners.ClaimForRunner(ctx, ClaimRequest{
			RunnerID:     req.RunnerID,
			SessionID:    req.SessionID,
			Capacity:     req.Capacity,
			Capabilities: req.Capabilities,
			Now:          time.Now(),
		})
		if err != nil {
			return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
		}
		if !ok {
			return protocol.PollTaskResponse{Wait: c.pollWait}, nil
		}

		// A leased claim is a durable replay after a response-loss or process
		// restart. It must not call BuildTaskLease again: doing so would either
		// create a new lease or strand the existing fenced ownership.
		if claim.Lease != nil {
			lease := claim.Lease
			if lease.Input == nil {
				var recoverErr error
				lease, recoverErr = c.recoverTaskLease(ctx, &claim.Assignment.Task)
				if recoverErr != nil {
					return protocol.PollTaskResponse{}, recoverErr
				}
			}
			return protocol.PollTaskResponse{Lease: lease}, nil
		}

		if c.engine == nil {
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, ErrEngineNotConfigured
		}
		lease, err := c.engine.BuildTaskLease(ctx, &claim.Assignment.Task)
		switch {
		case err == nil:
			// xflow.task.dispatch spans the BuildTaskLease + FinalizeClaim
			// assignment and injects the W3C carrier the runner extracts to
			// start its execute span as a remote child.
			tracer := c.tracer
			if tracer == nil {
				tracer = tracing.NoopTracer{}
			}
			dispatchCtx, dispatchSpan := tracer.Start(ctx, "xflow.task.dispatch",
				"execution_id", string(claim.Assignment.Task.ExecutionID),
				"node_name", claim.Assignment.Task.NodeName,
			)
			lease.TraceCarrier = tracing.InjectCarrier(dispatchCtx)
			if err := c.runners.FinalizeClaim(dispatchCtx, claim.ClaimID, lease); err != nil {
				dispatchSpan.RecordError(err)
				dispatchSpan.End()
				_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
				return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
			}
			dispatchSpan.End()
			return protocol.PollTaskResponse{Lease: lease}, nil
		case errors.Is(err, engine.ErrLeaseAlreadyActive):
			// BuildTaskLease may already have committed a running lease when a
			// prior control-plane process died before FinalizeClaim. Recover and
			// finalize that exact fenced lease instead of waiting for its TTL.
			recovered, recoverErr := c.recoverTaskLease(ctx, &claim.Assignment.Task)
			if recoverErr == nil {
				if finalizeErr := c.runners.FinalizeClaim(ctx, claim.ClaimID, recovered); finalizeErr != nil {
					_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
					return protocol.PollTaskResponse{}, normalizeRunnerError(finalizeErr, c.logger, "poll")
				}
				return protocol.PollTaskResponse{Lease: recovered}, nil
			}
			if errors.Is(recoverErr, engine.ErrExecutionInactive) {
				_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
				continue
			}
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, recoverErr
		case errors.Is(err, engine.ErrExecutionInactive):
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
		default:
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, err
		}
	}
}

func (c *Core) recoverTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error) {
	recoverer, ok := c.engine.(leaseRecoveryEngine)
	if !ok || recoverer == nil {
		return nil, engine.ErrLeaseNotRecoverable
	}
	return recoverer.RecoverTaskLease(ctx, task)
}

func (c *Core) reportResult(ctx context.Context, req protocol.ReportResultRequest, info TransportInfo) (protocol.ReportResultResponse, error) {
	if req.RunnerID == "" || req.SessionID == "" || req.Lease == nil {
		return protocol.ReportResultResponse{}, ErrLeaseRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "report_result", info, authErr); err != nil {
		return protocol.ReportResultResponse{}, err
	}
	if c.engine == nil {
		return protocol.ReportResultResponse{}, ErrEngineNotConfigured
	}
	if err := c.runners.ValidateSession(ctx, req.RunnerID, req.SessionID); err != nil {
		return protocol.ReportResultResponse{}, normalizeRunnerError(err, c.logger, "report_result")
	}

	// Restore the runner's execute span context so the commit span is properly
	// parented. Prefer the runner-side report carrier; fall back to the dispatch
	// carrier embedded in the lease for old runners that don't send their own.
	carrier := req.TraceCarrier
	if len(carrier) == 0 {
		carrier = req.Lease.TraceCarrier
	}
	ctx = tracing.ExtractCarrier(ctx, carrier)
	tracer := c.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	_, span := tracer.Start(ctx, "xflow.task.commit",
		"execution_id", string(req.Lease.Task.ExecutionID),
		"node_name", req.Lease.Task.NodeName,
		"attempt", req.Lease.Attempt,
	)
	defer span.End()

	outcome, err := c.engine.CommitTaskResultWithOutcome(ctx, req.Lease, req.Result)
	if err != nil {
		span.RecordError(err)
	}
	if outcome.ReleasesLeasedCapacity() {
		removeSeen := outcome == engine.CommitOutcomeAccepted || outcome == engine.CommitOutcomeDuplicateTerminal || outcome == engine.CommitOutcomeExecutionInactive
		if err := c.runners.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID:     req.RunnerID,
			SessionID:    req.SessionID,
			AssignmentID: BuildAssignmentID(&req.Lease.Task),
			LeaseID:      req.Lease.LeaseID,
			LeaseToken:   req.Lease.LeaseToken,
			RemoveSeen:   removeSeen,
		}); err != nil {
			return protocol.ReportResultResponse{}, normalizeRunnerError(err, c.logger, "report_result")
		}
	}
	if err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			return protocol.ReportResultResponse{Accepted: false, Error: err.Error()}, err
		}
		return protocol.ReportResultResponse{}, normalizeRunnerError(err, c.logger, "report_result")
	}
	return protocol.ReportResultResponse{Accepted: true}, nil
}

// normalizeRunnerError maps an error returned by Core logic to the message a
// client should see. Known transport-agnostic sentinel errors (and engine
// lease-token errors) are returned verbatim so callers receive actionable
// messages like "runner not found". Any other error is collapsed to the
// generic ErrInternalServer: the full error never reaches a client, since it
// may contain Redis error text, internal paths, or backend details that aid
// reconnaissance. The original error is logged server-side (with op) for the
// default branch only — known sentinels are expected outcomes and not logged.
func normalizeRunnerError(err error, logger engine.Logger, op string) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrRunnerIDRequired),
		errors.Is(err, ErrRunnerSessionRequired),
		errors.Is(err, ErrConcurrencyRequired),
		errors.Is(err, ErrInvalidTenant),
		errors.Is(err, ErrRunnerNotFound),
		errors.Is(err, ErrLeaseRequired),
		errors.Is(err, ErrEngineNotConfigured),
		errors.Is(err, ErrUnauthenticated),
		errors.Is(err, ErrRunnerSessionStale),
		errors.Is(err, engine.ErrInvalidLeaseToken):
		return err
	default:
		if logger != nil {
			logger.Error("runner op failed", "op", op, "err", err)
		}
		return ErrInternalServer
	}
}

func tenantIDs(strs []string) []tenant.TenantID {
	if len(strs) == 0 {
		return nil
	}
	out := make([]tenant.TenantID, 0, len(strs))
	seen := make(map[tenant.TenantID]struct{})
	for _, s := range strs {
		if s == "" {
			continue
		}
		t := tenant.TenantID(s)
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}
