package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// Transport-agnostic outcome errors. Each transport (HTTP, gRPC) maps these to
// its own status representation so the core handling logic stays free of
// net/http and grpc/codes.
var (
	ErrRunnerIDRequired      = errors.New("runner_id is required")
	ErrRunnerSessionRequired = errors.New("runner_id and session_id are required")
	ErrConcurrencyRequired   = errors.New("runner_id and concurrency are required")
	ErrInvalidNamespace      = errors.New("invalid namespace")
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

// traceCarrierFetcher is an optional capability of EngineFacade: the concrete
// *engine.Engine exposes the W3C carrier persisted at submission so the
// dispatch span can inherit submit/invoke causality via a real W3C remote
// parent. Test doubles that do not implement it simply fall back to the poll
// context (no submit parent), preserving source compatibility.
type traceCarrierFetcher interface {
	ExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, error)
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
	for _, t := range namespaceIDs(req.Namespaces) {
		if err := namespace.Validate(t); err != nil {
			return protocol.RegisterRunnerResponse{}, fmt.Errorf("%w: %v", ErrInvalidNamespace, err)
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
		Namespaces:   namespaceIDs(req.Namespaces),
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

		// Inject the assignment's authoritative namespace so the downstream
		// ExecutionTraceCarrier / BuildTaskLease / RecoverTaskLease calls read
		// the W3C carrier and engine state from the correct Redis namespace.
		// The runner-protocol poll context (gRPC PollTask / HTTP HandlePollTask)
		// does not carry namespace — there is no principal resolver on the runner
		// protocol path. Assignment.Namespace is the submit-time authoritative
		// value recorded by the control plane from the authenticated principal,
		// so it is the correct source — NOT a client-supplied value. It is only
		// used as a span attribute and ctx injection (namespace.WithNamespace); it is
		// never placed in W3C baggage (RELEASE-GATES §4.1, cross-namespace leak
		// risk).
		if tid := claim.Assignment.Namespace; tid != "" {
			ctx = namespace.WithNamespace(ctx, tid)
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
			lease.Namespace = claim.Assignment.Namespace
			return protocol.PollTaskResponse{Lease: lease}, nil
		}

		if c.engine == nil {
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
			return protocol.PollTaskResponse{}, ErrEngineNotConfigured
		}
		tracer := c.tracer
		if tracer == nil {
			tracer = tracing.NoopTracer{}
		}
		// Inherit the workflow submit/invoke causality: extract the W3C carrier
		// persisted on the execution snapshot at submission. This is a real W3C
		// remote-parent round-trip (preserves tracestate + sampled flag), NOT a
		// trace_id/span_id string reconstruction (RELEASE-GATES §4 forbids that).
		// Falls back to the poll context (no submit parent) when the engine does
		// not expose the carrier or tracing was disabled at submission.
		dispatchCtx := ctx
		if fetcher, ok := c.engine.(traceCarrierFetcher); ok {
			if carrier, ferr := fetcher.ExecutionTraceCarrier(ctx, claim.Assignment.Task.ExecutionID); ferr == nil && len(carrier) > 0 {
				dispatchCtx = tracing.ExtractCarrier(ctx, carrier)
			}
		}
		// xflow.task.dispatch spans BuildTaskLease + FinalizeClaim (not started
		// after Build) and is parented to the submit/invoke carrier above. It
		// injects the W3C carrier the runner extracts to start its execute span
		// as a remote child of dispatch.
		dispatchCtx, dispatchSpan := tracer.Start(dispatchCtx, "xflow.task.dispatch",
			"execution_id", string(claim.Assignment.Task.ExecutionID),
			"node_name", claim.Assignment.Task.NodeName,
		)
		lease, err := c.engine.BuildTaskLease(dispatchCtx, &claim.Assignment.Task)
		switch {
		case err == nil:
			lease.TraceCarrier = tracing.InjectCarrier(dispatchCtx)
			lease.Namespace = claim.Assignment.Namespace
			if err := c.runners.FinalizeClaim(dispatchCtx, claim.ClaimID, lease); err != nil {
				dispatchSpan.RecordError(err)
				dispatchSpan.End()
				_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimRequeue)
				return protocol.PollTaskResponse{}, normalizeRunnerError(err, c.logger, "poll")
			}
			dispatchSpan.End()
			return protocol.PollTaskResponse{Lease: lease}, nil
		case errors.Is(err, engine.ErrLeaseAlreadyActive):
			dispatchSpan.End()
			// BuildTaskLease may already have committed a running lease when a
			// prior control-plane process died before FinalizeClaim. Recover and
			// finalize that exact fenced lease instead of waiting for its TTL.
			recovered, recoverErr := c.recoverTaskLease(ctx, &claim.Assignment.Task)
			if recoverErr == nil {
				recovered.Namespace = claim.Assignment.Namespace
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
			dispatchSpan.End()
			_ = c.runners.ReleaseClaim(ctx, claim.ClaimID, ReleaseClaimDrop)
		default:
			dispatchSpan.RecordError(err)
			dispatchSpan.End()
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
	tracer := c.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	// Restore the runner's execute span context so the report/commit spans are
	// properly parented. Prefer the runner-side report carrier; fall back to
	// the dispatch carrier embedded in the lease for old runners that don't
	// send their own. Real W3C ExtractCarrier round-trip (preserves tracestate
	// + sampled flag), not a trace_id/span_id string reconstruction.
	carrier := req.TraceCarrier
	if len(carrier) == 0 {
		carrier = req.Lease.TraceCarrier
	}
	ctx = tracing.ExtractCarrier(ctx, carrier)
	// xflow.task.report spans auth, session validation, lease-authority lookup,
	// commit, and capacity release. Parented to the runner's execute span via
	// the carrier above (NOT to the inbound transport span — the runner carries
	// traceparent in the request body, not gRPC metadata). The narrower
	// xflow.task.commit span below is its child.
	reportCtx, reportSpan := tracer.Start(ctx, "xflow.task.report",
		"runner_id", req.RunnerID,
	)
	defer reportSpan.End()
	ctx = reportCtx

	// Auth and session validation MUST precede the lease-authority lookup. A
	// runner that fails auth or holds a stale session must not learn anything
	// about lease state, and the directory query must run only for an
	// authenticated, live session (2026-07-21 B1: ordering + fail-closed).
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

	// LeaseLookup is a mandatory production capability on the report path. The
	// lease JSON the runner echoes is unsigned and client-mutable, so it MUST
	// NOT select the commit namespace's namespace. The directory resolves the
	// authoritative finalized lease, fenced to (runner, session). A directory
	// that cannot resolve leases, or a lookup that does not hit a finalized
	// lease for THIS runner+session, is a fencing rejection — fail closed.
	// There is NO fallback to req.Lease: the old "degraded" fallback let a
	// runner report another runner's lease and commit cross-namespace
	// (2026-07-21 cross-runner lease-swap probe).
	lookup, hasLookup := c.runners.(LeaseLookup)
	if !hasLookup {
		if c.logger != nil {
			c.logger.Error("report directory does not implement LeaseLookup; rejecting (fail closed)",
				"op", "report_result", "runner_id", req.RunnerID)
		}
		return protocol.ReportResultResponse{Accepted: false, Error: engine.ErrInvalidLeaseToken.Error()}, engine.ErrInvalidLeaseToken
	}
	resolved, found, lerr := lookup.LookupLease(ctx, req.RunnerID, req.SessionID, LeaseLookupKey{
		AssignmentID: BuildAssignmentID(&req.Lease.Task),
		LeaseID:      req.Lease.LeaseID,
		LeaseToken:   req.Lease.LeaseToken,
	})
	if lerr != nil {
		return protocol.ReportResultResponse{}, normalizeRunnerError(lerr, c.logger, "report_result")
	}
	if !found {
		// ok=false: no finalized lease matches this runner+session (wrong
		// runner/session, token/leaseID mismatch, already released, or not
		// found). Fencing rejection — the commit must NOT run and no namespace
		// namespace is selected from the echoed lease.
		if c.logger != nil {
			c.logger.Warn("report rejected: authoritative lease not found for runner+session (fail closed)",
				"op", "report_result", "runner_id", req.RunnerID)
		}
		return protocol.ReportResultResponse{Accepted: false, Error: engine.ErrInvalidLeaseToken.Error()}, engine.ErrInvalidLeaseToken
	}
	// Immutable fields must match what the runner echoed. A mismatch on
	// ExecutionID/NodeName/NodeIdx/Attempt/LeaseID/LeaseToken means the runner
	// is reporting against a different lease than the one it was issued —
	// reject with the existing fencing sentinel. Namespace is NOT in this set:
	// an old runner may echo a stale/missing Namespace, so namespace is always taken
	// from the authoritative lease (logged if it disagrees, but not rejected).
	if leaseImmutableMismatch(resolved, req.Lease) {
		return protocol.ReportResultResponse{Accepted: false, Error: engine.ErrInvalidLeaseToken.Error()}, engine.ErrInvalidLeaseToken
	}
	if req.Lease.Namespace != resolved.Namespace && c.logger != nil {
		c.logger.Warn("report namespace mismatch: runner echoed namespace differs from authoritative lease; using authoritative",
			"runner_id", req.RunnerID,
			"echoed_namespace", string(req.Lease.Namespace),
			"authoritative_namespace", string(resolved.Namespace))
	}
	authoritativeLease := resolved
	// Inject the authoritative namespace so the commit path reads/writes from the
	// correct Redis namespace. ctx injection only (namespace.WithNamespace); the
	// namespace is never placed in W3C baggage (RELEASE-GATES §4.1).
	if authoritativeLease.Namespace != "" {
		ctx = namespace.WithNamespace(ctx, authoritativeLease.Namespace)
	}

	_, span := tracer.Start(ctx, "xflow.task.commit",
		"execution_id", string(authoritativeLease.Task.ExecutionID),
		"node_name", authoritativeLease.Task.NodeName,
		"attempt", authoritativeLease.Attempt,
	)
	defer span.End()

	outcome, err := c.engine.CommitTaskResultWithOutcome(ctx, authoritativeLease, req.Result)
	if err != nil {
		span.RecordError(err)
	}
	if outcome.ReleasesLeasedCapacity() {
		removeSeen := outcome == engine.CommitOutcomeAccepted || outcome == engine.CommitOutcomeDuplicateTerminal || outcome == engine.CommitOutcomeExecutionInactive
		if err := c.runners.ReleaseLeased(ctx, ReleaseLeasedRequest{
			RunnerID:     req.RunnerID,
			SessionID:    req.SessionID,
			AssignmentID: BuildAssignmentID(&authoritativeLease.Task),
			LeaseID:      authoritativeLease.LeaseID,
			LeaseToken:   authoritativeLease.LeaseToken,
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

// leaseImmutableMismatch reports whether the lease a runner echoed back differs
// from the authoritative finalized lease on any immutable identity field that is
// part of the runner JSON contract. The runner must report against the exact
// lease it was issued; a mismatch on ExecutionID/NodeName/NodeIdx/Attempt/
// LeaseID/LeaseToken is a fencing violation (engine.ErrInvalidLeaseToken).
//
// ActivationID and AutoDepth are intentionally excluded: they are tagged
// json:"-" on engine.Task (internal cyclic metadata) and are NOT carried in
// the runner-facing lease JSON, so a runner echo always round-trips them as 0.
// Their authority is guaranteed instead by committing with the authoritative
// (resolved) lease, whose Task carries the real values, rather than the echoed
// req.Lease. Namespace is also excluded — taken unconditionally from the
// authoritative lease so an old runner that echoes a stale/missing Namespace is
// not penalized.
func leaseImmutableMismatch(authoritative, echoed *engine.TaskLease) bool {
	if authoritative == nil || echoed == nil {
		return false
	}
	return authoritative.Task.ExecutionID != echoed.Task.ExecutionID ||
		authoritative.Task.NodeName != echoed.Task.NodeName ||
		authoritative.Task.NodeIdx != echoed.Task.NodeIdx ||
		authoritative.Attempt != echoed.Attempt ||
		authoritative.LeaseID != echoed.LeaseID ||
		authoritative.LeaseToken != echoed.LeaseToken
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
		errors.Is(err, ErrInvalidNamespace),
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

func namespaceIDs(strs []string) []namespace.Namespace {
	if len(strs) == 0 {
		return nil
	}
	out := make([]namespace.Namespace, 0, len(strs))
	seen := make(map[namespace.Namespace]struct{})
	for _, s := range strs {
		if s == "" {
			continue
		}
		t := namespace.Namespace(s)
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}
