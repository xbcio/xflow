package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// Min/MaxMetricsReportInterval bound what the server will ask a runner to use.
// The floor keeps a mis-typed "1s" from turning the fleet into a load
// generator; the ceiling keeps a report window from outliving the Redis
// retention (3 × DefaultRunnerLiveTTL = 90s is shorter than 300s, so a runner
// at the ceiling relies on IsLive rather than the key surviving — which is why
// the ceiling is a documented maximum, not a recommendation).
const (
	MinMetricsReportInterval = 5 * time.Second
	MaxMetricsReportInterval = 300 * time.Second
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
	// ErrStaleGeneration is returned when an entry seed carries an activation
	// generation older than the currently-assigned generation AND targets an
	// admission key that has not yet been accepted. It fences a forged or stale
	// runner from seeding a fresh execution (spec §11.6). A stale seed for an
	// already-accepted key is NOT an error — it is duplicate-accepted so the
	// runner can commit its offset.
	ErrStaleGeneration = errors.New("stale activation generation")
	// ErrEntrySeedWorkflowUnknown is returned when a remote entry seed cannot be
	// resolved to a registered workflow graph + entry unit. The seed is rejected
	// (fail closed): a seed whose workflow is not registered, or whose entry unit
	// is not found in the compiled graph, must never be admitted with no
	// downstream fan-out (spec §11.5). Surfaced by the transport as 404/409.
	ErrEntrySeedWorkflowUnknown = errors.New("entry seed workflow or unit unknown")
	// ErrMissingWorkflowVersion is returned when an ActivationAck does not carry
	// the required workflow_version field. Mapped to 400 by the HTTP transport.
	ErrMissingWorkflowVersion = errors.New("activation ack: missing required field workflow_version")
	// ErrInternalServer is the generic message returned to clients for any
	// error that is not a recognised transport-agnostic sentinel. The full
	// error is logged server-side; clients must never see internal stack
	// traces, Redis errors, or backend paths.
	ErrInternalServer = errors.New("internal server error")
	// ErrMetricsProxyDisabled means this server was built without the runner
	// metrics proxy, so the endpoint exists but has nowhere to put a report.
	ErrMetricsProxyDisabled = errors.New("runner metrics proxy is not enabled")
	// ErrMetricsPayloadTooLarge means the report exceeded
	// protocol.MaxRunnerMetricsBytes. The body is not decoded.
	ErrMetricsPayloadTooLarge = errors.New("runner metrics payload too large")
	// ErrMetricsEncodingUnsupported means the report was not gzip'd.
	ErrMetricsEncodingUnsupported = errors.New("runner metrics payload must be gzip encoded")
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
	// timeoutObserver, when set, records node execution timeout events emitted
	// from the server side. The only server-side origin is the renewLease
	// backstop that commits a terminal timeout when a lease's ExecutionDeadline
	// has passed (see group_control_loop.go). Nil is safe: observeNodeTimeout
	// nil-guards, so a Core with no metrics wired is byte-identical to before.
	timeoutObserver execution.TimeoutObserver
	// tracer instruments the runner protocol dispatch and commit path.
	// NoopTracer when tracing is disabled.
	tracer tracing.Tracer
	// entryReconciler, when non-nil, is the node-generic entry-activation
	// reconciler. When set it supplies heartbeat activation directives.
	// Nil-guarded; wired by the ControlPlane.
	entryReconciler *EntryActivationReconciler
	// entryActivations, when non-nil, is the durable EntryActivation store used
	// to fence entry seeds by activation generation (spec §11.6). Nil disables
	// generation fencing — every seed is admitted (legacy / locally-hosted
	// triggers with no remote activation).
	entryActivations engine.EntryActivationStore
	// workflowRegistry, when non-nil, is the durable registry of compiled
	// workflow graphs. Threaded from the ControlPlane so later tasks can resolve
	// a graph on the seed path to derive entry activations. Nil means no registry
	// is configured.
	workflowRegistry backend.WorkflowRegistry
	// supplyHinter, when non-nil, computes the per-runner supply hints
	// piggybacked on the heartbeat response. Nil (the default whenever
	// Config.Supplies is not provided) means heartbeats never carry
	// SupplyHints — byte-identical to the pre-Task-19 behavior.
	supplyHinter *SupplyHinter
	// supplyObserved, when non-nil, records each runner's reported "applied
	// content hash per supply" from HeartbeatRequest.SupplyObserved. Nil means
	// the report is accepted but discarded (no aggregation), which is safe: it
	// is a diagnostic read, never a gate on anything.
	supplyObserved SupplyObservedSink
	// supplyEncryptor, when non-nil, enables AES-256-GCM encryption of supply
	// content. The key is delivered to runners on registration and rotated via
	// heartbeat responses.
	supplyEncryptor *SupplyEncryptor
	// metricsInbox, when set, retains proxied runner metrics so the server's
	// own /metrics can expose them. Nil disables the report endpoint: a runner
	// in another network domain cannot be scraped, but a server that was not
	// built to proxy should say so rather than silently discard.
	metricsInbox *MetricsInbox
	// metricsReportInterval is the cadence this server asks runners to report
	// metrics at. Zero means "no opinion" — the runner keeps its local default.
	// Negative suspends reporting fleet-wide.
	metricsReportInterval time.Duration
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

// observeNodeTimeout records a server-detected node execution timeout. The
// label set is exactly {node_type, source="server"} -- never node name,
// execution ID, params, or output. nil-guarded so a Core without a metrics
// observer is byte-identical to before this feature.
func (c *Core) observeNodeTimeout(ctx context.Context, nodeType string) {
	if c.timeoutObserver == nil {
		return
	}
	c.timeoutObserver.OnNodeExecutionTimeout(ctx, nodeType, "server")
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
	policy, authErr := c.authn().AuthenticateRegister(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "register", info, authErr); err != nil {
		return protocol.RegisterRunnerResponse{}, err
	}
	// Shape first, then entitlement. A runner's namespace membership is decided
	// by the server's policy, never by what the runner declares: the declared
	// value is stored verbatim and ClaimForRunner filters task dispatch by it,
	// so an unchecked declaration decides which namespace's work this runner is
	// handed.
	//
	// The gate below runs against the *effective* set, not the *declared* one.
	// A runner that declares no namespace (or only ""s, which namespaceIDs
	// drops) is not asking for "no namespace" — normalizeRunnerNamespaces
	// downstream in the runner directory resolves that to [namespace.Default]
	// regardless, so it is asking for default. If the entitlement check ran
	// against the raw (possibly empty) declaration instead, an empty
	// declaration would skip the loop entirely and register into default with
	// zero policy checks, even under a policy whose AllowedNamespaces does not
	// include default. Checking and persisting the same effective value keeps
	// "what got checked" and "what got stored" from diverging.
	requested := namespaceIDs(req.Namespaces)
	effective := requested
	if len(effective) == 0 {
		effective = []namespace.Namespace{namespace.Default}
	}
	for _, t := range effective {
		if err := namespace.Validate(t); err != nil {
			return protocol.RegisterRunnerResponse{}, fmt.Errorf("%w: %v", ErrInvalidNamespace, err)
		}
		if !policy.AllowsNamespace(t) {
			// Namespace is the authorization boundary in this system, so a
			// denial on it must be observable. It deliberately does not go
			// through authDeny: that collapses every error into
			// ErrUnauthenticated, and "authenticated but not entitled" is a
			// different answer than "not authenticated" — callers should be
			// able to tell them apart.
			c.observeAuth(ctx, "register", "deny_namespace")
			if c.logger != nil {
				c.logger.Error("auth_denied",
					"op", "register", "reason", "namespace_not_granted",
					"runner", req.RunnerID, "policy", policy.Name, "namespace", string(t))
			}
			return protocol.RegisterRunnerResponse{}, fmt.Errorf("%w: policy %q does not grant namespace %q", ErrAuthNamespaceDenied, policy.Name, t)
		}
	}
	session, err := c.runners.Register(ctx, RegisterRunnerRequest{
		RunnerID:     req.RunnerID,
		Capacity:     req.Concurrency,
		Labels:       req.Labels,
		Capabilities: req.Capabilities,
		Policy:       policy,
		Namespaces:   effective,
		Now:          time.Now(),
	})
	if err != nil {
		return protocol.RegisterRunnerResponse{}, normalizeRunnerError(err, c.logger, "register")
	}
	// Reconnect reconciliation: renew leases for activations the runner still
	// reports hosting (generation unchanged) and revoke assignments it no longer
	// hosts so they become reassignable. Best-effort — a reconcile failure must
	// not fail an otherwise-valid registration; the periodic reconcile loop is a
	// backstop. Only runs when the node-generic reconciler is wired.
	if c.entryReconciler != nil {
		if err := c.entryReconciler.ReconcileRunnerInventory(ctx, req.RunnerID, req.Activations, time.Now()); err != nil && c.logger != nil {
			c.logger.Warn("register inventory reconcile failed", "runner_id", req.RunnerID, "err", err)
		}
	}
	resp := protocol.RegisterRunnerResponse{RunnerID: req.RunnerID, SessionID: session.SessionID}
	if req.SupportsEncryption && c.supplyEncryptor != nil {
		resp.SupplyKey = c.supplyEncryptor.KeyForRunner()
	}
	return resp, nil
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
	resp := protocol.HeartbeatResponse{ServerTime: time.Now().Unix()}
	// The node-generic entry reconciler supplies activation directives when wired.
	if c.entryReconciler != nil {
		resp.Activations = c.entryReconciler.DirectivesForRunner(req.RunnerID)
	}
	// Supply hints/observed reporting: both optional, wired only when
	// Config.Supplies is provided (see ControlPlane assembly). Nil means this
	// heartbeat's request/response bodies are unaffected.
	if c.supplyHinter != nil {
		resp.SupplyHints = c.supplyHinter.HintsForRunner(ctx, req.RunnerID)
	}
	if len(req.SupplyObserved) > 0 && c.supplyObserved != nil {
		// Record what this runner has actually applied. Best-effort: an observed
		// report is diagnostic, never a gate on the heartbeat succeeding.
		c.supplyObserved.Record(req.RunnerID, req.SupplyObserved)
	}
	if c.supplyEncryptor != nil {
		if rot := c.supplyEncryptor.RotationForHolder(req.SupplyKeyID); rot != "" {
			resp.SupplyKeyRotation = rot
		}
	}
	if secs := clampMetricsReportInterval(c.metricsReportInterval); secs != 0 {
		resp.MetricsReportIntervalSeconds = secs
	}
	return resp, nil
}

func (c *Core) activationAck(ctx context.Context, req protocol.ActivationAck, info TransportInfo) error {
	if req.RunnerID == "" || req.SessionID == "" {
		return ErrRunnerSessionRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(req.RunnerID, req.AuthToken, info)
	if err := c.authDeny(ctx, req.RunnerID, req.AuthToken, "activation_ack", info, authErr); err != nil {
		return err
	}
	if c.entryReconciler == nil {
		return nil
	}
	// Resolve the namespace server-side from the runner's registration record
	// (never from the client body). A runner registers with one or more
	// namespaces; we probe each with a precise Get to find the matching
	// activation. This is O(runner namespace count) exact Gets, NOT a scan.
	namespaces := c.runnerNamespaces(ctx, req.RunnerID)
	for _, ns := range namespaces {
		nsCtx := namespace.WithNamespace(ctx, ns)
		err := c.entryReconciler.MarkActivationFailed(nsCtx, req.RunnerID, req)
		if err != nil {
			return normalizeRunnerError(err, c.logger, "activation_ack")
		}
	}
	return nil
}

// reportMetrics retains one runner's Prometheus snapshot.
//
// Authentication runs on every call for the same reason the other four runner
// endpoints do it (core.go heartbeat/pollTask/reportResult/activationAck): HTTP
// connections carry no identity, so the only thing the server can rely on is
// what arrived inside THIS request. sessionID is an identifier, not an
// authenticator — it does not rotate and can be read off a log — so accepting
// it alone would downgrade this endpoint to "anyone who knows the session id
// can write". ValidateSession is the separate, additional check that a zombie
// process holding a still-valid token cannot keep overwriting the live
// session's data.
//
// The token is never logged, quoted, or returned: per the organization's
// security policy it is on the absolute log blacklist. authDeny logs
// TokenFingerprint(token) instead, and nothing here dumps request headers.
func (c *Core) reportMetrics(ctx context.Context, runnerID, sessionID, token string, body []byte, info TransportInfo) error {
	if c.metricsInbox == nil {
		return ErrMetricsProxyDisabled
	}
	if runnerID == "" || sessionID == "" {
		return ErrRunnerSessionRequired
	}
	_, authErr := c.authn().AuthenticateOngoing(runnerID, token, info)
	if err := c.authDeny(ctx, runnerID, token, "report_metrics", info, authErr); err != nil {
		return err
	}
	if err := c.runners.ValidateSession(ctx, runnerID, sessionID); err != nil {
		return err
	}
	// runnerID here is the authenticated identity. Everything downstream keys
	// on it, so a payload that names a different runner_id cannot forge series.
	return c.metricsInbox.Accept(ctx, runnerID, body)
}

// runnerNamespaces returns the namespace set for a runner from the directory's
// authoritative registration record. Falls back to {namespace.Default} when the
// runner is not found (e.g. expired) or registered with an empty list.
func (c *Core) runnerNamespaces(ctx context.Context, runnerID string) []namespace.Namespace {
	snap, ok := c.runners.Runner(ctx, runnerID)
	if ok && len(snap.Namespaces) > 0 {
		return snap.Namespaces
	}
	return []namespace.Namespace{namespace.Default}
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
			RunnerID:       req.RunnerID,
			SessionID:      req.SessionID,
			Capacity:       req.Capacity,
			Labels:         req.Labels,
			Capabilities:   req.Capabilities,
			ActiveLeaseIDs: req.ActiveLeaseIDs,
			Now:            time.Now(),
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
			if isGroupTask(&lease.Task) {
				// Group replay: rebuild GroupPayload from backend state.
				var recoverErr error
				lease, recoverErr = c.replayGroupLease(ctx, lease)
				if errors.Is(recoverErr, engine.ErrGroupLeaseNotActive) {
					// The group already committed and released its lease
					// between the directory handing back this durable replay
					// and the recover read. There is nothing left to run: the
					// replay is a duplicate of finished work, not a failure.
					// Drop the assignment and keep polling. Propagating this
					// as an error would return 500 to the runner, whose
					// pollLoop treats a poll error as fatal and stops claiming
					// work altogether — one lost race would idle the runner.
					_ = c.runners.ClearAssignment(ctx, BuildAssignmentID(&claim.Lease.Task))
					continue
				}
				if recoverErr != nil {
					return protocol.PollTaskResponse{}, recoverErr
				}
			} else if lease.Input == nil {
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

		// Group tasks dispatch through a separate path that builds a GroupLease
		// with the full package payload instead of a regular TaskLease.
		if isGroupTask(&claim.Assignment.Task) {
			resp, err := c.dispatchGroupLease(ctx, claim)
			if err != nil {
				return protocol.PollTaskResponse{}, err
			}
			if resp.Lease != nil {
				return resp, nil
			}
			// resp.Lease == nil with no error means the execution is inactive
			// (dropped). Loop to try the next claim.
			continue
		}

		// Batch tasks likewise dispatch through their own lease path: they name
		// a synthetic node outside the compiled graph, so BuildTaskLease
		// refuses them.
		if isBatchTask(&claim.Assignment.Task) {
			resp, err := c.dispatchSubgraphLease(ctx, claim)
			if err != nil {
				return protocol.PollTaskResponse{}, err
			}
			if resp.Lease != nil {
				return resp, nil
			}
			continue
		}

		// The span deliberately opens BEFORE BuildTaskLease and closes after
		// FinalizeClaim, so a lease built but never finalized is visible as such.
		dispatchCtx, dispatchSpan := c.startDispatchSpan(ctx, &claim.Assignment.Task)
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

// startDispatchSpan opens the xflow.task.dispatch span for one claim, parented
// to the workflow's submit/invoke causality.
//
// The parent comes from the W3C carrier persisted on the execution snapshot at
// submission: a real remote-parent round-trip that preserves tracestate and the
// sampled flag, NOT a trace_id/span_id string reconstruction (RELEASE-GATES §4
// forbids that). Falls back to the poll context — no submit parent — when the
// engine does not expose the carrier or tracing was disabled at submission.
//
// It is shared by all three dispatch paths (node, group, batch) rather than
// living inline in the node path, because the carrier injected from the returned
// context is the ONLY thing that gives a runner's execute span a remote parent:
// a lease dispatched without one produces a span with no parent at all, and the
// whole remote execution detaches from the workflow trace.
func (c *Core) startDispatchSpan(ctx context.Context, task *engine.Task) (context.Context, tracing.Span) {
	tracer := c.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	dispatchCtx := ctx
	if fetcher, ok := c.engine.(traceCarrierFetcher); ok {
		if carrier, ferr := fetcher.ExecutionTraceCarrier(ctx, task.ExecutionID); ferr == nil && len(carrier) > 0 {
			dispatchCtx = tracing.ExtractCarrier(ctx, carrier)
		}
	}
	return tracer.Start(dispatchCtx, "xflow.task.dispatch",
		"execution_id", string(task.ExecutionID),
		"node_name", task.NodeName,
	)
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

	var outcome engine.CommitOutcome
	var err error
	if req.GroupResult != nil && isGroupTask(&authoritativeLease.Task) {
		outcome, err = c.commitGroupResult(ctx, authoritativeLease, *req.GroupResult)
	} else {
		outcome, err = c.engine.CommitTaskResultWithOutcome(ctx, authoritativeLease, req.Result)
	}
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

// SeedExecutionFromEntry admits an entry-unit (single node or group node)
// result through the engine's EntryAdmissionStore. The namespace is resolved
// server-side from the request context (injected by the apiserver authz
// wrapper from the authenticated principal) and stamped onto the request — it
// is NEVER taken from a client-supplied body, so a forged or cross-namespace
// admission key fails closed.
func (c *Core) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	if c.engine == nil {
		return engine.SeedExecutionFromEntryResponse{}, ErrEngineNotConfigured
	}
	// Authoritative namespace comes from the request context, not the body.
	req.Namespace = namespace.FromContext(ctx)

	// Resolve the compiled graph + downstream topology server-side. A remote
	// runner seeds carrying only the entry-unit ID + generation; the graph,
	// entry-unit index and downstream arrivals MUST be resolved from the
	// authoritative registry so the admitted seed actually fans out downstream.
	// When no registry is wired (embedded / in-process seeds already carry a
	// resolved Graph/Downstream/EntryUnitIdx), preserve today's behavior.
	if c.workflowRegistry != nil {
		if err := c.resolveEntrySeedTopology(ctx, &req); err != nil {
			return engine.SeedExecutionFromEntryResponse{}, err
		}
	}

	// Generation fence (spec §11.6): when an activation store is configured and
	// the activation exists, a seed carrying a generation below the currently
	// assigned generation must not create a NEW execution. It is still
	// duplicate-accepted for an already-accepted admission key so the stale
	// runner can commit its Kafka offset and stop redelivering.
	if err := c.fenceEntrySeedGeneration(ctx, req); err != nil {
		return engine.SeedExecutionFromEntryResponse{}, err
	}

	return c.engine.SeedExecutionFromEntry(ctx, req)
}

// resolveEntrySeedTopology looks up the registered workflow for req and stamps
// the authoritative Graph, EntryUnitIdx and downstream arrivals onto it. It
// fails CLOSED (ErrEntrySeedWorkflowUnknown) when the workflow is not
// registered, the version/hash does not match, or the entry unit cannot be
// resolved in the compiled graph — a remote seed without a resolvable graph
// must never be admitted with no downstream fan-out (spec §11.5). Any other
// registry error is normalized to a generic internal error so backend details
// never reach the caller.
func (c *Core) resolveEntrySeedTopology(ctx context.Context, req *engine.SeedExecutionFromEntryRequest) error {
	rec, err := c.workflowRegistry.GetWorkflow(ctx, req.WorkflowID)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			return ErrEntrySeedWorkflowUnknown
		}
		return normalizeRunnerError(err, c.logger, "entry_seed_resolve")
	}
	// The seed's declared version must match the registered record. On the remote
	// path (registry present), an empty version is treated as a mismatch and
	// rejected — a remote runner MUST declare the version it seeds against, and a
	// fail-open empty-vs-registered bypass would let it fan out over the wrong
	// topology. A non-empty version that disagrees with the record is likewise
	// rejected.
	if req.WorkflowVersion == "" || (rec.Version != "" && req.WorkflowVersion != rec.Version) {
		return ErrEntrySeedWorkflowUnknown
	}
	if rec.Graph == nil {
		return ErrEntrySeedWorkflowUnknown
	}
	idx, downstream, err := deriveEntrySeedTopology(rec.Graph, req.EntryUnitID, req.Exits)
	if err != nil {
		// deriveEntrySeedTopology only returns ErrEntrySeedWorkflowUnknown-wrapped
		// errors; surface the sentinel so the transport maps it to 404/409.
		if errors.Is(err, ErrEntrySeedWorkflowUnknown) {
			return ErrEntrySeedWorkflowUnknown
		}
		return normalizeRunnerError(err, c.logger, "entry_seed_resolve")
	}
	req.Graph = rec.Graph
	req.EntryUnitIdx = idx
	req.Downstream = downstream
	return nil
}

// fenceEntrySeedGeneration enforces the generation fence for one seed request.
// It returns ErrStaleGeneration when the seed is stale AND targets an
// admission key that has not been accepted yet; it returns nil (admit) when the
// generation exactly matches the currently assigned generation, when no
// activation record for the logical entry exists, or when the admission key was
// already accepted (so a duplicate accept can proceed).
func (c *Core) fenceEntrySeedGeneration(ctx context.Context, req engine.SeedExecutionFromEntryRequest) error {
	if c.entryActivations == nil {
		return nil
	}
	key := engine.EntryActivationKey{
		Namespace:       req.Namespace,
		WorkflowID:      req.WorkflowID,
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		ReplicaIndex:    req.ReplicaIndex,
	}
	act, ok, err := c.entryActivations.Get(ctx, key)
	if err != nil {
		return normalizeRunnerError(err, c.logger, "entry_seed_fence")
	}
	exactActivation := ok
	if !ok {
		// An unknown replica must not turn the activation fence into a fail-open
		// lookup. Check the logical entry before preserving the legacy unfenced
		// path used by local entries that have no durable activation at all.
		activations, lerr := c.entryActivations.List(ctx, req.Namespace)
		if lerr != nil {
			return normalizeRunnerError(lerr, c.logger, "entry_seed_fence")
		}
		for _, sibling := range activations {
			if sibling.WorkflowID == req.WorkflowID &&
				sibling.WorkflowVersion == req.WorkflowVersion &&
				sibling.EntryUnitID == req.EntryUnitID {
				// A durable sibling governs this logical entry. Treat a seed for
				// a nonexistent replica exactly like any other stale/forged seed.
				ok = true
				break
			}
		}
		if !ok {
			return nil
		}
	}
	if exactActivation && req.Generation == act.Generation {
		// Exactly the current assigned generation — admit normally. The
		// generation is monotonic and every legitimate runner receives its
		// generation from Assign, so the current owner always carries exactly
		// act.Generation. Any other value (below = superseded runner, above =
		// impossible in honest operation, i.e. forged) is treated as stale and
		// falls through to the duplicate-accept probe below, which fails closed
		// unless the admission key was already accepted.
		return nil
	}
	// Stale generation. Only allow it through if the admission key was already
	// accepted (duplicate accept path). We probe by the deterministic execution
	// ID: if it exists, the key was accepted and the engine seed will return a
	// duplicate; otherwise the stale runner must be rejected fail-closed.
	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	if _, ierr := c.engine.Inspect(ctx, execID); ierr != nil {
		if errors.Is(ierr, engine.ErrExecutionNotFound) {
			return ErrStaleGeneration
		}
		return normalizeRunnerError(ierr, c.logger, "entry_seed_fence")
	}
	// Execution already exists → allow the duplicate accept to proceed.
	return nil
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
		errors.Is(err, ErrMissingWorkflowVersion),
		errors.Is(err, engine.ErrInvalidLeaseToken):
		return err
	default:
		if logger != nil {
			logger.Error("runner op failed", "op", op, "err", err)
		}
		return ErrInternalServer
	}
}

// clampMetricsReportInterval converts the configured cadence into the wire's
// three-state integer. Clamping happens here rather than on the runner so a
// runner can adopt whatever arrives without re-validating it, and so changing
// the bounds is a server-side deploy.
func clampMetricsReportInterval(d time.Duration) int {
	switch {
	case d == 0:
		return 0
	case d < 0:
		return -1
	case d < MinMetricsReportInterval:
		return int(MinMetricsReportInterval / time.Second)
	case d > MaxMetricsReportInterval:
		return int(MaxMetricsReportInterval / time.Second)
	default:
		return int(d / time.Second)
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
