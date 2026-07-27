package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// maxReplayReasonLen bounds the operator-supplied reason recorded in the
// audit receipt. A longer reason is rejected as invalid_request rather than
// silently truncated, so operators cannot smuggle oversized payloads.
const maxReplayReasonLen = 1024

// DeadLetterReplayPrincipal is the authenticated identity injected by the
// caller (B3 authz for the HTTP API; the CLI injects a "cli:<user>" identity
// for the G0 maintenance path). It must never come from unverified free text
// in the request body.
type DeadLetterReplayPrincipal struct {
	Subject   string
	Namespace string
	Scopes    []string
}

// HasScope reports whether the principal holds a scope.
func (p DeadLetterReplayPrincipal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// ScopeDeadLetterReplay is the scope required to replay a dead-letter entry.
const ScopeDeadLetterReplay = "deadletter.replay"

// DeadLetterManager is the single outlet for dead-letter replay and listing.
// It wraps the engine.DeadLetterStore (which owns the Redis atomic contract
// and the authoritative receipt) with request validation, authorization,
// metrics, and an audit projection. CLI and HTTP API callers must go through
// the manager rather than the raw store so the atomicity, metric, and audit
// contracts are owned in one place.
//
// The Redis receipt written by the store is authoritative; the audit sink is a
// secondary projection (stdout/logger for G0, SQL for G1 reconcile). An
// unavailable audit sink does not block replay — the authoritative receipt
// survives in Redis.
type DeadLetterManager struct {
	store   engine.DeadLetterStore
	metrics engine.OutboxObserver // may be nil (e.g. one-shot CLI)
	audit   engine.DeadLetterAuditSink
}

// NewDeadLetterManager constructs a manager over the given store. metrics and
// audit may be nil.
func NewDeadLetterManager(store engine.DeadLetterStore, metrics engine.OutboxObserver, audit engine.DeadLetterAuditSink) *DeadLetterManager {
	return &DeadLetterManager{store: store, metrics: metrics, audit: audit}
}

// List returns one page of dead-lettered entries for an execution.
func (m *DeadLetterManager) List(ctx context.Context, id types.ExecutionID, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	return m.store.ListDeadLetters(ctx, id, page)
}

// Replay performs an activation-safe, idempotent replay. The principal must
// hold the deadletter.replay scope (G1: enforced here; G0 maintenance env may
// pass a principal that already has it). The operator recorded in the receipt
// is the principal's subject, never req.Operator from the request body. Every
// outcome (including invalid_request and unauthorized) is recorded at the
// single metric + audit outlet so CLI and API paths produce identical telemetry.
func (m *DeadLetterManager) Replay(ctx context.Context, principal DeadLetterReplayPrincipal, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	if req.ExecutionID == "" || req.EntryID == "" {
		return m.finish(ctx, engine.ReplayDeadLetterResult{Outcome: engine.ReplayInvalidRequest, ExecutionID: req.ExecutionID}, req, nil)
	}
	if req.Reason == "" {
		return m.finish(ctx, engine.ReplayDeadLetterResult{Outcome: engine.ReplayInvalidRequest, ExecutionID: req.ExecutionID}, req, errors.New("deadletter: reason is required"))
	}
	if len(req.Reason) > maxReplayReasonLen {
		return m.finish(ctx, engine.ReplayDeadLetterResult{Outcome: engine.ReplayInvalidRequest, ExecutionID: req.ExecutionID}, req, errors.New("deadletter: reason exceeds length limit"))
	}
	// Operator is injected from the authenticated principal; req.Operator is
	// ignored so callers cannot self-report identity.
	req.Operator = principal.Subject
	// G0: authorization is enforced by the maintenance environment. G1 adds
	// the B3 authorizer. A principal without the replay scope is rejected
	// here once B3 is wired; for G0 the caller injects a principal that holds
	// it. The unauthorized outcome never reaches Redis.
	if !principal.HasScope(ScopeDeadLetterReplay) {
		return m.finish(ctx, engine.ReplayDeadLetterResult{Outcome: engine.ReplayUnauthorized, ExecutionID: req.ExecutionID}, req, nil)
	}

	// Namespace double-check: the authoritative namespace isolation is enforced by
	// the store layer (Redis keys are prefixed by namespace). This is a fail-closed
	// guard so the manager rejects a replay when the injected context namespace
	// does not match the server-issued principal.Namespace before touching Redis.
	// An empty principal.Namespace skips the check for G0 CLI backward
	// compatibility; the store still scopes by the context namespace.
	ctxNamespace := namespace.FromContext(ctx)
	if principal.Namespace != "" && namespace.Namespace(principal.Namespace) != ctxNamespace {
		return m.finish(ctx, engine.ReplayDeadLetterResult{Outcome: engine.ReplayUnauthorized, ExecutionID: req.ExecutionID}, req, nil)
	}

	res, err := m.store.ReplayDeadLetter(ctx, req)
	if err != nil {
		if m.metrics != nil {
			m.metrics.OnOutboxError(ctx, "replay", err)
		}
		return engine.ReplayDeadLetterResult{}, err
	}
	return m.finish(ctx, res, req, nil)
}

// finish records the outcome at the single metric + audit outlet and returns
// the result (and optional validation error) to the caller.
func (m *DeadLetterManager) finish(ctx context.Context, res engine.ReplayDeadLetterResult, req engine.ReplayDeadLetterRequest, validationErr error) (engine.ReplayDeadLetterResult, error) {
	if m.metrics != nil {
		m.metrics.OnOutboxReplayed(ctx, res.Outcome)
		// The pending/dead-lettered gauge is owned by the outbox dispatcher's
		// periodic backlog scan (OnOutboxPending), not the replay path: replay
		// moves one entry from dead back to ready, but the manager does not have
		// the aggregate backlog snapshot here. Updating the gauge would require
		// an extra store read or a new observer method; we deliberately keep the
		// gauge emission in the dispatcher's existing scan to avoid
		// over-engineering and to preserve a single source of truth for backlog
		// metrics.
	}
	if m.audit != nil {
		_ = m.audit.RecordReplay(ctx, res, req)
	}
	return res, validationErr
}

// stdoutDeadLetterAuditSink is a G0 audit projection that writes one JSON line
// per replay to an io.Writer. It is NOT authoritative — the Redis receipt is.
type stdoutDeadLetterAuditSink struct {
	w  func(string)
	ts func() time.Time
}

// NewStdoutDeadLetterAuditSink returns a DeadLetterAuditSink that projects
// replay receipts to the given writer function (e.g. log.Println).
func NewStdoutDeadLetterAuditSink(write func(string)) engine.DeadLetterAuditSink {
	return &stdoutDeadLetterAuditSink{w: write, ts: time.Now}
}

func (s *stdoutDeadLetterAuditSink) RecordReplay(_ context.Context, res engine.ReplayDeadLetterResult, req engine.ReplayDeadLetterRequest) error {
	if s.w == nil {
		return nil
	}
	// Do not record the reason/operator verbatim if they could carry sensitive
	// content; reason is operator-supplied free text bounded by length. We log
	// outcome + audit_id + node + activation + operator subject, not payload.
	parts := []string{
		`"event":"deadletter_replay"`,
		`"outcome":"` + string(res.Outcome) + `"`,
		`"audit_id":"` + res.AuditID + `"`,
		`"execution":"` + string(res.ExecutionID) + `"`,
		`"entry":"` + req.EntryID + `"`,
		`"node":"` + res.NodeID + `"`,
		`"activation":"` + res.ActivationID + `"`,
		`"operator":"` + req.Operator + `"`,
		`"ts":"` + s.ts().UTC().Format(time.RFC3339Nano) + `"`,
	}
	s.w("{" + strings.Join(parts, ",") + "}")
	return nil
}
