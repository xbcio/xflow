package apiserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Principal is the authenticated identity of a caller. It is produced by an
// Authenticator (which verifies the credential) and consumed by an Authorizer
// (which decides allow/deny per operation+resource). The subject, tenant, and
// scopes come from the server-side identity source — never from the request
// body — so a caller cannot self-report another principal.
type Principal struct {
	Subject  string
	TenantID string
	Scopes   []string
}

// HasScope reports whether the principal holds a scope.
func (p Principal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// AuthorizationRequest describes one authorization decision. Operation and
// Resource/WorkflowID/ExecutionID are resolved by the route layer from the
// request path and method before the handler runs; Principal is the verified
// identity. Default-deny: an empty decision is Deny.
type AuthorizationRequest struct {
	Principal   Principal
	Operation   string
	WorkflowID  string
	ExecutionID string
	Resource    string
}

// Decision is the outcome of an authorization check.
type Decision string

const (
	DecisionAllow Decision = "allow"
	DecisionDeny  Decision = "deny"
)

// Authorizer decides whether a principal may perform an operation on a
// resource. It must default to Deny. Implementations may be a static scope
// map (G1 single-tenant reference) or a real policy engine (G2/G3).
type Authorizer interface {
	Authorize(ctx context.Context, req AuthorizationRequest) (Decision, error)
}

// Operation vocabulary. The route layer resolves each inbound request to one
// of these before invoking the handler.
const (
	OpWorkflowCreate  = "workflow.create"
	OpWorkflowInvoke   = "workflow.invoke"
	OpWorkflowRead     = "workflow.read"
	OpExecutionRead    = "execution.read"
	OpExecutionSignal  = "execution.signal"
	OpExecutionRevoke  = "execution.revoke"
	OpExecutionCancel  = "execution.cancel"
	OpDeadLetterList   = "deadletter.list"
	OpDeadLetterReplay = "deadletter.replay"
	OpManagementRead   = "management.read"
	OpManagementWrite  = "management.write"
)

// scopeForOperation maps an operation to the scope it requires. A principal
// must hold the scope to be allowed. This is the G1 single-tenant reference
// policy; G2/G3 may substitute a richer authorizer.
func scopeForOperation(op string) string {
	switch op {
	case OpWorkflowCreate, OpWorkflowInvoke, OpWorkflowRead:
		return "workflow"
	case OpExecutionRead, OpExecutionSignal, OpExecutionRevoke, OpExecutionCancel:
		return "execution"
	case OpDeadLetterList:
		return "deadletter.list"
	case OpDeadLetterReplay:
		return "deadletter.replay"
	case OpManagementRead:
		return "management.read"
	case OpManagementWrite:
		return "management.write"
	default:
		return ""
	}
}

// ScopeAuthorizer allows a principal whose scopes include the operation's
// required scope. It defaults to Deny and denies cross-execution access only
// at the resource level when a future tenant-bound authorizer is wired (G1
// single-tenant does not enforce per-execution ownership, which is the host's
// responsibility).
type ScopeAuthorizer struct{}

// Authorize returns Allow iff the principal holds the scope the operation
// requires; otherwise Deny. The decision is deterministic and does not depend
// on request timing.
func (ScopeAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) (Decision, error) {
	scope := scopeForOperation(req.Operation)
	if scope == "" {
		return DecisionDeny, nil
	}
	if !req.Principal.HasScope(scope) {
		return DecisionDeny, nil
	}
	return DecisionAllow, nil
}

// PrincipalAuthenticator verifies a credential and returns a trusted Principal.
// It replaces the bare WorkflowAuthenticator for the G1 authz path: the
// authenticator must return the principal, not just nil/error, so the
// authorizer and audit log have a verified identity.
type PrincipalAuthenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// BearerPrincipalAuth maps a static bearer token to a subject + scopes. The
// token comparison is sha256 + constant-time to prevent enumeration. The
// subject and scopes are configured server-side, so the caller cannot
// self-report them.
type BearerPrincipalAuth struct {
	tokenHash [32]byte
	subject   string
	scopes    []string
}

// NewBearerPrincipalAuth creates a BearerPrincipalAuth that accepts the given
// token and maps it to the given subject + scopes. The token must have at
// least 128 bits of entropy.
func NewBearerPrincipalAuth(token, subject string, scopes []string) *BearerPrincipalAuth {
	h := sha256.Sum256([]byte(token))
	return &BearerPrincipalAuth{tokenHash: h, subject: subject, scopes: scopes}
}

// Authenticate returns the configured Principal when the request carries the
// expected bearer token; otherwise ErrWorkflowUnauthenticated.
func (a *BearerPrincipalAuth) Authenticate(r *http.Request) (Principal, error) {
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	token := strings.TrimPrefix(hdr, "Bearer ")
	if token == "" {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	candidate := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(candidate[:], a.tokenHash[:]) != 1 {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	return Principal{Subject: a.subject, Scopes: a.scopes}, nil
}

// disabledPrincipalAuth is the dev authenticator that returns an anonymous
// principal with the wildcard scope. It must NOT be the production default.
type disabledPrincipalAuth struct{}

func (disabledPrincipalAuth) Authenticate(*http.Request) (Principal, error) {
	return Principal{Subject: "anonymous", Scopes: []string{"*"}}, nil
}

// DisabledPrincipalAuth returns the dev anonymous authenticator. It must not
// be used in production; Config.RequireWorkflowAuth enforces a real one.
func DisabledPrincipalAuth() PrincipalAuthenticator { return disabledPrincipalAuth{} }

// AuditEvent is one append-only audit record. Sensitive fields (token, full
// signal payload, approval data) must never appear here.
type AuditEvent struct {
	RequestID   string
	Principal   string
	TenantID    string
	Operation   string
	Resource    string
	WorkflowID  string
	ExecutionID string
	Decision    Decision
	Reason      string // deny reason code, not free text
	Outcome     string // admitted/denied/reconciled
	TraceID     string
	Timestamp   time.Time
}

// AuditSink is an append-only projection of authorization + mutation events.
// For G1 the in-memory/stdout sink is the projection; a durable sink (SQL) is
// the authoritative reconcile target and must be configured in production.
// A failing sink must fail-closed for mutations (admission written before the
// operation, outcome appended after).
type AuditSink interface {
	Append(ctx context.Context, ev AuditEvent) error
}

// InMemoryAuditSink is a dev/test audit sink that records events in memory.
// It is not authoritative and must not be the sole record in production.
type InMemoryAuditSink struct {
	events []AuditEvent
}

// NewInMemoryAuditSink returns an empty in-memory audit sink.
func NewInMemoryAuditSink() *InMemoryAuditSink { return &InMemoryAuditSink{} }

func (s *InMemoryAuditSink) Append(_ context.Context, ev AuditEvent) error {
	s.events = append(s.events, ev)
	return nil
}

// Events returns a copy of the recorded audit events.
func (s *InMemoryAuditSink) Events() []AuditEvent {
	out := make([]AuditEvent, len(s.events))
	copy(out, s.events)
	return out
}

// ErrAuditUnavailable is returned when the audit sink cannot accept an event.
// Mutations must fail-closed when the admission audit cannot be persisted.
var ErrAuditUnavailable = errors.New("apiserver: audit sink unavailable")
// authzContextKey carries the resolved Principal + AuthorizationRequest
// through the handler chain so handlers don't re-resolve operation/resource.
type authzContextKey struct{}

// principalFromRequest returns the Principal stored by the authz middleware.
func principalFromRequest(r *http.Request) (Principal, bool) {
	p, ok := r.Context().Value(authzContextKey{}).(Principal)
	return p, ok
}

// newRequestID returns a request id from the inbound header or a generated
// value. It is bounded in length and used only for audit correlation — never
// trusted as an identity.
func newRequestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		if len(id) > 128 {
			id = id[:128]
		}
		return id
	}
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}
