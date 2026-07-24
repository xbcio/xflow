package apiserver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend/tenant"
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
//
// Tenant boundary (Task 7.3): Principal.TenantID is the server-issued tenant
// and is the authoritative tenant for the decision. ResourceTenant is the
// tenant of the target resource, resolved by the route layer when it can be
// looked up without a chicken-and-egg dependency on the authz decision; when
// empty (e.g. create operations, or management endpoints where the resource
// tenant is enforced downstream by a tenant-scoped store read) the
// per-resource tenant check is skipped and IDOR is enforced by the
// tenant-scoped store read (see module_management.go: a cross-tenant Inspect
// resolves to not-found → 404, never leaking existence).
type AuthorizationRequest struct {
	Principal      Principal
	Operation      string
	WorkflowID     string
	ExecutionID    string
	Resource       string
	ResourceTenant string
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

// TenantAwareAuthorizer is the G2 multi-tenant authorizer. It extends
// ScopeAuthorizer with tenant-bound enforcement:
//
//  1. Default-deny when Principal.TenantID is empty — a principal without a
//     server-issued tenant is never allowed. This is fail-closed: every
//     authenticated principal must carry a tenant (the authenticator normalizes
//     empty to tenant.DefaultTenant).
//  2. The operation's required scope (same as ScopeAuthorizer).
//  3. When the route layer resolved ResourceTenant (the target resource's
//     tenant), it must equal Principal.TenantID — defense in depth against
//     cross-tenant access. ResourceTenant is optional: management endpoints
//     that cannot resolve the resource tenant without a chicken-and-egg store
//     lookup leave it empty and rely on the tenant-scoped store read
//     (Inject-in-context → cross-tenant read resolves to not-found → 404) as
//     the authoritative IDOR defense (see module_management.go handleExecution).
type TenantAwareAuthorizer struct{}

// Authorize returns Allow iff the principal carries a non-empty tenant, holds
// the operation's required scope, and (when set) the resource tenant matches
// the principal's tenant. Otherwise Deny.
func (TenantAwareAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) (Decision, error) {
	if req.Principal.TenantID == "" {
		return DecisionDeny, nil
	}
	scope := scopeForOperation(req.Operation)
	if scope == "" {
		return DecisionDeny, nil
	}
	if !req.Principal.HasScope(scope) {
		return DecisionDeny, nil
	}
	if req.ResourceTenant != "" && req.ResourceTenant != req.Principal.TenantID {
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

// principalEntry is the server-configured identity bound to one bearer token.
// The token itself is never stored; only its sha256 is retained as the map key
// (see BearerPrincipalAuth). The tenant is server-issued and authoritative —
// callers cannot self-report tenant.
type principalEntry struct {
	subject  string
	tenantID string
	scopes   []string
}

// TokenPrincipalMapping binds one static bearer token to a server-issued
// (subject, tenant, scopes) triple. Used by NewBearerPrincipalAuthMulti to
// build a multi-tenant token registry (design §2.3 scheme A). The token must
// have at least 128 bits of entropy.
type TokenPrincipalMapping struct {
	Token    string
	Subject  string
	TenantID string
	Scopes   []string
}

// BearerPrincipalAuth maps static bearer tokens to verified principals. It
// supports multi-tenant operation: a registry of token-hash → principalEntry
// lets each token bind to its own (subject, tenant, scopes). The token
// comparison is sha256-keyed: the plaintext token is never retained in memory
// after construction, and token values are never logged (security policy §7
// password blacklist). The subject, tenant, and scopes are configured
// server-side, so a caller cannot self-report them — this is the IDOR defense
// (security policy §1a: identity must come from the server, never the client).
type BearerPrincipalAuth struct {
	// principalByHash maps token-hash → principalEntry. For the single-token
	// constructor the map has one entry. A map lookup by sha256 is not
	// constant-time across entries, but it reveals only the authentication
	// outcome (valid/invalid), not per-token timing that would aid enumeration
	// of which token matched.
	principalByHash map[[32]byte]principalEntry
}

// NewBearerPrincipalAuth creates a BearerPrincipalAuth that accepts the given
// single token and maps it to the given subject + scopes under the default
// tenant. This preserves G1 single-tenant backwards compatibility: when no
// multi-tenant token registry is configured, every authenticated caller is in
// tenant.DefaultTenant. The token must have at least 128 bits of entropy.
func NewBearerPrincipalAuth(token, subject string, scopes []string) *BearerPrincipalAuth {
	return NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{{
		Token: token, Subject: subject, TenantID: string(tenant.DefaultTenant), Scopes: scopes,
	}})
}

// NewBearerPrincipalAuthMulti creates a BearerPrincipalAuth from a multi-token
// registry. Each mapping binds one token to its own (subject, tenant, scopes).
// At least one mapping is required; a duplicate token is rejected. An empty
// TenantID in a mapping is normalized to tenant.DefaultTenant so the principal
// always carries a non-empty, key-safe tenant (required by TenantAwareAuthorizer
// and every downstream tenant-scoped store read).
func NewBearerPrincipalAuthMulti(mappings []TokenPrincipalMapping) *BearerPrincipalAuth {
	byHash := make(map[[32]byte]principalEntry, len(mappings))
	for _, m := range mappings {
		t := m.TenantID
		if t == "" {
			t = string(tenant.DefaultTenant)
		}
		entry := principalEntry{subject: m.Subject, tenantID: t, scopes: m.Scopes}
		h := sha256.Sum256([]byte(m.Token))
		// Reject duplicate tokens at construction rather than silently shadowing
		// one principal with another.
		if _, exists := byHash[h]; exists {
			panic("apiserver: duplicate auth token in BearerPrincipalAuth registry")
		}
		byHash[h] = entry
	}
	return &BearerPrincipalAuth{principalByHash: byHash}
}

// Authenticate returns the Principal bound to the request's bearer token, or
// ErrWorkflowUnauthenticated. The returned Principal carries the server-issued
// TenantID so downstream tenant-scoped store reads and audit are consistent.
// A wrong token yields the same error as a missing one — no existence leak.
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
	entry, ok := a.principalByHash[candidate]
	if !ok {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	return Principal{Subject: entry.subject, TenantID: entry.tenantID, Scopes: entry.scopes}, nil
}

// AuthenticateRequest implements WorkflowAuthenticator so the same multi-token
// registry can gate the outer management middleware while the route-level authz
// wrapper supplies the principal, scopes, and tenant. It delegates to
// Authenticate and returns only the authentication error, so plaintext tokens
// are never retained and the outcome is simply valid/invalid.
func (a *BearerPrincipalAuth) AuthenticateRequest(r *http.Request) error {
	_, err := a.Authenticate(r)
	return err
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
