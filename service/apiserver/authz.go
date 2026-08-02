package apiserver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// Principal is the authenticated identity of a caller. It is produced by an
// Authenticator (which verifies the credential) and consumed by an Authorizer
// (which decides allow/deny per operation+resource). The subject, namespace, and
// scopes come from the server-side identity source — never from the request
// body — so a caller cannot self-report another principal.
type Principal struct {
	Subject   string
	Namespace string
	Scopes    []string
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
// Namespace boundary (Task 7.3): Principal.Namespace is the server-issued namespace
// and is the authoritative namespace for the decision. ResourceNamespace is the
// namespace of the target resource, resolved by the route layer when it can be
// looked up without a chicken-and-egg dependency on the authz decision; when
// empty (e.g. create operations, or management endpoints where the resource
// namespace is enforced downstream by a namespace-scoped store read) the
// per-resource namespace check is skipped and IDOR is enforced by the
// namespace-scoped store read (see module_management.go: a cross-namespace Inspect
// resolves to not-found → 404, never leaking existence).
type AuthorizationRequest struct {
	Principal         Principal
	Operation         string
	WorkflowID        string
	ExecutionID       string
	Resource          string
	ResourceNamespace string
}

// Decision is the outcome of an authorization check.
type Decision string

const (
	DecisionAllow Decision = "allow"
	DecisionDeny  Decision = "deny"
)

// Authorizer decides whether a principal may perform an operation on a
// resource. It must default to Deny. Implementations may be a static scope
// map (G1 single-namespace reference) or a real policy engine (G2/G3).
type Authorizer interface {
	Authorize(ctx context.Context, req AuthorizationRequest) (Decision, error)
}

// Operation vocabulary. The route layer resolves each inbound request to one
// of these before invoking the handler.
const (
	OpWorkflowCreate = "workflow.create"
	OpWorkflowInvoke = "workflow.invoke"
	OpWorkflowRead   = "workflow.read"
	// OpWorkflowRegister is the mutation that persists a compiled workflow graph
	// into the server-side registry via POST /v1/workflows/register, and its
	// deregister counterpart via DELETE /v1/workflows/register/{id}. It maps to
	// the "workflow" scope like the other workflow operations.
	OpWorkflowRegister           = "workflow.register"
	OpWorkflowDefinitionCreate   = "workflowdefinition.create"
	OpWorkflowDefinitionRead     = "workflowdefinition.read"
	OpWorkflowDefinitionUpdate   = "workflowdefinition.update" // draft
	OpWorkflowDefinitionValidate = "workflowdefinition.validate"
	OpWorkflowDefinitionPublish  = "workflowdefinition.publish"
	OpWorkflowExecutionInvoke    = "workflowexecution.invoke"
	OpExecutionRead              = "execution.read"
	OpExecutionSignal            = "execution.signal"
	OpExecutionRevoke            = "execution.revoke"
	OpExecutionCancel            = "execution.cancel"
	// OpExecutionSeed is the mutation that seeds an execution from an entry unit
	// (single node or group node) result via POST /v1/executions. It maps to the
	// "execution" scope like the other execution operations.
	OpExecutionSeed    = "execution.seed"
	OpDeadLetterList   = "deadletter.list"
	OpDeadLetterReplay = "deadletter.replay"
	// OpManagementRead is the stable operation for management execution inspect
	// (single-resource lookup; the management surface exposes no list API). It
	// maps to the "management.read" scope.
	OpManagementRead = "management.read"
	// OpManagementLeaderRead and OpManagementRunnerRead give the leader-status
	// and single-runner-lookup routes their own stable operations + scopes
	// (Task 8 blocker 2: management leader/runner get independent scopes rather
	// than riding on a blanket management.read). Each maps to its own scope so a
	// token can be granted runner read without leader read and vice-versa.
	OpManagementLeaderRead = "management.leader.read"
	OpManagementRunnerRead = "management.runner.read"
	OpManagementWrite = "management.write"
	// OpSupplyWrite / OpSupplyRead gate the supply content endpoints. Writing a
	// supply changes production data-processing logic, so its authorization
	// strength matches writing a workflow definition: its own scope, denied by
	// default. Both MUST also appear in scopeForOperation — an operation that
	// falls through to the default "" scope is denied by both ScopeAuthorizer
	// and NamespaceAwareAuthorizer, making the route silently unreachable.
	OpSupplyWrite = "supply.write"
	OpSupplyRead  = "supply.read"
	// OpArtifactRead gates GET/HEAD /v1/artifacts/{digest}. It gets its own
	// scope rather than riding on supply.read because the two are granted to
	// different populations: every runner needs artifact read to fetch script
	// bytes, while supply read reaches tenant cleansing rules. There is no
	// artifact.write counterpart — uploads happen at publish time through the
	// store, never over HTTP (design §6.3). Like the supply operations, this
	// MUST also appear in scopeForOperation or the route is silently
	// unreachable.
	OpArtifactRead = "artifact.read"
)

// scopeForOperation maps an operation to the scope it requires. A principal
// must hold the scope to be allowed. This is the G1 single-namespace reference
// policy; G2/G3 may substitute a richer authorizer.
func scopeForOperation(op string) string {
	switch op {
	case OpWorkflowCreate, OpWorkflowInvoke, OpWorkflowRead,
		OpWorkflowRegister,
		OpWorkflowDefinitionCreate, OpWorkflowDefinitionRead, OpWorkflowDefinitionUpdate,
		OpWorkflowDefinitionValidate, OpWorkflowDefinitionPublish, OpWorkflowExecutionInvoke:
		return "workflow"
	case OpExecutionRead, OpExecutionSignal, OpExecutionRevoke, OpExecutionCancel, OpExecutionSeed:
		return "execution"
	case OpDeadLetterList:
		return "deadletter.list"
	case OpDeadLetterReplay:
		return "deadletter.replay"
	case OpManagementRead:
		return "management.read"
	case OpManagementLeaderRead:
		return "management.leader.read"
	case OpManagementRunnerRead:
		return "management.runner.read"
	case OpManagementWrite:
		return "management.write"
	case OpSupplyWrite:
		return "supply.write"
	case OpSupplyRead:
		return "supply.read"
	case OpArtifactRead:
		return "artifact.read"
	default:
		return ""
	}
}

// ScopeAuthorizer allows a principal whose scopes include the operation's
// required scope. It defaults to Deny and denies cross-execution access only
// at the resource level when a future namespace-bound authorizer is wired (G1
// single-namespace does not enforce per-execution ownership, which is the host's
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

// NamespaceAwareAuthorizer is the G2 multi-namespace authorizer. It extends
// ScopeAuthorizer with namespace-bound enforcement:
//
//  1. Default-deny when Principal.Namespace is empty — a principal without a
//     server-issued namespace is never allowed. This is fail-closed: every
//     authenticated principal must carry a namespace (the authenticator normalizes
//     empty to namespace.Default).
//  2. The operation's required scope (same as ScopeAuthorizer).
//  3. When the route layer resolved ResourceNamespace (the target resource's
//     namespace), it must equal Principal.Namespace — defense in depth against
//     cross-namespace access. ResourceNamespace is optional: management endpoints
//     that cannot resolve the resource namespace without a chicken-and-egg store
//     lookup leave it empty and rely on the namespace-scoped store read
//     (Inject-in-context → cross-namespace read resolves to not-found → 404) as
//     the authoritative IDOR defense (see module_management.go handleExecution).
type NamespaceAwareAuthorizer struct{}

// Authorize returns Allow iff the principal carries a non-empty namespace, holds
// the operation's required scope, and (when set) the resource namespace matches
// the principal's namespace. Otherwise Deny.
func (NamespaceAwareAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) (Decision, error) {
	if req.Principal.Namespace == "" {
		return DecisionDeny, nil
	}
	scope := scopeForOperation(req.Operation)
	if scope == "" {
		return DecisionDeny, nil
	}
	if !req.Principal.HasScope(scope) {
		return DecisionDeny, nil
	}
	if req.ResourceNamespace != "" && req.ResourceNamespace != req.Principal.Namespace {
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
// (see BearerPrincipalAuth). The namespace is server-issued and authoritative —
// callers cannot self-report namespace.
type principalEntry struct {
	subject     string
	namespaceID string
	scopes      []string
}

// TokenPrincipalMapping binds one static bearer token to a server-issued
// (subject, namespace, scopes) triple. Used by NewBearerPrincipalAuthMulti to
// build a multi-namespace token registry (design §2.3 scheme A). The token must
// have at least 128 bits of entropy.
type TokenPrincipalMapping struct {
	Token     string
	Subject   string
	Namespace string
	Scopes    []string
}

// BearerPrincipalAuth maps static bearer tokens to verified principals. It
// supports multi-namespace operation: a registry of token-hash → principalEntry
// lets each token bind to its own (subject, namespace, scopes). The token
// comparison is sha256-keyed: the plaintext token is never retained in memory
// after construction, and token values are never logged (security policy §7
// password blacklist). The subject, namespace, and scopes are configured
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
// namespace. This preserves G1 single-namespace backwards compatibility: when no
// multi-namespace token registry is configured, every authenticated caller is in
// namespace.Default. The token must have at least 128 bits of entropy.
func NewBearerPrincipalAuth(token, subject string, scopes []string) *BearerPrincipalAuth {
	return NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{{
		Token: token, Subject: subject, Namespace: string(namespace.Default), Scopes: scopes,
	}})
}

// NewBearerPrincipalAuthMulti creates a BearerPrincipalAuth from a multi-token
// registry. Each mapping binds one token to its own (subject, namespace, scopes).
// At least one mapping is required; a duplicate token is rejected. An empty
// Namespace in a mapping is normalized to namespace.Default so the principal
// always carries a non-empty, key-safe namespace (required by NamespaceAwareAuthorizer
// and every downstream namespace-scoped store read).
func NewBearerPrincipalAuthMulti(mappings []TokenPrincipalMapping) *BearerPrincipalAuth {
	byHash := make(map[[32]byte]principalEntry, len(mappings))
	for _, m := range mappings {
		t := m.Namespace
		if t == "" {
			t = string(namespace.Default)
		}
		entry := principalEntry{subject: m.Subject, namespaceID: t, scopes: m.Scopes}
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
// Namespace so downstream namespace-scoped store reads and audit are consistent.
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
	return Principal{Subject: entry.subject, Namespace: entry.namespaceID, Scopes: entry.scopes}, nil
}

// AuthenticateRequest implements WorkflowAuthenticator so the same multi-token
// registry can gate the outer management middleware while the route-level authz
// wrapper supplies the principal, scopes, and namespace. It delegates to
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
	Namespace   string
	Operation   string
	Resource    string
	WorkflowID  string
	ExecutionID string
	Decision    Decision
	Reason      string // deny reason code, not free text
	Outcome     string // admitted/denied/reconciled
	Phase       string // immutable audit phase: admission/outcome (T9)
	TraceID     string
	Timestamp   time.Time
	// Receipt correlation (T4 dead-letter receipt projector; T9 outcome
	// phase). Populated only by the receipt projection path; admission and
	// reconcile outcome rows leave them empty. ReceiptAuditID is the Redis
	// receipt audit_id and the projector's idempotency key.
	NodeID         string
	ActivationID   string
	EntryID        string
	ReceiptAuditID string
	// Revision is the resource version a mutation produced. Zero means "not
	// applicable" (reads, denials) or "not yet known" (admission row written
	// before the handler). Only the outcome row carries the resulting revision.
	// The resource CONTENT is never recorded here — only the version number.
	Revision uint64
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
	mu     sync.Mutex
	events []AuditEvent
}

// NewInMemoryAuditSink returns an empty in-memory audit sink.
func NewInMemoryAuditSink() *InMemoryAuditSink { return &InMemoryAuditSink{} }

func (s *InMemoryAuditSink) Append(_ context.Context, ev AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

// Events returns a copy of the recorded audit events.
func (s *InMemoryAuditSink) Events() []AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
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
