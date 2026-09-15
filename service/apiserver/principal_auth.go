package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store/objectstore"
)

// ErrPrincipalNotApplicable reports that an authenticator did not find its
// credential on this request. It is for authenticator composition only: HTTP
// handlers must collapse it to ErrWorkflowUnauthenticated rather than expose
// which credential form a server supports.
var ErrPrincipalNotApplicable = errors.New("apiserver: principal authenticator not applicable")

type trustedPrincipalContextKey struct{}

type trustedPrincipalContextValue struct {
	principal Principal
}

// ContextWithPrincipal attaches a host-verified principal to ctx.
//
// This is an in-process trust boundary: ContextPrincipalAuthenticator reads only
// this private context value and never examines request headers. A host must call
// it only after it has authenticated its caller. The principal and its scope
// slice are copied so later caller mutation cannot change what a handler sees.
func ContextWithPrincipal(ctx context.Context, principal Principal) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, trustedPrincipalContextKey{}, trustedPrincipalContextValue{
		principal: copyPrincipal(principal),
	})
}

// ContextPrincipalAuthenticator returns an authenticator for a principal the
// embedding server has already verified and placed in request context with
// ContextWithPrincipal. It never trusts an HTTP header.
func ContextPrincipalAuthenticator() PrincipalAuthenticator {
	return contextPrincipalAuthenticator{}
}

type contextPrincipalAuthenticator struct{}

var _ PrincipalAuthenticator = contextPrincipalAuthenticator{}

func (contextPrincipalAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	if r == nil {
		return Principal{}, ErrPrincipalNotApplicable
	}
	value, ok := r.Context().Value(trustedPrincipalContextKey{}).(trustedPrincipalContextValue)
	if !ok {
		return Principal{}, ErrPrincipalNotApplicable
	}
	principal, ok := normalizedPrincipal(value.principal)
	if !ok {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	return principal, nil
}

// MultiPrincipalAuthenticator tries applicable authenticators in order. Only
// ErrPrincipalNotApplicable permits trying the next one; an invalid credential
// is terminal. In particular, an invalid X-Xflow-Runner-Id credential cannot
// be paired with a valid static bearer token to downgrade into a different
// principal.
type MultiPrincipalAuthenticator struct {
	authenticators []PrincipalAuthenticator
}

var _ PrincipalAuthenticator = (*MultiPrincipalAuthenticator)(nil)

// NewMultiPrincipalAuthenticator composes principal authenticators in order.
// Nil authenticators are ignored.
func NewMultiPrincipalAuthenticator(authenticators ...PrincipalAuthenticator) *MultiPrincipalAuthenticator {
	kept := make([]PrincipalAuthenticator, 0, len(authenticators))
	for _, authenticator := range authenticators {
		if authenticator != nil {
			kept = append(kept, authenticator)
		}
	}
	return &MultiPrincipalAuthenticator{authenticators: kept}
}

func (a *MultiPrincipalAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	if a == nil {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	for _, authenticator := range a.authenticators {
		principal, err := authenticator.Authenticate(r)
		if err == nil {
			principal, ok := normalizedPrincipal(principal)
			if !ok {
				return Principal{}, ErrWorkflowUnauthenticated
			}
			return principal, nil
		}
		if errors.Is(err, ErrPrincipalNotApplicable) {
			continue
		}
		return Principal{}, ErrWorkflowUnauthenticated
	}
	return Principal{}, ErrWorkflowUnauthenticated
}

// IssuedIdentityPrincipalAuthenticator authenticates enrollment-issued runner
// identities for the runner's HTTP resource requests. It deliberately accepts
// only a runner ID header plus a bearer token; its namespace is derived from
// the issued RunnerPolicy rather than trusted from a request header.
type IssuedIdentityPrincipalAuthenticator struct {
	authenticator *control.IssuedIdentityAuthenticator
	store         control.IssuedIdentityStore
}

var _ PrincipalAuthenticator = (*IssuedIdentityPrincipalAuthenticator)(nil)

// NewIssuedIdentityPrincipalAuthenticator creates an authenticator backed by
// enrollment-issued identities. It uses control's established constant-time
// token, revocation, and expiry checks and never returns their internal error
// detail to an HTTP caller.
func NewIssuedIdentityPrincipalAuthenticator(store control.IssuedIdentityStore) *IssuedIdentityPrincipalAuthenticator {
	return &IssuedIdentityPrincipalAuthenticator{
		authenticator: control.NewIssuedIdentityAuthenticator(store),
		store:         store,
	}
}

func (a *IssuedIdentityPrincipalAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	runnerID, applicable, ok := issuedRunnerID(r)
	if !applicable {
		return Principal{}, ErrPrincipalNotApplicable
	}
	if !ok || a == nil || a.authenticator == nil || a.store == nil {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	// Every SDK runner now declares its RunnerID on resource requests, including
	// runners authenticated by a regular static principal. An ID not present in
	// the issued store is therefore not an issued-identity credential at all and
	// may proceed to the regular authenticator. Once an issued ID exists, though,
	// a missing/wrong token, revocation, expiry, or policy failure is terminal;
	// it must never downgrade into a static bearer principal.
	_, exists, lookupErr := a.store.Lookup(r.Context(), runnerID)
	if lookupErr != nil {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	if !exists {
		return Principal{}, ErrPrincipalNotApplicable
	}
	token, ok := strictBearerToken(r)
	if !ok {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	policy, err := a.authenticator.AuthenticateOngoing(runnerID, token, httpTransportInfoFromRequest(r))
	if err != nil {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	if policy.IDPrefix != "" && !strings.HasPrefix(runnerID, policy.IDPrefix) {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	ns, ok := issuedIdentityNamespace(r, policy)
	if !ok {
		return Principal{}, ErrWorkflowUnauthenticated
	}
	return Principal{
		Subject:   runnerID,
		Namespace: string(ns),
		Scopes:    []string{ScopeRunnerResource},
	}, nil
}

func issuedRunnerID(r *http.Request) (runnerID string, applicable, ok bool) {
	if r == nil {
		return "", false, false
	}
	values := r.Header.Values(protocol.RunnerIDHeader)
	if len(values) == 0 {
		return "", false, false
	}
	if len(values) != 1 {
		return "", true, false
	}
	runnerID = strings.TrimSpace(values[0])
	if runnerID == "" || runnerID != values[0] {
		return "", true, false
	}
	return runnerID, true, true
}

func strictBearerToken(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || parts[0] != "Bearer" || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

// issuedIdentityNamespace resolves the one namespace represented by this HTTP
// request. A runner may explicitly declare a policy-entitled namespace. Without
// a declaration, only an unambiguous issued scope is accepted; broad policies
// fail closed rather than arbitrarily selecting a tenant.
func issuedIdentityNamespace(r *http.Request, policy control.RunnerPolicy) (namespace.Namespace, bool) {
	if r != nil {
		values := r.Header.Values(objectstore.NamespaceHeader)
		if len(values) > 0 {
			if len(values) != 1 {
				return "", false
			}
			ns := namespace.Namespace(strings.TrimSpace(values[0]))
			if ns == "" || ns != namespace.Namespace(values[0]) || namespace.Validate(ns) != nil || !policy.AllowsNamespace(ns) {
				return "", false
			}
			return ns, true
		}
	}

	if len(policy.AllowedNamespaces) == 0 {
		return namespace.Default, true
	}
	var resolved namespace.Namespace
	for _, raw := range policy.AllowedNamespaces {
		if raw == "*" {
			return "", false
		}
		ns := namespace.Namespace(raw)
		if namespace.Validate(ns) != nil {
			return "", false
		}
		if resolved == "" {
			resolved = ns
			continue
		}
		if resolved != ns {
			return "", false
		}
	}
	return resolved, resolved != ""
}

func copyPrincipal(principal Principal) Principal {
	principal.Scopes = append([]string(nil), principal.Scopes...)
	return principal
}

func normalizedPrincipal(principal Principal) (Principal, bool) {
	principal = copyPrincipal(principal)
	principal.Subject = strings.TrimSpace(principal.Subject)
	if principal.Subject == "" {
		return Principal{}, false
	}
	principal.Namespace = strings.TrimSpace(principal.Namespace)
	if principal.Namespace == "" {
		principal.Namespace = string(namespace.Default)
	}
	if namespace.Validate(namespace.Namespace(principal.Namespace)) != nil {
		return Principal{}, false
	}
	for i := range principal.Scopes {
		principal.Scopes[i] = strings.TrimSpace(principal.Scopes[i])
	}
	return principal, true
}
