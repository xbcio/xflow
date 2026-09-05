package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

func issueTestIdentity(t *testing.T, st IssuedIdentityStore, runnerID, token string, scope RunnerPolicy) {
	t.Helper()
	err := st.Issue(context.Background(), IssuedIdentity{
		RunnerID:  runnerID,
		TokenHash: HashSecret(token),
		Scope:     scope,
		CodeID:    "code-1",
		IssuedAt:  time.Unix(1700000000, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
}

func TestIssuedIdentityAuthenticatorAcceptsTheIssuedToken(t *testing.T) {
	st := NewMemoryIssuedIdentityStore()
	scope := RunnerPolicy{Name: "n", AllowedNodeTypes: []string{"kafka.trigger"}, AllowedNamespaces: []string{"sas"}}
	issueTestIdentity(t, st, "runner-1", "tok-secret", scope)

	a := NewIssuedIdentityAuthenticator(st)
	got, err := a.AuthenticateRegister("runner-1", "tok-secret", TransportInfo{})
	if err != nil {
		t.Fatalf("AuthenticateRegister: %v", err)
	}
	if !got.Allows("kafka.trigger") || got.Allows("http.request") {
		t.Fatalf("issued scope not returned intact: %+v", got)
	}
	if !got.AllowsNamespace(namespace.Namespace("sas")) || got.AllowsNamespace(namespace.Namespace("other")) {
		t.Fatalf("issued namespace scope not returned intact: %+v", got)
	}
	if _, err := a.AuthenticateOngoing("runner-1", "tok-secret", TransportInfo{}); err != nil {
		t.Fatalf("AuthenticateOngoing: %v", err)
	}
}

func TestIssuedIdentityAuthenticatorRejectsWrongCredentials(t *testing.T) {
	st := NewMemoryIssuedIdentityStore()
	issueTestIdentity(t, st, "runner-1", "tok-secret", RunnerPolicy{Name: "n"})
	a := NewIssuedIdentityAuthenticator(st)

	if _, err := a.AuthenticateRegister("runner-1", "", TransportInfo{}); !errors.Is(err, ErrAuthMissingToken) {
		t.Fatalf("empty token err = %v, want ErrAuthMissingToken", err)
	}
	if _, err := a.AuthenticateRegister("runner-1", "wrong", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("wrong token err = %v, want ErrAuthUnknownToken", err)
	}
	// The right token under someone else's runner id must not authenticate.
	if _, err := a.AuthenticateRegister("runner-2", "tok-secret", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("token replayed under another id: err = %v, want ErrAuthUnknownToken", err)
	}
}

// TestMemoryIssuedIdentityStoreReturnsDefensiveCopies proves the store never
// hands out an IssuedIdentity whose Scope's AllowedNodeTypes /
// AllowedNamespaces slices alias its internal backing arrays — in either
// direction. Without this, a caller mutating a stored/returned identity's
// scope would corrupt store state without ever holding s.mu, which is a data
// race under concurrent access. Mirrors TestMemoryStoreReturnsDefensiveCopies
// for RegistrationCode (registration_code_test.go).
func TestMemoryIssuedIdentityStoreReturnsDefensiveCopies(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryIssuedIdentityStore()
	id := IssuedIdentity{
		RunnerID:  "runner-1",
		TokenHash: HashSecret("tok-secret"),
		Scope:     RunnerPolicy{Name: "n", AllowedNodeTypes: []string{"kafka.trigger"}, AllowedNamespaces: []string{"sas"}},
		CodeID:    "code-1",
		IssuedAt:  time.Unix(1700000000, 0).UTC(),
	}

	if err := st.Issue(ctx, id); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	// Mutating the caller's own slice after Issue must not reach the store:
	// Issue must clone on the way in.
	id.Scope.AllowedNodeTypes[0] = "mutated-after-issue"
	id.Scope.AllowedNamespaces[0] = "mutated-after-issue"

	got, ok, err := st.Lookup(ctx, "runner-1")
	if err != nil || !ok {
		t.Fatalf("Lookup: err=%v ok=%v", err, ok)
	}
	if got.Scope.AllowedNodeTypes[0] != "kafka.trigger" {
		t.Fatalf("Issue aliased the caller's slice: Lookup returned %q, want %q", got.Scope.AllowedNodeTypes[0], "kafka.trigger")
	}
	if got.Scope.AllowedNamespaces[0] != "sas" {
		t.Fatalf("Issue aliased the caller's slice: Lookup returned %q, want %q", got.Scope.AllowedNamespaces[0], "sas")
	}

	// Mutating a slice returned by Lookup must not affect a later Lookup call:
	// Lookup must clone on the way out.
	got.Scope.AllowedNodeTypes[0] = "mutated-via-lookup"
	got.Scope.AllowedNamespaces[0] = "mutated-via-lookup"
	again, ok, err := st.Lookup(ctx, "runner-1")
	if err != nil || !ok {
		t.Fatalf("Lookup (again): err=%v ok=%v", err, ok)
	}
	if again.Scope.AllowedNodeTypes[0] != "kafka.trigger" {
		t.Fatalf("Lookup returned an aliased slice: second Lookup returned %q, want %q", again.Scope.AllowedNodeTypes[0], "kafka.trigger")
	}
	if again.Scope.AllowedNamespaces[0] != "sas" {
		t.Fatalf("Lookup returned an aliased slice: second Lookup returned %q, want %q", again.Scope.AllowedNamespaces[0], "sas")
	}

	// Mutating a slice returned by List must not affect a later List call:
	// List must clone on the way out too.
	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List returned %d identities, want 1", len(list))
	}
	list[0].Scope.AllowedNodeTypes[0] = "mutated-via-list"
	list[0].Scope.AllowedNamespaces[0] = "mutated-via-list"
	listAgain, err := st.List(ctx)
	if err != nil {
		t.Fatalf("List (again): %v", err)
	}
	if listAgain[0].Scope.AllowedNodeTypes[0] != "kafka.trigger" {
		t.Fatalf("List returned an aliased slice: second List returned %q, want %q", listAgain[0].Scope.AllowedNodeTypes[0], "kafka.trigger")
	}
	if listAgain[0].Scope.AllowedNamespaces[0] != "sas" {
		t.Fatalf("List returned an aliased slice: second List returned %q, want %q", listAgain[0].Scope.AllowedNamespaces[0], "sas")
	}
}

// stubAuth is a minimal Authenticator for composing MultiAuthenticator.
type stubAuth struct {
	policy RunnerPolicy
	err    error
}

func (s stubAuth) AuthenticateRegister(string, string, TransportInfo) (RunnerPolicy, error) {
	return s.policy, s.err
}
func (s stubAuth) AuthenticateOngoing(string, string, TransportInfo) (RunnerPolicy, error) {
	return s.policy, s.err
}

func TestMultiAuthenticatorReturnsFirstSuccess(t *testing.T) {
	want := RunnerPolicy{Name: "second"}
	m := NewMultiAuthenticator(
		stubAuth{err: ErrAuthUnknownToken},
		stubAuth{policy: want},
		stubAuth{policy: RunnerPolicy{Name: "third"}},
	)
	got, err := m.AuthenticateRegister("r", "t", TransportInfo{})
	if err != nil {
		t.Fatalf("AuthenticateRegister: %v", err)
	}
	if got.Name != "second" {
		t.Fatalf("policy = %q, want the first successful authenticator's", got.Name)
	}
}

func TestMultiAuthenticatorSkipsNilMembers(t *testing.T) {
	m := NewMultiAuthenticator(nil, stubAuth{policy: RunnerPolicy{Name: "ok"}}, nil)
	got, err := m.AuthenticateOngoing("r", "t", TransportInfo{})
	if err != nil {
		t.Fatalf("AuthenticateOngoing: %v", err)
	}
	if got.Name != "ok" {
		t.Fatalf("policy = %q, want ok", got.Name)
	}
}

// A dry-run FilePolicyStore returns (permissivePolicy, dryRunError) so the
// caller lets the request through and merely logs (auth.go:373-380). If
// MultiAuthenticator returned the *last* error unconditionally, composing an
// issued-identity store after a dry-run file store would silently turn dry-run
// into enforcing — a rollout-breaking regression with no test to catch it.
func TestMultiAuthenticatorPreservesDryRunSuppression(t *testing.T) {
	dryRun := stubAuth{policy: permissivePolicy, err: dryRunDenial(ErrAuthUnknownToken)}
	m := NewMultiAuthenticator(dryRun, stubAuth{err: ErrAuthUnknownToken})

	got, err := m.AuthenticateRegister("r", "t", TransportInfo{})
	if !IsDryRunDenial(err) {
		t.Fatalf("err = %v, want a dry-run denial so the caller still allows the request", err)
	}
	if !got.Allows("anything") {
		t.Fatalf("dry-run must yield the permissive policy, got %+v", got)
	}
}

func TestMultiAuthenticatorReturnsAnErrorWhenAllFail(t *testing.T) {
	m := NewMultiAuthenticator(stubAuth{err: ErrAuthMissingToken}, stubAuth{err: ErrAuthUnknownToken})
	if _, err := m.AuthenticateRegister("r", "t", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("err = %v, want the last authenticator's error", err)
	}
}

func TestMultiAuthenticatorIsConfigured(t *testing.T) {
	// IsConfigured must see through the composite: a MultiAuthenticator wrapping
	// only DisabledAuthenticator is still "auth disabled", and a production
	// server that trusted the wrapper would start unauthenticated.
	if IsConfigured(NewMultiAuthenticator(DisabledAuthenticator{})) {
		t.Fatal("a composite of only DisabledAuthenticator must not count as configured")
	}
	if !IsConfigured(NewMultiAuthenticator(DisabledAuthenticator{}, stubAuth{})) {
		t.Fatal("a composite containing a real authenticator must count as configured")
	}
}
