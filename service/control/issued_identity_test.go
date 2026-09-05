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

// TestMultiAuthenticatorDryRunSuppressionSurvivesReversedOrder is the mirror
// of TestMultiAuthenticatorPreservesDryRunSuppression with the two members
// swapped: a plain error ahead of the dry-run member must not stop dispatch
// from remembering and returning the dry-run denial. Order must not matter —
// otherwise composing a dry-run file store *after* another authenticator
// (rather than before it, the only order the sibling test covers) would
// silently turn dry-run into enforcing.
func TestMultiAuthenticatorDryRunSuppressionSurvivesReversedOrder(t *testing.T) {
	dryRun := stubAuth{policy: permissivePolicy, err: dryRunDenial(ErrAuthUnknownToken)}
	m := NewMultiAuthenticator(stubAuth{err: ErrAuthMissingToken}, dryRun)

	got, err := m.AuthenticateRegister("r", "t", TransportInfo{})
	if !IsDryRunDenial(err) {
		t.Fatalf("err = %v, want a dry-run denial so the caller still allows the request", err)
	}
	if !got.Allows("anything") {
		t.Fatalf("dry-run must yield the permissive policy, got %+v", got)
	}
}

// TestMultiAuthenticatorTwoDryRunDenialsKeepsTheFirst pins dispatch's choice
// when more than one member is dry-run-denying: it remembers the FIRST
// dry-run denial and discards later ones. This choice is arbitrary — every
// dry-run denial is equally "let the request through and log", so keeping the
// last instead would not be wrong — this test exists only to force a human
// to notice and re-decide if the behavior ever changes, not to claim
// first-wins is the one correct answer.
func TestMultiAuthenticatorTwoDryRunDenialsKeepsTheFirst(t *testing.T) {
	first := stubAuth{policy: RunnerPolicy{Name: "first-dryrun"}, err: dryRunDenial(ErrAuthUnknownToken)}
	second := stubAuth{policy: RunnerPolicy{Name: "second-dryrun"}, err: dryRunDenial(ErrAuthMissingToken)}
	m := NewMultiAuthenticator(first, second)

	got, err := m.AuthenticateRegister("r", "t", TransportInfo{})
	if !IsDryRunDenial(err) {
		t.Fatalf("err = %v, want a dry-run denial", err)
	}
	if got.Name != "first-dryrun" {
		t.Fatalf("policy = %q, want the first dry-run member's (%q)", got.Name, "first-dryrun")
	}
}

// TestMultiAuthenticatorDispatchWithNoMembers covers the true
// zero-real-members path, in every shape that reaches it: no arguments at
// all, arguments that the constructor's nil filter reduces to an empty
// slice, and a nil *MultiAuthenticator itself. TestMultiAuthenticatorSkipsNilMembers
// always leaves one real member behind, so none of these shapes was
// previously exercised by any test.
//
// The nil-receiver case is not just belt-and-suspenders: for the first two
// cases, dispatch's fast-path guard (`m == nil || len(m.auths) == 0`) is
// externally unobservable on its own — deleting it still falls through the
// empty loop to `lastErr`'s zero-value initialization, which already equals
// ErrAuthUnknownToken, so the returned value is identical either way. Only a
// nil receiver makes the guard's removal observable, because without it
// `m.auths` dereferences a nil pointer instead of returning a value.
func TestMultiAuthenticatorDispatchWithNoMembers(t *testing.T) {
	cases := map[string]*MultiAuthenticator{
		"no arguments":              NewMultiAuthenticator(),
		"all-nil filtered to empty": NewMultiAuthenticator(nil, nil),
		"nil receiver":              (*MultiAuthenticator)(nil),
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := m.AuthenticateRegister("r", "t", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
				t.Fatalf("AuthenticateRegister err = %v, want ErrAuthUnknownToken", err)
			}
			if _, err := m.AuthenticateOngoing("r", "t", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
				t.Fatalf("AuthenticateOngoing err = %v, want ErrAuthUnknownToken", err)
			}
		})
	}
}

// TestMultiAuthenticatorMembersReturnsACopy proves Members() hands out a copy,
// not the live m.auths backing array: mutating every element of the returned
// slice must not change which authenticators dispatch actually consults on
// the next call. A test that only compared lengths would not catch Members()
// returning the live slice — overwriting elements in place doesn't change the
// length — so this asserts dispatch's observable behavior instead.
func TestMultiAuthenticatorMembersReturnsACopy(t *testing.T) {
	want := RunnerPolicy{Name: "real"}
	m := NewMultiAuthenticator(stubAuth{err: ErrAuthUnknownToken}, stubAuth{policy: want})

	members := m.Members()
	if len(members) != 2 {
		t.Fatalf("Members() length = %d, want 2", len(members))
	}
	for i := range members {
		members[i] = stubAuth{policy: RunnerPolicy{Name: "tampered"}}
	}

	got, err := m.AuthenticateRegister("r", "t", TransportInfo{})
	if err != nil {
		t.Fatalf("AuthenticateRegister: %v", err)
	}
	if got.Name != "real" {
		t.Fatalf("policy = %q, want %q — mutating the slice from Members() leaked into dispatch's own state", got.Name, "real")
	}
}

// TestMemoryIssuedIdentityStoreClonePreservesNilSlices pins the nil-vs-empty
// half of clone()'s contract that TestMemoryIssuedIdentityStoreReturnsDefensiveCopies
// leaves unexercised (that test only ever feeds non-empty slices in): a nil
// AllowedNodeTypes / AllowedNamespaces going into Issue must still be nil
// coming out of Lookup and List, not an empty non-nil slice. The reverse
// direction — a non-nil empty slice collapsing to nil — is the established,
// already-accepted RegistrationCode.clone() convention and is not asserted
// here.
func TestMemoryIssuedIdentityStoreClonePreservesNilSlices(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryIssuedIdentityStore()
	id := IssuedIdentity{
		RunnerID:  "runner-1",
		TokenHash: HashSecret("tok-secret"),
		Scope:     RunnerPolicy{Name: "n"}, // AllowedNodeTypes / AllowedNamespaces left nil
		CodeID:    "code-1",
		IssuedAt:  time.Unix(1700000000, 0).UTC(),
	}
	if id.Scope.AllowedNodeTypes != nil || id.Scope.AllowedNamespaces != nil {
		t.Fatal("test setup: expected nil slices before Issue")
	}

	if err := st.Issue(ctx, id); err != nil {
		t.Fatalf("Issue: %v", err)
	}

	got, ok, err := st.Lookup(ctx, "runner-1")
	if err != nil || !ok {
		t.Fatalf("Lookup: err=%v ok=%v", err, ok)
	}
	if got.Scope.AllowedNodeTypes != nil {
		t.Fatalf("Lookup: AllowedNodeTypes = %#v, want nil", got.Scope.AllowedNodeTypes)
	}
	if got.Scope.AllowedNamespaces != nil {
		t.Fatalf("Lookup: AllowedNamespaces = %#v, want nil", got.Scope.AllowedNamespaces)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List returned %d identities, want 1", len(list))
	}
	if list[0].Scope.AllowedNodeTypes != nil {
		t.Fatalf("List: AllowedNodeTypes = %#v, want nil", list[0].Scope.AllowedNodeTypes)
	}
	if list[0].Scope.AllowedNamespaces != nil {
		t.Fatalf("List: AllowedNamespaces = %#v, want nil", list[0].Scope.AllowedNamespaces)
	}
}
