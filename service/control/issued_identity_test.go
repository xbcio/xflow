package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/storecontract"
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

// TestMemoryIssuedIdentityStoreSatisfiesContract holds the in-memory
// implementation to the same cross-implementation contract store/sqlstore's
// SQL implementation is held to (store/sqlstore/registration_code_repo_test.go).
func TestMemoryIssuedIdentityStoreSatisfiesContract(t *testing.T) {
	storecontract.RunIssuedIdentityStoreContract(t, func(t *testing.T) store.IssuedIdentityStore {
		return NewMemoryIssuedIdentityStore()
	})
}

// failingIssuedIdentityStore makes Lookup fail the way a database outage does.
type failingIssuedIdentityStore struct{ err error }

func (s failingIssuedIdentityStore) Issue(context.Context, IssuedIdentity) error { return s.err }
func (s failingIssuedIdentityStore) Lookup(context.Context, string) (IssuedIdentity, bool, error) {
	return IssuedIdentity{}, false, s.err
}
func (s failingIssuedIdentityStore) List(context.Context) ([]IssuedIdentity, error) {
	return nil, s.err
}
func (s failingIssuedIdentityStore) Revoke(context.Context, string, OwnerScope) error { return s.err }
func (s failingIssuedIdentityStore) Renew(context.Context, string, time.Time) error {
	return s.err
}

// TestAuthenticateRejectsExpiredAndRevokedIdenticallyToCallers pins R7 at the
// place R7 actually lives.
//
// This test used to be named ...ByteIdentically and asserted that the expired,
// revoked and unknown-token rejections produced byte-identical err.Error()
// strings. That assertion was stricter than the invariant it named, and it was
// stricter in a direction that cost the operator: authenticate's error text
// never reaches a caller — Core.authDeny logs it and returns the
// ErrUnauthenticated constant, which is the only thing either transport ever
// renders (server.go writeRunnerError, grpc_server.go runnerStatus) — so
// equal strings bought no external indistinguishability, while distinct
// strings buy an operator the difference between "your fleet's TTL lapsed" and
// "someone is knocking with a bad token". The same file's
// TestIssuedIdentityLookupFailureIsExternallyIdenticalButInternallyDistinct
// had already established that exact shape for the store-failure path.
//
// So the R7 boundary is: identical error VALUE (errors.Is) and identical
// TRANSPORT-VISIBLE result (authDeny's ErrUnauthenticated); distinct
// server-side log text. This test asserts all three, and the third in the
// negative direction — the three log strings must differ pairwise, so a
// regression that reverts 1a's wraps fails here rather than passing silently.
func TestAuthenticateRejectsExpiredAndRevokedIdenticallyToCallers(t *testing.T) {
	base := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	newAuth := func(t *testing.T, id IssuedIdentity) *IssuedIdentityAuthenticator {
		t.Helper()
		st := NewMemoryIssuedIdentityStore()
		if err := st.Issue(context.Background(), id); err != nil {
			t.Fatalf("issue: %v", err)
		}
		a := NewIssuedIdentityAuthenticator(st)
		a.now = func() time.Time { return base }
		return a
	}

	// The secret is spelled out in full rather than as "tok" so the
	// no-caller-values assertion below is not trivially satisfied — "tok" is a
	// substring of "unknown auth token" and would make that check unfailable.
	const secret = "s3cret-runner-token"

	live := IssuedIdentity{
		RunnerID: "r", TokenHash: HashSecret(secret), CodeID: "c",
		IssuedAt: base.Add(-time.Hour), ExpiresAt: base.Add(time.Hour),
	}
	expired := live
	expired.ExpiresAt = base.Add(-time.Minute)
	revoked := live
	revoked.RevokedAt = base.Add(-time.Minute)
	noExpiry := live
	noExpiry.ExpiresAt = time.Time{}

	// The live and never-expiring identities authenticate.
	for name, id := range map[string]IssuedIdentity{"live": live, "no expiry": noExpiry} {
		t.Run(name, func(t *testing.T) {
			if _, err := newAuth(t, id).authenticate("r", secret); err != nil {
				t.Fatalf("authenticate = %v, want nil", err)
			}
		})
	}

	unknown := NewIssuedIdentityAuthenticator(NewMemoryIssuedIdentityStore())
	unknown.now = func() time.Time { return base }
	_, unknownErr := unknown.authenticate("nobody", secret)

	_, expiredErr := newAuth(t, expired).authenticate("r", secret)
	_, revokedErr := newAuth(t, revoked).authenticate("r", secret)

	rejections := []struct {
		name string
		err  error
	}{
		{"unknown", unknownErr},
		{"expired", expiredErr},
		{"revoked", revokedErr},
	}

	// (1) Same error VALUE. ErrAuthUnknownToken stays the single matchable
	// identity on all three paths; a second sentinel would let a caller (or a
	// future transport mapping) tell them apart programmatically.
	for _, tc := range rejections {
		if !errors.Is(tc.err, ErrAuthUnknownToken) {
			t.Fatalf("%s rejection err = %v, want errors.Is ErrAuthUnknownToken", tc.name, tc.err)
		}
	}

	// (2) Same CALLER-VISIBLE result. This is R7's real landing point and the
	// original test never covered it: it stopped at authenticate and never
	// drove authDeny, the one hop that decides what a caller receives. Feed all
	// three through authDeny and require the identical ErrUnauthenticated
	// value. An implementation that "helpfully" propagated the wrapped auth
	// error out of authDeny would still satisfy assertion (1) — it wraps
	// ErrAuthUnknownToken, not ErrUnauthenticated — and would sail past the
	// old byte-comparison too, because that comparison never looked here.
	core := &Core{}
	for _, tc := range rejections {
		got := core.authDeny(context.Background(), "r", secret, "heartbeat", TransportInfo{}, tc.err)
		if got != ErrUnauthenticated {
			t.Fatalf("authDeny(%s) = %v (%T), want the ErrUnauthenticated constant itself; "+
				"anything else leaks the rejection reason to the caller through "+
				"writeRunnerError/runnerStatus, which render err.Error() on the "+
				"ErrUnauthenticated branch", tc.name, got, got)
		}
		if !errors.Is(got, ErrUnauthenticated) {
			t.Fatalf("authDeny(%s) result does not match ErrUnauthenticated: %v", tc.name, got)
		}
	}

	// (3) Distinct SERVER-SIDE text. The inverse of the old assertion, and the
	// guard on 1a: authDeny logs the error verbatim, so collapsing these three
	// strings makes a fleet-wide TTL lapse read exactly like a bad-token flood
	// and sends the operator to credential distribution instead of
	// --runner-identity-ttl. A regression that reverts either wrap to a bare
	// `return ErrAuthUnknownToken` makes the reverted path equal "unknown" here
	// and fails this loop.
	for i := 0; i < len(rejections); i++ {
		for j := i + 1; j < len(rejections); j++ {
			a, b := rejections[i], rejections[j]
			if a.err.Error() == b.err.Error() {
				t.Fatalf("%s and %s both log as %q; an operator reading auth_denied "+
					"cannot tell a lapsed identity TTL from a bad token, which is the "+
					"whole reason these are wrapped", a.name, b.name, a.err.Error())
			}
		}
	}

	// The reasons must stay free of caller-supplied values and of the expiry
	// timestamp. authDeny logs the runner id in its own field, and a timestamp
	// would be indirect evidence that this runner id was once real — plus a
	// disclosure surface the day one of these strings is mistakenly wired onto
	// an outward path. The runner id "r" is not probed as a substring here
	// because it is one character and matches almost any English word; the
	// token and the timestamps are the values that would actually hurt.
	for _, tc := range rejections {
		for _, forbidden := range []string{
			secret,
			base.String(),
			base.Format(time.RFC3339),
			expired.ExpiresAt.Format(time.RFC3339),
			revoked.RevokedAt.Format(time.RFC3339),
		} {
			if strings.Contains(tc.err.Error(), forbidden) {
				t.Fatalf("%s rejection message %q contains %q, which must not appear in it",
					tc.name, tc.err.Error(), forbidden)
			}
		}
	}

	// Expiry is strictly in the past: exactly-at-expiry is expired.
	atExpiry := live
	atExpiry.ExpiresAt = base
	if _, err := newAuth(t, atExpiry).authenticate("r", secret); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("identity expiring exactly now authenticated; want rejected")
	}
}

// TestIssuedIdentityLookupFailureIsExternallyIdenticalButInternallyDistinct
// pins both halves of the collapsed-error contract at once, because either
// half alone is satisfied by a wrong implementation:
//
//   - errors.Is must still match ErrAuthUnknownToken, or a store outage would
//     become externally distinguishable from an absent runner id — the
//     enumeration oracle spec §2.3.4 condition 4 forbids.
//   - the error text must NOT be identical to the absent-identity error, or
//     Core.authDeny (which logs this error verbatim) would report a database
//     outage as "unknown auth token" and send an operator after credentials.
//
// The assertion is a comparison between the two paths, not a substring match
// on either one: a substring assertion would pass against an implementation
// that also leaked the store detail outward.
func TestIssuedIdentityLookupFailureIsExternallyIdenticalButInternallyDistinct(t *testing.T) {
	storeErr := errors.New("dial tcp: connection refused")
	failing := NewIssuedIdentityAuthenticator(failingIssuedIdentityStore{err: storeErr})

	absent := NewIssuedIdentityAuthenticator(NewMemoryIssuedIdentityStore())
	_, absentErr := absent.AuthenticateRegister("runner-1", "tok", TransportInfo{})
	if !errors.Is(absentErr, ErrAuthUnknownToken) {
		t.Fatalf("absent identity err = %v, want ErrAuthUnknownToken", absentErr)
	}

	for _, tc := range []struct {
		name string
		call func(*IssuedIdentityAuthenticator) (RunnerPolicy, error)
	}{
		{"register", func(a *IssuedIdentityAuthenticator) (RunnerPolicy, error) {
			return a.AuthenticateRegister("runner-1", "tok", TransportInfo{})
		}},
		{"ongoing", func(a *IssuedIdentityAuthenticator) (RunnerPolicy, error) {
			return a.AuthenticateOngoing("runner-1", "tok", TransportInfo{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := tc.call(failing)
			if !errors.Is(err, ErrAuthUnknownToken) {
				t.Fatalf("store failure err = %v, want it to match ErrAuthUnknownToken", err)
			}
			if err.Error() == absentErr.Error() {
				t.Fatalf("store failure and absent identity log identically as %q; "+
					"an outage would be reported to operators as a bad token", err)
			}
			if !errors.Is(err, storeErr) {
				// Deliberate: %v, not %w. Nothing consumes the store error
				// programmatically, and a matchable tree would let callers
				// couple to store internals through an auth error.
				t.Logf("store error intentionally not in the errors.Is tree: %v", err)
			} else {
				t.Fatalf("store error was wrapped with %%w; use %%v so the error tree stays narrow")
			}
			if policy.Name != "" || len(policy.AllowedNodeTypes) != 0 || len(policy.AllowedNamespaces) != 0 {
				t.Fatalf("failed lookup returned a non-zero policy: %+v", policy)
			}
		})
	}
}

// capturedLog is one logger call: the event name plus its key/value args
// flattened the way Core.authDeny passes them.
type capturedLog struct {
	msg  string
	args []any
}

func (e capturedLog) field(key string) (any, bool) {
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok && k == key {
			return e.args[i+1], true
		}
	}
	return nil, false
}

// logCapturingLogger records Error calls. It satisfies engine.Logger, the
// interface Core.logger holds (mirrors controlplane_test.go's
// warnCapturingLogger, which captures the other half of the surface).
type logCapturingLogger struct{ entries []capturedLog }

func (l *logCapturingLogger) Debug(string, ...any)  {}
func (l *logCapturingLogger) Debugf(string, ...any) {}
func (l *logCapturingLogger) Info(string, ...any)   {}
func (l *logCapturingLogger) Infof(string, ...any)  {}
func (l *logCapturingLogger) Warn(string, ...any)   {}
func (l *logCapturingLogger) Warnf(string, ...any)  {}
func (l *logCapturingLogger) Error(msg string, args ...any) {
	l.entries = append(l.entries, capturedLog{msg: msg, args: append([]any(nil), args...)})
}
func (l *logCapturingLogger) Errorf(string, ...any) {}
func (l *logCapturingLogger) Panic(string, ...any)  {}
func (l *logCapturingLogger) Panicf(string, ...any) {}

// TestAuthDeniedLogNamesTheLifecycleReason drives the whole rejection through a
// real Core — authenticator, authDeny, logger — and asserts the thing an
// operator actually reads.
//
// Every other test in this file stops at authenticate() and inspects a returned
// error. That leaves the delivery mechanism untested: authDeny is free to log
// something other than the error it was handed (a fixed string, a
// TokenFingerprint, nothing at all) and every authenticate-level assertion
// still passes. This test closes that gap from the other end: it reads the
// auth_denied event's err field and requires the reason to be legible there,
// while requiring the value returned to the caller to remain the
// ErrUnauthenticated constant.
func TestAuthDeniedLogNamesTheLifecycleReason(t *testing.T) {
	base := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	// Spelled out in full: "tok" is a substring of "unknown auth token", so a
	// short token would make the "token never reaches the log" check unfailable.
	const secret = "s3cret-runner-token"

	live := IssuedIdentity{
		RunnerID: "runner-1", TokenHash: HashSecret(secret), CodeID: "c",
		IssuedAt: base.Add(-time.Hour), ExpiresAt: base.Add(time.Hour),
	}
	expired := live
	expired.ExpiresAt = base.Add(-time.Minute)
	revoked := live
	revoked.RevokedAt = base.Add(-time.Minute)

	for _, tc := range []struct {
		name     string
		identity IssuedIdentity
		// wantSubstr is the reason word an operator greps for. It must be
		// absent from the plain unknown-token rejection, which the loop below
		// also checks, so "unknown auth token" alone cannot satisfy it.
		wantSubstr string
	}{
		{"expired", expired, "expired"},
		{"revoked", revoked, "revoked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := NewMemoryIssuedIdentityStore()
			if err := st.Issue(context.Background(), tc.identity); err != nil {
				t.Fatalf("issue: %v", err)
			}
			auth := NewIssuedIdentityAuthenticator(st)
			auth.now = func() time.Time { return base }

			logger := &logCapturingLogger{}
			core := &Core{auth: auth, logger: logger}

			_, err := core.heartbeat(context.Background(), protocol.HeartbeatRequest{
				RunnerID:  "runner-1",
				SessionID: "s-1",
				Capacity:  1,
				AuthToken: secret,
			}, TransportInfo{})

			// The caller learns nothing beyond "unauthenticated".
			if err != ErrUnauthenticated {
				t.Fatalf("heartbeat err = %v (%T), want the ErrUnauthenticated constant", err, err)
			}

			var denied *capturedLog
			for i := range logger.entries {
				if logger.entries[i].msg == "auth_denied" {
					denied = &logger.entries[i]
					break
				}
			}
			if denied == nil {
				t.Fatalf("no auth_denied event logged; entries = %+v", logger.entries)
			}
			raw, ok := denied.field("err")
			if !ok {
				t.Fatalf("auth_denied carries no err field: %+v", denied.args)
			}
			logged, ok := raw.(error)
			if !ok {
				t.Fatalf("auth_denied err field = %#v, want an error", raw)
			}
			if !strings.Contains(logged.Error(), tc.wantSubstr) {
				t.Fatalf("auth_denied logged err=%q, which does not name %q; an operator "+
					"reading this cannot tell a lapsed/revoked identity from a bad token "+
					"and will go audit credential distribution instead of "+
					"--runner-identity-ttl and the renewal loop",
					logged.Error(), tc.wantSubstr)
			}
			// The log line still identifies the same failure class, so existing
			// alerting on "unknown auth token" keeps matching.
			if !errors.Is(logged, ErrAuthUnknownToken) {
				t.Fatalf("auth_denied logged err=%v, which no longer matches ErrAuthUnknownToken", logged)
			}
			// And the token itself is never in the log (org policy blacklist);
			// authDeny logs a fingerprint in its own field instead.
			if strings.Contains(logged.Error(), secret) {
				t.Fatalf("auth_denied logged err=%q, which contains the token", logged.Error())
			}
		})
	}

	// Control: the unknown-token rejection must NOT contain either reason word,
	// otherwise the assertions above would be satisfied by a message that says
	// everything on every path and distinguishes nothing.
	logger := &logCapturingLogger{}
	core := &Core{auth: NewIssuedIdentityAuthenticator(NewMemoryIssuedIdentityStore()), logger: logger}
	if _, err := core.heartbeat(context.Background(), protocol.HeartbeatRequest{
		RunnerID: "runner-1", SessionID: "s-1", Capacity: 1, AuthToken: secret,
	}, TransportInfo{}); err != ErrUnauthenticated {
		t.Fatalf("unknown-token heartbeat err = %v, want ErrUnauthenticated", err)
	}
	if len(logger.entries) == 0 {
		t.Fatal("unknown-token rejection logged nothing")
	}
	raw, _ := logger.entries[0].field("err")
	unknownText := ""
	if e, ok := raw.(error); ok {
		unknownText = e.Error()
	}
	for _, word := range []string{"expired", "revoked"} {
		if strings.Contains(unknownText, word) {
			t.Fatalf("unknown-token rejection logs %q, which already contains %q; "+
				"the lifecycle reasons are then indistinguishable from it", unknownText, word)
		}
	}
}
