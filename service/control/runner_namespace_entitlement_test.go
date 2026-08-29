package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

// newEntitlementTestCore assembles a *Core wired with the given Authenticator
// and a fresh MemoryRunnerDirectory. Modeled on newRenewLeaseAuthTestCore
// (renew_lease_auth_test.go), which is the existing helper for exercising
// Core.register against a real Authenticator/FilePolicyStore; unlike that
// helper this one does not pre-register a session, since these tests call
// register directly and inspect its own accept/reject outcome.
func newEntitlementTestCore(t *testing.T, auth Authenticator) *Core {
	t.Helper()
	return &Core{
		runners:  NewMemoryRunnerDirectory(),
		auth:     auth,
		pollWait: time.Second,
	}
}

func entitlementPolicyStore(t *testing.T, allowed []string) *FilePolicyStore {
	t.Helper()
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:              "team-a",
			IDPrefix:          "runner-",
			Token:             "team-a-token",
			AllowedNodeTypes:  []string{"*"},
			AllowedNamespaces: allowed,
		}},
	}, false)
	if err != nil {
		t.Fatalf("policy store: %v", err)
	}
	return store
}

// TestRegisterRejectsUnauthorizedNamespace pins that a runner cannot join a
// namespace its policy does not grant. Before this guard the declared value was
// stored verbatim and ClaimForRunner filtered task dispatch by it, so declaring
// another namespace was enough to be handed its queued assignments.
func TestRegisterRejectsUnauthorizedNamespace(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, []string{"team-a"}))

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{"team-b"},
	}, TransportInfo{})

	if err == nil {
		t.Fatal("register accepted a namespace the policy does not grant")
	}
	if !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("want ErrAuthNamespaceDenied, got %v", err)
	}
}

// TestRegisterAcceptsAuthorizedNamespace is the positive control: without it the
// test above would pass against an implementation that rejects every namespace.
func TestRegisterAcceptsAuthorizedNamespace(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, []string{"team-a"}))

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{"team-a"},
	}, TransportInfo{}); err != nil {
		t.Fatalf("register rejected an authorized namespace: %v", err)
	}
}

// TestEmptyAllowedNamespacesMeansDefaultOnly pins the back-compat semantics to
// the value canServeNamespace already uses for an empty set: default only, not
// "everything". A policy file that predates this field must not silently grant
// every namespace.
func TestEmptyAllowedNamespacesMeansDefaultOnly(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, nil))

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{string(namespace.Default)},
	}, TransportInfo{}); err != nil {
		t.Fatalf("empty allowed_namespaces must still permit default: %v", err)
	}

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-2",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{"team-b"},
	}, TransportInfo{})
	if !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("empty allowed_namespaces must deny non-default; got %v", err)
	}
}

// TestDisabledAuthenticatorStillAllowsAnyNamespace pins that turning auth off
// keeps today's behavior. The decision to run without runner auth is made once,
// explicitly, at server construction; re-denying here would produce a second
// contradictory rejection point with no policy behind it.
func TestDisabledAuthenticatorStillAllowsAnyNamespace(t *testing.T) {
	c := newEntitlementTestCore(t, DisabledAuthenticator{})

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		Namespaces:  []string{"team-b"},
	}, TransportInfo{}); err != nil {
		t.Fatalf("disabled authenticator must not gate namespaces: %v", err)
	}
}

// TestRegisterRejectsUndeclaredNamespaceWithoutGrant pins the effective-value
// gate: a runner that declares no Namespaces at all is not asking for "no
// namespace" — normalizeRunnerNamespaces downstream resolves that to
// [namespace.Default] regardless of what the entitlement check saw. Before
// this guard, namespaceIDs(nil) returned nil, the entitlement loop ran zero
// iterations, and the runner registered into default with zero policy checks
// even though this policy's AllowedNamespaces does not include it.
func TestRegisterRejectsUndeclaredNamespaceWithoutGrant(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, []string{"team-a"}))

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
	}, TransportInfo{})

	if !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("want ErrAuthNamespaceDenied for an undeclared namespace under a policy that excludes default, got %v", err)
	}
}

// TestRegisterRejectsBlankNamespaceWithoutGrant pins the second door called
// out in the review: declaring [""] must not silently bypass the gate either.
// namespaceIDs drops empty strings, so this exercises the same effective-set
// path as the fully-undeclared case above via a different input shape.
func TestRegisterRejectsBlankNamespaceWithoutGrant(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, []string{"team-a"}))

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{""},
	}, TransportInfo{})

	if !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("want ErrAuthNamespaceDenied for a blank-string namespace under a policy that excludes default, got %v", err)
	}
}

// TestRegisterUndeclaredNamespaceBackCompat is the positive control for the
// two tests above: a legacy runner that declares no Namespaces must still be
// able to register under a legacy policy (empty AllowedNamespaces, meaning
// default-only). Without this control, the fix above could regress into
// "reject every undeclared namespace unconditionally" and both denial tests
// would stay green while breaking every pre-existing deployment.
func TestRegisterUndeclaredNamespaceBackCompat(t *testing.T) {
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, nil))

	if _, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
	}, TransportInfo{}); err != nil {
		t.Fatalf("undeclared namespace under a default-only policy must still register: %v", err)
	}

	snap, ok := c.runners.Runner(context.Background(), "runner-1")
	if !ok {
		t.Fatal("registered runner not found in directory")
	}
	if len(snap.Namespaces) != 1 || snap.Namespaces[0] != namespace.Default {
		t.Fatalf("stored namespaces = %v, want [%q]", snap.Namespaces, namespace.Default)
	}
}

// recordingNamespaceAuthObserver is a test-only AuthObserver implementation
// that records every decision in order, so a test can assert exactly what
// fired (and what did not) instead of only "at least one allow happened
// somewhere". Named distinctly from auth_test.go's recordingAuthObserver
// (same package, already declared with a different field/format:
// "op:result:authMode" vs. this file's "op:result") to avoid a redeclaration.
type recordingNamespaceAuthObserver struct {
	decisions []string // "op:result" pairs, in order
}

func (o *recordingNamespaceAuthObserver) OnAuthDecision(_ context.Context, op, result, _ string) {
	o.decisions = append(o.decisions, op+":"+result)
}

// TestNamespaceDenialIsObservable pins that a namespace denial reaches the auth
// decision observer. Namespace is this system's authorization boundary, so a
// denial on it that emits nothing is an operational blind spot — and this repo
// has shipped metric families that were wired but never fired.
func TestNamespaceDenialIsObservable(t *testing.T) {
	obs := &recordingNamespaceAuthObserver{}
	c := newEntitlementTestCore(t, entitlementPolicyStore(t, []string{"team-a"}))
	c.authObserver = obs

	_, err := c.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID:    "runner-1",
		Concurrency: 1,
		AuthToken:   "team-a-token",
		Namespaces:  []string{"team-b"},
	}, TransportInfo{})
	if !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("setup: want ErrAuthNamespaceDenied, got %v", err)
	}

	// Exactly one decision, and it is the namespace denial — not the "allow"
	// from the token check that preceded it. An assertion of "contains" would
	// pass even if the denial never fired, since the allow is always there.
	want := []string{"register:allow", "register:deny_namespace"}
	if len(obs.decisions) != len(want) {
		t.Fatalf("auth decisions = %v, want exactly %v", obs.decisions, want)
	}
	for i := range want {
		if obs.decisions[i] != want[i] {
			t.Fatalf("auth decisions = %v, want exactly %v", obs.decisions, want)
		}
	}
}
