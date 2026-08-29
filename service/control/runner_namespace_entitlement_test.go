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
