package xflow

import (
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
)

// TestNewServerRequiresRunnerAuthPosture pins that an embedder cannot get an
// unauthenticated runner protocol by omission. The runner protocol's claim,
// heartbeat and complete routes carry no other authentication, so a silent
// permissive default is the whole of the decision — it should be made out loud.
func TestNewServerRequiresRunnerAuthPosture(t *testing.T) {
	_, err := NewServer(ServerConfig{})
	if err == nil {
		t.Fatal("NewServer accepted an undeclared runner-auth posture")
	}
	if !errors.Is(err, ErrRunnerAuthPostureUndeclared) {
		t.Fatalf("want ErrRunnerAuthPostureUndeclared, got %v", err)
	}
}

// TestNewServerAcceptsExplicitInsecure is the positive control for the escape
// hatch: dev and test embedders keep today's behavior by naming it.
func TestNewServerAcceptsExplicitInsecure(t *testing.T) {
	if _, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth()); err != nil {
		t.Fatalf("explicit insecure posture rejected: %v", err)
	}
}

// TestNewServerAcceptsAuthenticator is the positive control for the intended
// path.
func TestNewServerAcceptsAuthenticator(t *testing.T) {
	auth, err := control.NewStaticTokenAuthenticator("runner-", "loopback-token", []string{"default"}, []string{"*"})
	if err != nil {
		t.Fatalf("static token authenticator: %v", err)
	}
	if _, err := NewServer(ServerConfig{}, WithServerAuth(auth)); err != nil {
		t.Fatalf("authenticator path rejected: %v", err)
	}
}

// TestNewServerRejectsBothPostures pins the other failure mode: passing both
// WithServerAuth and WithServerInsecureNoRunnerAuth is a contradiction, not a
// "insecure wins" or "auth wins" default that a caller could get wrong by
// composing options from two code paths.
func TestNewServerRejectsBothPostures(t *testing.T) {
	auth, err := control.NewStaticTokenAuthenticator("runner-", "loopback-token", []string{"default"}, []string{"*"})
	if err != nil {
		t.Fatalf("static token authenticator: %v", err)
	}
	if _, err := NewServer(ServerConfig{}, WithServerAuth(auth), WithServerInsecureNoRunnerAuth()); err == nil {
		t.Fatal("NewServer accepted both WithServerAuth and WithServerInsecureNoRunnerAuth")
	}
}

// TestNewServerRejectsDisabledAuthenticatorSentinel pins that
// WithServerAuth(control.DisabledAuthenticator{}) does not count as declaring
// a runner-auth posture. DisabledAuthenticator{} is a non-nil Authenticator
// with a fully permissive policy, so `sc.auth == nil` alone cannot tell
// "explicitly disabled" apart from "really configured" — control.IsConfigured
// exists for exactly that distinction. Without this gate, an embedder could
// write WithServerAuth(control.DisabledAuthenticator{}), which reads like "I
// configured auth", and get a runner protocol that accepts every runner.
func TestNewServerRejectsDisabledAuthenticatorSentinel(t *testing.T) {
	_, err := NewServer(ServerConfig{}, WithServerAuth(control.DisabledAuthenticator{}))
	if err == nil {
		t.Fatal("NewServer accepted WithServerAuth(control.DisabledAuthenticator{}) as a declared posture")
	}
	if !errors.Is(err, ErrRunnerAuthPostureUndeclared) {
		t.Fatalf("want ErrRunnerAuthPostureUndeclared, got %v", err)
	}
}

// TestNewServerRejectsDisabledAuthenticatorPlusInsecure pins M3, the reverse
// judgment: WithServerAuth(DisabledAuthenticator{}) plus
// WithServerInsecureNoRunnerAuth() must still be rejected as two contradictory
// postures declared at once. This is deliberately asymmetric with the posture
// gate above — this check must keep using sc.auth != nil (which
// DisabledAuthenticator{} satisfies), not control.IsConfigured(sc.auth); if it
// were switched to IsConfigured, this exact combination would slip through
// because DisabledAuthenticator{} is never "configured", silently widening
// what NewServer accepts.
func TestNewServerRejectsDisabledAuthenticatorPlusInsecure(t *testing.T) {
	_, err := NewServer(ServerConfig{}, WithServerAuth(control.DisabledAuthenticator{}), WithServerInsecureNoRunnerAuth())
	if err == nil {
		t.Fatal("NewServer accepted WithServerAuth(DisabledAuthenticator{}) together with WithServerInsecureNoRunnerAuth()")
	}
}

// TestStaticTokenAuthenticatorRejectsEmptyInputs pins the observable behavior
// that the loopback helper cannot be talked into accepting everyone: an empty
// token would match a runner that sent no credential, and an empty prefix
// would name no subject at all. It does NOT pin NewStaticTokenAuthenticator's
// own two guards specifically — the underlying FilePolicyStore.resolveConfig
// independently rejects both an empty token (no token/token_file/mtls_subject)
// and an empty id_prefix, so deleting either guard here leaves this test
// green via that lower layer. The guards stay anyway as defense-in-depth at
// this exported constructor's boundary, since resolveConfig is an internal
// implementation detail that could change independently of this API.
func TestStaticTokenAuthenticatorRejectsEmptyInputs(t *testing.T) {
	if _, err := control.NewStaticTokenAuthenticator("runner-", "", []string{"default"}, []string{"*"}); err == nil {
		t.Fatal("NewStaticTokenAuthenticator accepted an empty token")
	}
	if _, err := control.NewStaticTokenAuthenticator("", "tok", []string{"default"}, []string{"*"}); err == nil {
		t.Fatal("NewStaticTokenAuthenticator accepted an empty runner ID prefix")
	}
}

// TestStaticTokenAuthenticatorAuthenticates walks the real authentication path.
// The constructor tests above only prove it was built; this proves the wrapper
// actually delegates — a right token under the right prefix is accepted, a
// wrong token is not, and a runner ID outside the prefix is not.
func TestStaticTokenAuthenticatorAuthenticates(t *testing.T) {
	auth, err := control.NewStaticTokenAuthenticator("sas-", "tok", []string{"default"}, []string{"*"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if _, err := auth.AuthenticateRegister("sas-runner-1", "tok", control.TransportInfo{}); err != nil {
		t.Fatalf("right prefix + right token rejected: %v", err)
	}
	if _, err := auth.AuthenticateRegister("sas-runner-1", "wrong", control.TransportInfo{}); err == nil {
		t.Fatal("wrong token accepted")
	}
	if _, err := auth.AuthenticateRegister("other-runner", "tok", control.TransportInfo{}); err == nil {
		t.Fatal("runner ID outside the prefix accepted")
	}
}

// TestStaticTokenAuthenticatorEmptyNamespacesMeansDefaultOnly pins the
// fail-closed meaning of an empty allowedNamespaces argument:
// RunnerPolicy.AllowsNamespace treats an empty set as "default namespace
// only", not "every namespace". A future edit that "simplifies" the empty case
// to []string{"*"} would silently grant every namespace to a token that named
// none — this test exists so that edit turns red instead of shipping quietly.
func TestStaticTokenAuthenticatorEmptyNamespacesMeansDefaultOnly(t *testing.T) {
	auth, err := control.NewStaticTokenAuthenticator("runner-", "tok", nil, []string{"*"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	policy, err := auth.AuthenticateRegister("runner-1", "tok", control.TransportInfo{})
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if !policy.AllowsNamespace(namespace.Default) {
		t.Fatal("policy built with no allowedNamespaces denies the default namespace")
	}
	if policy.AllowsNamespace(namespace.Namespace("some-other-namespace")) {
		t.Fatal("policy built with no allowedNamespaces allows a non-default namespace")
	}
}
