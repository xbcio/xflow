package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xbcio/xflow/service/control"
)

// buildAuthenticator had no test of its own: grep -rn "buildAuthenticator("
// across cmd/server's _test.go files matches only the production call site in
// main.go, never a test. --auth-policy and --auth-dry-run were similarly
// never asserted on out of parseServerConfig. A flag that binds, parses, and
// is then ignored by buildAuthenticator (e.g. the authPolicy=="" branch
// swallowing a non-empty path) would leave every runner request unauthenticated
// against a control plane the operator believed was running policy-gated
// auth, and no test in this package would go red.

func TestParseServerConfigSupportsAuthPolicyFlags(t *testing.T) {
	cfg, err := parseServerConfig([]string{
		"-memory",
		"-auth-policy", "/etc/xflow/runners.yaml",
		"-auth-dry-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.authPolicy != "/etc/xflow/runners.yaml" {
		t.Fatalf("authPolicy = %q, want /etc/xflow/runners.yaml", cfg.authPolicy)
	}
	if !cfg.authDryRun {
		t.Fatal("authDryRun = false, want true")
	}
}

// writeRunnersPolicyFile writes a minimal, valid runners.yaml at 0600 (the
// permission control.FilePolicyStore.Reload requires).
func writeRunnersPolicyFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runners.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write runners.yaml: %v", err)
	}
	return path
}

func TestBuildAuthenticatorDefaultsToDisabledWhenNoPolicy(t *testing.T) {
	auth, err := buildAuthenticator(serverConfig{})
	if err != nil {
		t.Fatalf("buildAuthenticator: %v", err)
	}
	if _, ok := auth.(control.DisabledAuthenticator); !ok {
		t.Fatalf("buildAuthenticator() = %T, want control.DisabledAuthenticator when --auth-policy is unset", auth)
	}
}

// TestBuildAuthenticatorLoadsFilePolicyStore proves --auth-policy actually
// switches the returned Authenticator, not just that it parses. A regression
// that made buildAuthenticator always return DisabledAuthenticator regardless
// of cfg.authPolicy would leave every runner request accepted no matter what
// policy file was configured, and TestParseServerConfigSupportsAuthPolicyFlags
// alone (flag parsing only) would not catch it.
func TestBuildAuthenticatorLoadsFilePolicyStore(t *testing.T) {
	path := writeRunnersPolicyFile(t, "version: 1\nrunners:\n  - name: r1\n    id_prefix: \"runner-\"\n    token: tok\n    allowed_node_types: [\"*\"]\n")

	auth, err := buildAuthenticator(serverConfig{authPolicy: path})
	if err != nil {
		t.Fatalf("buildAuthenticator: %v", err)
	}
	if _, ok := auth.(*control.FilePolicyStore); !ok {
		t.Fatalf("buildAuthenticator() = %T, want *control.FilePolicyStore when --auth-policy is set", auth)
	}
}

// TestBuildAuthenticatorPropagatesFilePolicyStoreError proves a malformed or
// missing policy file fails server startup rather than silently falling back
// to the disabled (allow-everything) authenticator.
func TestBuildAuthenticatorPropagatesFilePolicyStoreError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	if _, err := buildAuthenticator(serverConfig{authPolicy: missing}); err == nil {
		t.Fatal("buildAuthenticator() error = nil, want error for a policy file that does not exist")
	}
}

// TestBuildAuthenticatorPropagatesDryRun covers the second argument of
// control.NewFilePolicyStore(cfg.authPolicy, cfg.authDryRun), which the tests
// above leave at its zero value and therefore never observe. Hardcoding that
// argument to true would turn every configured enforcing policy into
// allow-everything-and-log; hardcoding it to false would make an operator's
// staged rollout start rejecting runners on day one. Both are silent: the
// returned type is *control.FilePolicyStore either way.
//
// The assertion is behavioural rather than a getter read: it presents an
// unknown token and checks whether the denial is suppressed into a permissive
// policy, which is what dryRun actually changes at the request path
// (FilePolicyStore.deny).
func TestBuildAuthenticatorPropagatesDryRun(t *testing.T) {
	const policy = "version: 1\nrunners:\n  - name: r1\n    id_prefix: \"runner-\"\n    token: tok\n    allowed_node_types: [\"*\"]\n"

	for _, tc := range []struct {
		name        string
		dryRun      bool
		wantDryRun  bool
		wantErrKind string
	}{
		{name: "enforcing", dryRun: false, wantDryRun: false, wantErrKind: "a hard denial"},
		{name: "dry-run", dryRun: true, wantDryRun: true, wantErrKind: "a suppressed dry-run denial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeRunnersPolicyFile(t, policy)
			auth, err := buildAuthenticator(serverConfig{authPolicy: path, authDryRun: tc.dryRun})
			if err != nil {
				t.Fatalf("buildAuthenticator: %v", err)
			}
			store, ok := auth.(*control.FilePolicyStore)
			if !ok {
				t.Fatalf("buildAuthenticator() = %T, want *control.FilePolicyStore", auth)
			}
			if got := store.IsDryRun(); got != tc.wantDryRun {
				t.Fatalf("IsDryRun() = %v, want %v", got, tc.wantDryRun)
			}

			// "runner-x" matches the policy's id_prefix but presents a token
			// the policy does not know, so this reaches deny() -- the only
			// place dryRun changes the outcome.
			gotPolicy, authErr := store.AuthenticateRegister("runner-x", "wrong-token", control.TransportInfo{})
			if authErr == nil {
				t.Fatalf("AuthenticateRegister with an unknown token succeeded outright, want %s", tc.wantErrKind)
			}
			if got := control.IsDryRunDenial(authErr); got != tc.wantDryRun {
				t.Fatalf("IsDryRunDenial(%v) = %v, want %v -- buildAuthenticator did not forward authDryRun", authErr, got, tc.wantDryRun)
			}
			// Under dry-run the caller must still receive a usable policy so
			// the request proceeds; enforcing must hand back nothing.
			if tc.wantDryRun && len(gotPolicy.AllowedNodeTypes) == 0 {
				t.Fatalf("dry-run denial returned an empty policy %#v; the request would be rejected downstream anyway", gotPolicy)
			}
			if !tc.wantDryRun && len(gotPolicy.AllowedNodeTypes) != 0 {
				t.Fatalf("enforcing denial returned a non-empty policy %#v", gotPolicy)
			}
		})
	}
}
