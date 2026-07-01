package control

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func mustWriteFile(t *testing.T, path string, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatal(err)
	}
}

func TestFilePolicyStoreAcceptsMatchingToken(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name:             "orders",
			IDPrefix:         "order-runner-",
			Token:            "s3cret",
			AllowedNodeTypes: []string{"xflow.function"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := store.AuthenticateRegister("order-runner-1", "s3cret", TransportInfo{})
	if err != nil {
		t.Fatalf("AuthenticateRegister() error = %v", err)
	}
	if policy.Name != "orders" || !policy.Allows("xflow.function") {
		t.Fatalf("policy = %+v, want orders/xflow.function allowed", policy)
	}
	if policy.Allows("xflow.database") {
		t.Fatal("policy allowed disallowed node type")
	}
}

func TestFilePolicyStoreRejectsUnknownToken(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{IDPrefix: "r-", Token: "correct", AllowedNodeTypes: []string{"*"}}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateRegister("r-1", "wrong", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("err = %v, want ErrAuthUnknownToken", err)
	}
	if _, err := store.AuthenticateRegister("r-1", "", TransportInfo{}); !errors.Is(err, ErrAuthMissingToken) {
		t.Fatalf("err (empty token) = %v, want ErrAuthMissingToken", err)
	}
}

func TestFilePolicyStoreEnforcesIDPrefix(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{IDPrefix: "payment-", Token: "tok", AllowedNodeTypes: []string{"*"}}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateRegister("evil-runner", "tok", TransportInfo{}); !errors.Is(err, ErrAuthIDPrefixDenied) {
		t.Fatalf("err = %v, want ErrAuthIDPrefixDenied", err)
	}
}

func TestFilePolicyStoreRequiresBothTokenAndMTLSWhenConfigured(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			IDPrefix:         "r-",
			Token:            "tok",
			MTLSSubject:      "CN=runner-prod",
			AllowedNodeTypes: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	// Token matches but no TLS peer info — should fail.
	if _, err := store.AuthenticateRegister("r-1", "tok", TransportInfo{}); err == nil {
		t.Fatal("expected auth failure without mTLS peer")
	}
	// Both match — should succeed.
	if _, err := store.AuthenticateRegister("r-1", "tok", TransportInfo{TLSPeerCN: "CN=runner-prod"}); err != nil {
		t.Fatalf("both match, err = %v", err)
	}
}

func TestFilePolicyStoreWildcardAllowsAllNodeTypes(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{IDPrefix: "any-", Token: "tok", AllowedNodeTypes: []string{"*"}}},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := store.AuthenticateRegister("any-1", "tok", TransportInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, nt := range []string{"xflow.function", "xflow.database", "xflow.http", "custom.type"} {
		if !policy.Allows(nt) {
			t.Fatalf("wildcard policy did not allow %q", nt)
		}
	}
}

func TestFilePolicyStoreDryRunSuppressesDenials(t *testing.T) {
	store, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{IDPrefix: "r-", Token: "real", AllowedNodeTypes: []string{"*"}}},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := store.AuthenticateRegister("r-1", "wrong", TransportInfo{})
	if err == nil {
		t.Fatal("expected err to carry the underlying denial for logging")
	}
	if !IsDryRunDenial(err) {
		t.Fatalf("err = %v, want dry-run denial", err)
	}
	if !policy.Allows("xflow.function") {
		t.Fatal("dry-run should return permissive policy so the request proceeds")
	}
}

func TestFilePolicyStoreReloadRefusesInsecureFileMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	mustWriteFile(t, path, `version: 1
runners: []`, 0o644)
	if _, err := NewFilePolicyStore(path, false); err == nil {
		t.Fatal("expected insecure file mode to be refused")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFilePolicyStore(path, false); err != nil {
		t.Fatalf("secure mode reload err = %v", err)
	}
}

func TestFilePolicyStoreReloadParsesYAMLAndSwapsAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	mustWriteFile(t, path, `version: 1
runners:
  - id_prefix: v1-
    token: initial
    allowed_node_types: ["xflow.http"]
`, 0o600)
	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateRegister("v1-a", "initial", TransportInfo{}); err != nil {
		t.Fatalf("initial auth err = %v", err)
	}
	// Rewrite with new token; secure the new file first.
	mustWriteFile(t, path, `version: 1
runners:
  - id_prefix: v1-
    token: rotated
    allowed_node_types: ["xflow.http"]
`, 0o600)
	if err := store.Reload(path); err != nil {
		t.Fatalf("Reload err = %v", err)
	}
	if _, err := store.AuthenticateRegister("v1-a", "initial", TransportInfo{}); err == nil {
		t.Fatal("old token still accepted after reload")
	}
	if _, err := store.AuthenticateRegister("v1-a", "rotated", TransportInfo{}); err != nil {
		t.Fatalf("new token rejected after reload: %v", err)
	}
}

func TestTokenFingerprintIsStableAndNonEmpty(t *testing.T) {
	a := TokenFingerprint("secret")
	b := TokenFingerprint("secret")
	if a != b {
		t.Fatalf("fingerprints diverge for identical input: %q vs %q", a, b)
	}
	if len(a) != 8 || a == "none" {
		t.Fatalf("fingerprint = %q, want 8 hex chars", a)
	}
	if TokenFingerprint("") != "none" {
		t.Fatal("empty token should return 'none'")
	}
}

func TestDisabledAuthenticatorAlwaysPermissive(t *testing.T) {
	var a Authenticator = DisabledAuthenticator{}
	p, err := a.AuthenticateRegister("anyone", "", TransportInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Allows("xflow.custom") {
		t.Fatal("disabled auth should always allow")
	}
}
