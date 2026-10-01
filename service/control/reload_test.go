package control

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// writePolicyFile writes contents to path at mode, failing the test on error.
// A helper distinct from mustWriteFile (auth_test.go) only in name, kept local
// to this file so this file's intent (building a runners.yaml on disk for
// Reload to read) is self-contained.
func writePolicyFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

const validPolicyV1 = `version: 1
runners:
  - name: orders
    id_prefix: order-
    token: "original-token"
    allowed_node_types: ["*"]
`

// TestFilePolicyStoreReloadPicksUpNewToken pins that a Reload which adds a
// runner entry with a new token makes that token authenticate immediately,
// without reconstructing the store.
func TestFilePolicyStoreReloadPicksUpNewToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	if _, err := store.AuthenticateRegister("order-1", "new-token", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("before reload: err = %v, want ErrAuthUnknownToken", err)
	}

	writePolicyFile(t, path, `version: 1
runners:
  - name: orders
    id_prefix: order-
    token: "new-token"
    allowed_node_types: ["*"]
`, 0o600)
	if err := store.Reload(path); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, err := store.AuthenticateRegister("order-1", "new-token", TransportInfo{}); err != nil {
		t.Fatalf("after reload: AuthenticateRegister(new-token) err = %v, want nil", err)
	}
	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("after reload: AuthenticateRegister(original-token) err = %v, want ErrAuthUnknownToken", err)
	}
}

// TestFilePolicyStoreReloadDeniesRemovedToken pins that a Reload which drops a
// runner entry makes that entry's token stop authenticating immediately.
func TestFilePolicyStoreReloadDeniesRemovedToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); err != nil {
		t.Fatalf("before reload: err = %v, want nil", err)
	}

	writePolicyFile(t, path, `version: 1
runners:
  - name: other
    id_prefix: other-
    token: "unrelated-token"
    allowed_node_types: ["*"]
`, 0o600)
	if err := store.Reload(path); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("after reload: err = %v, want ErrAuthUnknownToken (removed entry denied)", err)
	}
}

// TestFilePolicyStoreReloadMalformedYAMLKeepsOldPolicy pins that a Reload
// failure (malformed YAML) returns an error AND leaves the previous snapshot
// serving unchanged — the atomic.Pointer swap in Reload only happens after
// every check succeeds.
func TestFilePolicyStoreReloadMalformedYAMLKeepsOldPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}

	writePolicyFile(t, path, "not: [valid: yaml: at all", 0o600)
	if err := store.Reload(path); err == nil {
		t.Fatal("Reload with malformed YAML: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); err != nil {
		t.Fatalf("after failed reload: AuthenticateRegister(original-token) err = %v, want nil (old policy still in force)", err)
	}
}

// TestFilePolicyStoreReloadWorldReadableTokenFileKeepsOldPolicy pins the same
// invariant for a token_file whose permissions loosened to 0644 between loads:
// resolveConfig's token_file check must fail the whole Reload, not just that
// one entry, and the previous snapshot must still serve.
func TestFilePolicyStoreReloadWorldReadableTokenFileKeepsOldPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}

	tokenFile := filepath.Join(dir, "token.txt")
	writePolicyFile(t, tokenFile, "insecure-token\n", 0o644) // world/group-readable
	writePolicyFile(t, path, `version: 1
runners:
  - name: orders
    id_prefix: order-
    token_file: "`+tokenFile+`"
    allowed_node_types: ["*"]
`, 0o600)

	if err := store.Reload(path); err == nil {
		t.Fatal("Reload with 0644 token_file: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); err != nil {
		t.Fatalf("after failed reload: AuthenticateRegister(original-token) err = %v, want nil (old policy still in force)", err)
	}
	if _, err := store.AuthenticateRegister("order-1", "insecure-token", TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("after failed reload: AuthenticateRegister(insecure-token) err = %v, want ErrAuthUnknownToken (rejected entry never took effect)", err)
	}
}

// TestFilePolicyStoreReloadMissingFileKeepsOldPolicy pins the same invariant
// when the policy file itself is deleted or moved before Reload runs.
func TestFilePolicyStoreReloadMissingFileKeepsOldPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove policy file: %v", err)
	}
	if err := store.Reload(path); err == nil {
		t.Fatal("Reload with missing file: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", TransportInfo{}); err != nil {
		t.Fatalf("after failed reload: AuthenticateRegister(original-token) err = %v, want nil (old policy still in force)", err)
	}
}

// TestFilePolicyStoreReloadConcurrentAuthenticateIsRaceFree exercises Reload
// racing against a burst of concurrent AuthenticateRegister calls. It asserts
// no crash / no data race (run this test with -race); it does not assert which
// specific answer a request straddling the swap receives, since that instant
// is legitimately either side of the atomic swap.
func TestFilePolicyStoreReloadConcurrentAuthenticateIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writePolicyFile(t, path, validPolicyV1, 0o600)

	store, err := NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}

	const readers = 8
	const reloads = 50
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = store.AuthenticateRegister("order-1", "original-token", TransportInfo{})
				}
			}
		}()
	}

	for i := 0; i < reloads; i++ {
		if err := store.Reload(path); err != nil {
			t.Errorf("Reload iteration %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}
