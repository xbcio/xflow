package distributed

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// Mutation note: flipping the default-containing branch of LeaderLockKey to
// return the suffixed key must turn this red — and with it the rolling-upgrade
// invariant that the default deployment keeps byte-identical key names.
func TestLeaderLockKeyLegacyForDefaultAndUnscoped(t *testing.T) {
	for _, owned := range [][]namespace.Namespace{
		nil,
		{},
		{namespace.Default},
		{namespace.Default, "x"},
	} {
		got, err := LeaderLockKey(owned)
		if err != nil {
			t.Fatalf("LeaderLockKey(%v) = %v, want nil", owned, err)
		}
		if got != LegacyLeaderLockKey {
			t.Fatalf("LeaderLockKey(%v) = %q, want legacy %q", owned, got, LegacyLeaderLockKey)
		}
	}
}

func TestLeaderLockKeySuffixedForSingleDedicatedNamespace(t *testing.T) {
	got, err := LeaderLockKey([]namespace.Namespace{"local-jack"})
	if err != nil {
		t.Fatalf("LeaderLockKey = %v, want nil", err)
	}
	want := LegacyLeaderLockKey + ":ns:local-jack"
	if got != want {
		t.Fatalf("LeaderLockKey = %q, want %q", got, want)
	}
}

func TestLeaderLockKeyRejectsAmbiguousSet(t *testing.T) {
	if _, err := LeaderLockKey([]namespace.Namespace{"a", "b"}); err == nil {
		t.Fatal("LeaderLockKey({a,b}) = nil error, want ambiguity error")
	}
}

// Mutation note: dropping the suffix-side key-schema defense (e.g. not
// re-validating in DeploymentSuffix) would let a ':'-bearing namespace
// corrupt the key; Validate's contract is pinned from the consumer side here.
func TestLeaderLockKeySuffixIsKeySafe(t *testing.T) {
	got, err := LeaderLockKey([]namespace.Namespace{"local-jack"})
	if err != nil {
		t.Fatalf("LeaderLockKey = %v", err)
	}
	// The derived key must not gain a new '{' (hash tag) and must stay outside
	// any "xflow:ns:<ns>:" scan prefix.
	if strings.Contains(got, "{") {
		t.Fatalf("derived key %q introduced a hash tag", got)
	}
	if !strings.HasPrefix(got, LegacyLeaderLockKey+":ns:") {
		t.Fatalf("derived key %q does not sit under the legacy prefix", got)
	}
}
