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

// A namespace that could corrupt the key schema must never reach the key: ':'
// would forge a longer path, '{' '}' would add a hash tag, and the glob
// metacharacters would widen any pattern built from the key.
// Both NormalizeOwned and DeploymentSuffix validate, so removing Validate from
// only one of them leaves these green; removing it from both turns every case
// red. The test pins Validate's character set, not either layer on its own.
func TestLeaderLockKeyRejectsKeyUnsafeNamespaces(t *testing.T) {
	for _, bad := range []namespace.Namespace{
		"a:b", "a{b", "a}b", "a*b", "a?b", "a[b", "a]b", `a\b`,
	} {
		t.Run(string(bad), func(t *testing.T) {
			if got, err := LeaderLockKey([]namespace.Namespace{bad}); err == nil {
				t.Fatalf("LeaderLockKey({%q}) = %q, want a validation error", bad, got)
			}
		})
	}
}

// The helper must return the key the backend campaigns on. New normalises the
// owned set (drops empty entries, removes duplicates) before deriving the key,
// so a caller handing LeaderLockKey the same raw set has to land on the same
// key rather than on an ambiguity error the backend would never raise.
// Mutation: dropping the NormalizeOwned call from LeaderLockKey turns the first
// three cases red; "duplicate default" stays green on purpose, because
// DeploymentSuffix already maps any default-containing set to the legacy key.
// Agreement with the key New really campaigns on is pinned separately by
// TestLeaderLockKeyMatchesTheKeyTheBackendHolds.
func TestLeaderLockKeyNormalisesLikeTheBackend(t *testing.T) {
	tests := []struct {
		name  string
		owned []namespace.Namespace
		want  string
	}{
		{"duplicate dedicated namespace", []namespace.Namespace{"x", "x"}, LegacyLeaderLockKey + ":ns:x"},
		{"empty entry beside a dedicated namespace", []namespace.Namespace{"", "x"}, LegacyLeaderLockKey + ":ns:x"},
		{"only empty entries are unscoped", []namespace.Namespace{"", ""}, LegacyLeaderLockKey},
		{"duplicate default stays legacy", []namespace.Namespace{namespace.Default, namespace.Default}, LegacyLeaderLockKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LeaderLockKey(tc.owned)
			if err != nil {
				t.Fatalf("LeaderLockKey(%v) = %v, want nil", tc.owned, err)
			}
			if got != tc.want {
				t.Fatalf("LeaderLockKey(%v) = %q, want %q", tc.owned, got, tc.want)
			}
		})
	}
}
