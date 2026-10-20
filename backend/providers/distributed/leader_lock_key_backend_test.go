package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/namespace"
)

const dedicatedLeaderKey = LegacyLeaderLockKey + ":ns:local-jack"

func newOwnedTestBackend(t *testing.T, addr string, owned []namespace.Namespace) *Backend {
	t.Helper()
	var opts []Option
	if owned != nil {
		opts = append(opts, WithOwnedNamespaces(owned))
	}
	b, err := New(addr, nil, opts...)
	if err != nil {
		t.Fatalf("New(owned=%v) error = %v", owned, err)
	}
	t.Cleanup(func() { _ = b.rdb.Close() })
	return b
}

func campaignWithin(t *testing.T, b *Backend, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return b.Campaign(ctx)
}

// Mutation note: hard-coding the legacy key in New (the pre-change behavior)
// must turn the {local-jack} case red; deriving a suffix for the default or
// unconfigured cases must turn those red — together they pin both halves of
// the rolling-upgrade invariant at the Redis level, not just in the pure
// mapping function.
func TestBackendCampaignsOnKeyDerivedFromOwnedSet(t *testing.T) {
	cases := []struct {
		name    string
		owned   []namespace.Namespace
		wantKey string
		absent  string
	}{
		{"unconfigured", nil, LegacyLeaderLockKey, dedicatedLeaderKey},
		{"default", []namespace.Namespace{namespace.Default}, LegacyLeaderLockKey, dedicatedLeaderKey},
		{"dedicated", []namespace.Namespace{"local-jack"}, dedicatedLeaderKey, LegacyLeaderLockKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()

			b := newOwnedTestBackend(t, mr.Addr(), tc.owned)
			if err := campaignWithin(t, b, time.Second); err != nil {
				t.Fatalf("Campaign() error = %v", err)
			}
			if !b.IsLeader() {
				t.Fatal("IsLeader() = false after Campaign, want true")
			}
			if !mr.Exists(tc.wantKey) {
				t.Fatalf("lease key %q not held in Redis", tc.wantKey)
			}
			if mr.Exists(tc.absent) {
				t.Fatalf("unexpected lease key %q held in Redis", tc.absent)
			}
		})
	}
}

// LeaderLockKey is exported so embedders can derive the key their backend will
// hold; that only works if it agrees with what New really campaigns on, also
// for the raw sets New normalises (duplicates, empty entries). Comparing
// against the lease key in Redis ties the helper to the backend instead of to
// a literal the helper's own author chose.
// Mutation: dropping NormalizeOwned from LeaderLockKey turns {"",""}, {x,x} and
// {"",x} red (the helper errors where the backend does not); making New skip
// its own normalisation turns the same three red (construction fails).
func TestLeaderLockKeyMatchesTheKeyTheBackendHolds(t *testing.T) {
	cases := []struct {
		name  string
		owned []namespace.Namespace
	}{
		{"unconfigured", nil},
		{"only empty entries", []namespace.Namespace{"", ""}},
		{"default", []namespace.Namespace{namespace.Default}},
		{"default and dedicated", []namespace.Namespace{namespace.Default, "local-jack"}},
		{"dedicated", []namespace.Namespace{"local-jack"}},
		{"duplicate dedicated", []namespace.Namespace{"local-jack", "local-jack"}},
		{"empty entry beside dedicated", []namespace.Namespace{"", "local-jack"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := LeaderLockKey(tc.owned)
			if err != nil {
				t.Fatalf("LeaderLockKey(%v) error = %v", tc.owned, err)
			}
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()

			b := newOwnedTestBackend(t, mr.Addr(), tc.owned)
			if err := campaignWithin(t, b, time.Second); err != nil {
				t.Fatalf("Campaign() error = %v", err)
			}
			if !mr.Exists(want) {
				t.Fatalf("backend for %v does not hold LeaderLockKey %q; Redis keys: %v", tc.owned, want, mr.Keys())
			}
			if got := len(mr.Keys()); got != 1 {
				t.Fatalf("backend for %v holds %d keys %v, want exactly the lease key", tc.owned, got, mr.Keys())
			}
		})
	}
}

// Mutation note: collapsing both deployments onto one key (the bug this whole
// change exists to fix: a local dev process stuck in Campaign behind the
// deployed leader) must turn this red.
func TestBackendDefaultAndDedicatedLeadSimultaneously(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	deployed := newOwnedTestBackend(t, mr.Addr(), []namespace.Namespace{namespace.Default})
	local := newOwnedTestBackend(t, mr.Addr(), []namespace.Namespace{"local-jack"})

	if err := campaignWithin(t, deployed, time.Second); err != nil {
		t.Fatalf("deployed.Campaign() error = %v", err)
	}
	if err := campaignWithin(t, local, 500*time.Millisecond); err != nil {
		t.Fatalf("local.Campaign() blocked behind the deployed leader: %v", err)
	}
	if !deployed.IsLeader() || !local.IsLeader() {
		t.Fatalf("IsLeader deployed=%v local=%v, want both true", deployed.IsLeader(), local.IsLeader())
	}
}

// Mutation note: giving default-owned (or unconfigured) backends distinct keys
// would let a v0.0.47-style pod and a v0.0.48 pod both lead during a rolling
// upgrade; this must turn red. Unconfigured-vs-default is the cross-version
// pairing that matters in practice.
func TestBackendSameDeploymentStaysMutuallyExclusive(t *testing.T) {
	cases := []struct {
		name         string
		first, other []namespace.Namespace
	}{
		{"default-vs-default", []namespace.Namespace{namespace.Default}, []namespace.Namespace{namespace.Default}},
		{"unconfigured-vs-default", nil, []namespace.Namespace{namespace.Default}},
		{"dedicated-vs-dedicated", []namespace.Namespace{"local-jack"}, []namespace.Namespace{"local-jack"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()

			first := newOwnedTestBackend(t, mr.Addr(), tc.first)
			other := newOwnedTestBackend(t, mr.Addr(), tc.other)

			if err := campaignWithin(t, first, time.Second); err != nil {
				t.Fatalf("first.Campaign() error = %v", err)
			}
			if err := campaignWithin(t, other, 200*time.Millisecond); err == nil {
				t.Fatal("other.Campaign() succeeded while the same deployment holds the lease")
			}
			if other.IsLeader() {
				t.Fatal("other.IsLeader() = true, want false")
			}
		})
	}
}

// Mutation note: dropping the DeploymentSuffix check or the NormalizeOwned
// error return in New must turn this red — an ambiguous or invalid owned set
// has to fail construction instead of silently picking an identity.
func TestNewRejectsAmbiguousOrInvalidOwnedSet(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	cases := []struct {
		name  string
		owned []namespace.Namespace
	}{
		{"two-dedicated", []namespace.Namespace{"a", "b"}},
		{"invalid-colon", []namespace.Namespace{"a:b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(mr.Addr(), nil, WithOwnedNamespaces(tc.owned))
			if err == nil {
				_ = b.rdb.Close()
				t.Fatalf("New(owned=%v) = nil error, want rejection", tc.owned)
			}
		})
	}
}

// Mutation note: returning the internal slice (or storing the caller's) must
// turn this red — the owned set is the scope of every sweep, so mutating it
// after construction must not move the boundary.
func TestBackendOwnedNamespacesAccessor(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	unscoped := newOwnedTestBackend(t, mr.Addr(), nil)
	if got := unscoped.OwnedNamespaces(); got != nil {
		t.Fatalf("OwnedNamespaces() = %v for an unconfigured backend, want nil (legacy)", got)
	}

	in := []namespace.Namespace{"local-jack"}
	scoped := newOwnedTestBackend(t, mr.Addr(), in)
	in[0] = "changed"
	got := scoped.OwnedNamespaces()
	if len(got) != 1 || got[0] != "local-jack" {
		t.Fatalf("OwnedNamespaces() = %v after caller mutation, want [local-jack]", got)
	}
	got[0] = "changed-again"
	if again := scoped.OwnedNamespaces(); again[0] != "local-jack" {
		t.Fatalf("OwnedNamespaces() exposed internal state: %v", again)
	}
}
