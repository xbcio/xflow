package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
)

func newScopedTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, time.Minute), srv
}

// Mutation note: making scopedNamespaces fall through to the registry (or
// re-inject namespace.Default into the owned result) must turn both owned-mode
// tests red — the whole point of the owned set is to iterate exactly what the
// embedder declared, nothing more.
func TestScopedNamespacesOwnedModeReturnsExactlyTheOwnedSet(t *testing.T) {
	s, _ := newScopedTestStore(t)
	// Seed the registry with namespaces the process does NOT own, so a
	// registry read (or a default injection) would leak them into the result.
	ctx := context.Background()
	if err := s.rdb.SAdd(ctx, namespaceSetKey, "other-a", "other-b").Err(); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	s.ConfigureOwnedNamespaces([]namespace.Namespace{"local-jack"})

	got, err := s.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	want := []namespace.Namespace{"local-jack"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("scopedNamespaces = %v, want exactly %v", got, want)
	}
}

// Mutation note: returning the store's backing slice instead of a copy must
// turn this red only under a mutating caller — the copy is the contract, so
// mutate the result and assert the store is unaffected.
func TestScopedNamespacesOwnedModeReturnsACopy(t *testing.T) {
	s, _ := newScopedTestStore(t)
	s.ConfigureOwnedNamespaces([]namespace.Namespace{"local-jack"})
	ctx := context.Background()

	got, err := s.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	got[0] = "mutated"

	again, err := s.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces again: %v", err)
	}
	if again[0] != "local-jack" {
		t.Fatalf("store scope mutated through returned slice: %v", again)
	}
}

// Mutation note: dropping the nil branch (always returning the owned field)
// must turn this red — legacy processes have no owned set and must keep
// reading the registry.
func TestScopedNamespacesNilOwnedFallsBackToRegistry(t *testing.T) {
	s, srv := newScopedTestStore(t)
	ctx := context.Background()
	if err := s.rdb.SAdd(ctx, namespaceSetKey, "registered-ns").Err(); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	got, err := s.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	// Legacy contract: registry contents plus the always-present default.
	if len(got) != 2 {
		t.Fatalf("scopedNamespaces = %v, want [default registered-ns]", got)
	}
	if got[0] != namespace.Default || got[1] != "registered-ns" {
		t.Fatalf("scopedNamespaces = %v, want sorted [default registered-ns] (miniredis has %s)", got, srv.Addr())
	}
}

// Mutation note: wiring ConfigureOwnedNamespaces to a no-op must turn this red
// — the sweep sites call scopedNamespaces, so the switch itself is only
// provable through the store field they read.
func TestConfigureOwnedNamespacesSetsTheField(t *testing.T) {
	s, _ := newScopedTestStore(t)
	owned := []namespace.Namespace{"local-jack"}
	s.ConfigureOwnedNamespaces(owned)
	if s.ownedNamespaces == nil || s.ownedNamespaces[0] != "local-jack" {
		t.Fatalf("ownedNamespaces = %v, want [local-jack]", s.ownedNamespaces)
	}
	// The store must keep its own copy semantics: normalizing afterwards in
	// the caller must not retroactively change the store.
	owned[0] = "changed"
	if s.ownedNamespaces[0] != "local-jack" {
		t.Fatalf("store aliased the caller's slice: %v", s.ownedNamespaces)
	}
}
