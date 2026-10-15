package timeout

// SetNamespaces scoping tests. These follow the package's Redis-backed
// convention (realRedisOrSkip) because processTimeouts' contract is about
// what is actually scanned in Redis — but unlike the monitor's delivery
// paths, scoping is observable through which timeout keys receive a scan at
// all, so a skipped Redis still leaves the non-scan behavior testable via
// scopedNamespaces below (pure, no Redis).

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// Mutation note: making scopedNamespaces re-inject namespace.Default (or fall
// through to the registry) must turn this red — an owned monitor scans its
// declared set and nothing else, exactly like rstate's owned sweeps.
func TestMonitorScopedNamespacesOwnedModeNoDefaultInjection(t *testing.T) {
	m := New(nil, nil, nil, nil, time.Minute)
	m.SetNamespaces([]namespace.Namespace{"local-jack"})

	got, err := m.scopedNamespaces(context.Background())
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	if len(got) != 1 || got[0] != "local-jack" {
		t.Fatalf("scopedNamespaces = %v, want exactly [local-jack]", got)
	}
}

// Mutation note: dropping the nil branch must turn this red — a legacy
// monitor keeps reading the registry. Redis-backed because the fallback is
// the registry read itself; with no registry members the invariant pinned is
// the default injection.
func TestMonitorScopedNamespacesNilOwnedReadsRegistry(t *testing.T) {
	rdb := realRedisOrSkip(t)
	ctx := context.Background()
	stamp := time.Now().UnixNano()
	regNS := fmt.Sprintf("registered-ns-%d", stamp)
	if err := rdb.SAdd(ctx, "xflow:namespaces", regNS).Err(); err != nil {
		t.Fatalf("seed registry: %v", err)
	}
	t.Cleanup(func() { _ = rdb.SRem(context.Background(), "xflow:namespaces", regNS).Err() })

	m := New(rdb, deadEngine(), nil, nil, time.Minute)
	got, err := m.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	found := false
	for _, ns := range got {
		if ns == namespace.Namespace(regNS) {
			found = true
		}
	}
	if !found {
		t.Fatalf("scopedNamespaces = %v, want to include registry member %q", got, regNS)
	}
	for _, ns := range got {
		if ns == namespace.Namespace(regNS) && ns == namespace.Default {
			t.Fatalf("registry member collided with default injection: %v", got)
		}
	}
}

// Mutation note: making SetNamespaces a no-op must turn this red — the
// monitor would keep scanning the global registry after being scoped.
func TestMonitorSetNamespacesStoresSet(t *testing.T) {
	m := New(nil, nil, nil, nil, time.Minute)
	if m.ownedNamespaces != nil {
		t.Fatalf("ownedNamespaces = %v before SetNamespaces, want nil", m.ownedNamespaces)
	}
	owned := []namespace.Namespace{"local-jack"}
	m.SetNamespaces(owned)
	if m.ownedNamespaces == nil || m.ownedNamespaces[0] != "local-jack" {
		t.Fatalf("ownedNamespaces = %v, want [local-jack]", m.ownedNamespaces)
	}
	owned[0] = "changed"
	if m.ownedNamespaces[0] != "local-jack" {
		t.Fatalf("monitor aliased the caller's slice: %v", m.ownedNamespaces)
	}
}
