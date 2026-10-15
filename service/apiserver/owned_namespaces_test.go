package apiserver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/namespace"
)

// Mutation: dropping OwnedNamespaces from the apiserver→control copy leaves the
// control plane's entry namespaces on {default} although the embedder declared a
// dedicated deployment; the in-memory path has no backend to inherit from, so
// this hop is the only place the set can arrive.
func TestBuildControlPlaneThreadsOwnedNamespaces(t *testing.T) {
	cp, err := buildControlPlane(Config{
		Concurrency:     1,
		OwnedNamespaces: []namespace.Namespace{"local-jack"},
	})
	if err != nil {
		t.Fatalf("buildControlPlane: %v", err)
	}
	got := cp.EntryActivationReconciler().Namespaces()
	if len(got) != 1 || got[0] != "local-jack" {
		t.Fatalf("reconciler namespaces = %v, want [local-jack]: OwnedNamespaces was lost in the apiserver→control config copy", got)
	}
}

// An ambiguous set must fail construction rather than silently share one
// deployment identity.
func TestBuildControlPlaneRejectsAmbiguousOwnedNamespaces(t *testing.T) {
	_, err := buildControlPlane(Config{
		Concurrency:     1,
		OwnedNamespaces: []namespace.Namespace{"a", "b"},
	})
	if err == nil || !strings.Contains(err.Error(), "OwnedNamespaces") {
		t.Fatalf("buildControlPlane error = %v, want it to name OwnedNamespaces", err)
	}
}

// The Redis path hands the set to both the backend and the control plane.
// Mutation: omitting distributed.WithOwnedNamespaces leaves the backend
// unscoped, so the control plane's own scope contradicts it and construction
// fails; omitting the leader-key derivation would campaign the shared legacy
// key and make a local process fight the deployed one.
func TestBuildControlPlaneRedisKeysLeaderLockByOwnedNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		owned   []namespace.Namespace
		wantKey string
	}{
		{"unscoped keeps the legacy key", nil, distributed.LegacyLeaderLockKey},
		{"default keeps the legacy key", []namespace.Namespace{"default"}, distributed.LegacyLeaderLockKey},
		{"dedicated namespace gets its own key", []namespace.Namespace{"local-jack"}, distributed.LegacyLeaderLockKey + ":ns:local-jack"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			cp, err := buildControlPlane(Config{
				Concurrency:     1,
				RedisAddr:       mr.Addr(),
				OwnedNamespaces: tc.owned,
			})
			if err != nil {
				t.Fatalf("buildControlPlane: %v", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = cp.Shutdown(ctx)
			})
			b, ok := cp.Backend().(*distributed.Backend)
			if !ok {
				t.Fatalf("Backend() = %T, want *distributed.Backend", cp.Backend())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := b.Campaign(ctx); err != nil {
				t.Fatalf("Campaign: %v", err)
			}
			t.Cleanup(func() { _ = b.Resign(context.Background()) })

			if !mr.Exists(tc.wantKey) {
				t.Fatalf("leader key %q not held after Campaign; keys = %v", tc.wantKey, mr.Keys())
			}
			for _, other := range []string{distributed.LegacyLeaderLockKey, distributed.LegacyLeaderLockKey + ":ns:local-jack"} {
				if other != tc.wantKey && mr.Exists(other) {
					t.Fatalf("unexpected leader key %q held; keys = %v", other, mr.Keys())
				}
			}
		})
	}
}
