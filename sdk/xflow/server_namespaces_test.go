package xflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// Mutation: dropping OwnedNamespaces from the SDK→apiserver copy leaves an
// embedded host running unscoped although it declared a dedicated deployment.
func TestWithServerNamespacesReachesTheAPIConfig(t *testing.T) {
	sc := &serverConfig{}
	WithServerNamespaces([]namespace.Namespace{"local-jack"})(sc)

	cfg := buildServerAPIConfig(ServerConfig{}, sc)
	if len(cfg.OwnedNamespaces) != 1 || cfg.OwnedNamespaces[0] != "local-jack" {
		t.Fatalf("OwnedNamespaces = %v, want [local-jack]", cfg.OwnedNamespaces)
	}
}

// Unset must stay nil: nil is the legacy-unscoped signal every scoped worker
// and key derivation keys on.
func TestServerNamespacesUnsetStaysNil(t *testing.T) {
	cfg := buildServerAPIConfig(ServerConfig{}, &serverConfig{})
	if cfg.OwnedNamespaces != nil {
		t.Fatalf("OwnedNamespaces = %v, want nil when no option was set", cfg.OwnedNamespaces)
	}
}

func TestWithServerNamespacesCopiesItsArgument(t *testing.T) {
	ns := []namespace.Namespace{"local-a"}
	sc := &serverConfig{}
	WithServerNamespaces(ns)(sc)

	ns[0] = "mutated"
	if sc.ownedNamespaces[0] != "local-a" {
		t.Fatalf("ownedNamespaces[0] = %q after the caller mutated its slice; the option must copy", sc.ownedNamespaces[0])
	}
}

func TestLeaderLockKey(t *testing.T) {
	tests := []struct {
		name    string
		owned   []namespace.Namespace
		want    string
		wantErr bool
	}{
		{"unscoped", nil, distributed.LegacyLeaderLockKey, false},
		{"default", []namespace.Namespace{"default"}, distributed.LegacyLeaderLockKey, false},
		{"default plus another", []namespace.Namespace{"default", "x"}, distributed.LegacyLeaderLockKey, false},
		{"dedicated", []namespace.Namespace{"local-jack"}, distributed.LegacyLeaderLockKey + ":ns:local-jack", false},
		{"ambiguous", []namespace.Namespace{"a", "b"}, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LeaderLockKey(tc.owned)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LeaderLockKey error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("LeaderLockKey = %q, want %q", got, tc.want)
			}
		})
	}
}

// The audit table is shared infrastructure, so the SDK-built reconciler must
// settle only the namespaces the server owns.
// Mutation: omitting Namespaces from newAuditReconciler settles (and probes)
// the other deployment's admission as well.
func TestServerReconcilerSettlesOnlyOwnedNamespaces(t *testing.T) {
	ms := memstore.New()
	for _, rec := range []*store.AuditRecord{
		{Namespace: "default", RequestID: "req-default", ExecutionID: "exec-default"},
		{Namespace: "local-jack", RequestID: "req-local", ExecutionID: "exec-local"},
	} {
		rec.Phase = store.AuditPhaseAdmission
		rec.Operation = "workflow.create"
		rec.Outcome = "admitted"
		rec.Timestamp = time.Now().Add(-time.Hour)
		if err := ms.AppendAudit(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}

	srv, err := NewServer(ServerConfig{Store: ms},
		WithServerInsecureNoRunnerAuth(),
		WithServerNamespaces([]namespace.Namespace{"local-jack"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	if settled := srv.Reconciler().ReconcileOnce(context.Background()); settled != 1 {
		t.Fatalf("ReconcileOnce settled %d admissions, want 1 (the owned namespace only)", settled)
	}
	if n, _, err := ms.CountUnreconciledAdmissionsIn(context.Background(), []namespace.Namespace{"local-jack"}, time.Now()); err != nil || n != 0 {
		t.Fatalf("owned namespace pending = %d (err %v), want 0", n, err)
	}
	if n, _, err := ms.CountUnreconciledAdmissionsIn(context.Background(), []namespace.Namespace{"default"}, time.Now()); err != nil || n != 1 {
		t.Fatalf("foreign namespace pending = %d (err %v), want 1 untouched", n, err)
	}
}

func TestNewServerRejectsInconsistentNamespaceScope(t *testing.T) {
	tests := []struct {
		name string
		opts []ServerOption
		part string
	}{
		{"ambiguous owned set", []ServerOption{
			WithServerNamespaces([]namespace.Namespace{"a", "b"}),
		}, "OwnedNamespaces"},
		{"entry namespace outside the owned set", []ServerOption{
			WithServerNamespaces([]namespace.Namespace{"local-jack"}),
			WithServerEntryNamespaces([]namespace.Namespace{"default"}),
		}, "outside the owned namespaces"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]ServerOption{WithServerInsecureNoRunnerAuth()}, tc.opts...)
			_, err := NewServer(ServerConfig{Store: memstore.New()}, opts...)
			if err == nil || !strings.Contains(err.Error(), tc.part) {
				t.Fatalf("NewServer error = %v, want it to contain %q", err, tc.part)
			}
		})
	}
}
