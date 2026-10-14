package control

import (
	"context"
	"reflect"
	"testing"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// EntryNamespaces is the only configuration entry for "a workflow registered
// outside the default namespace is reconciled": lost at this layer, the
// reconciler silently serves {namespace.Default} only, and every non-default
// activation never gets a runner — collection never starts, nothing on the
// registration path says why. NewControlPlane must hand the configured set to
// the reconciler (modulo the construction-time normalization).
func TestEntryNamespacesReachTheReconciler(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:              backendlocal.New(),
		EntryActivationStore: NewMemoryEntryActivationStore(),
		EntryNamespaces:      []namespace.Namespace{"local-dev"},
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	rec := cp.EntryActivationReconciler()
	if rec == nil {
		t.Fatal("reconciler not assembled although EntryActivationStore was provided")
	}
	got := rec.Namespaces()
	if len(got) != 1 || got[0] != "local-dev" {
		t.Fatalf("reconciler namespaces = %v, want [local-dev]: the configured set did not reach the reconciler", got)
	}
}

// Unset must keep the single-namespace behavior byte-for-byte: both consumers
// fall back to {namespace.Default} exactly as before the field existed. A
// regression here would change what every existing deployment reconciles.
func TestEntryNamespacesUnsetKeepTheDefault(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:              backendlocal.New(),
		EntryActivationStore: NewMemoryEntryActivationStore(),
		Supplies:             stubSupplies{},
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	rec := cp.EntryActivationReconciler()
	if rec == nil {
		t.Fatal("reconciler not assembled although EntryActivationStore was provided")
	}
	got := rec.Namespaces()
	if len(got) != 1 || got[0] != namespace.Default {
		t.Fatalf("reconciler namespaces = %v, want [%s]: unset must keep the single-namespace default", got, namespace.Default)
	}
	hinter := cp.httpServer.core.supplyHinter
	if hinter == nil {
		t.Fatal("hinter not assembled although EntryActivationStore and Supplies were provided")
	}
	if hGot := hinter.namespaces; len(hGot) != 1 || hGot[0] != namespace.Default {
		t.Fatalf("hinter namespaces = %v, want [%s]: unset must keep the single-namespace default", hGot, namespace.Default)
	}
}

// The configured list is normalized once at construction: empty entries are
// dropped and duplicates collapsed in first-seen order, then {namespace.Default}
// falls back for an empty result. The empty-entry drop is load-bearing, not
// tidiness — the write side normalizes an empty namespace to default, so a
// surviving "" entry would make the reconciler scan the empty namespace, find
// nothing, and silently reconcile NOTHING at all.
func TestEntryNamespacesNormalizeAtConstruction(t *testing.T) {
	t.Run("all-empty list falls back to the default", func(t *testing.T) {
		cp, err := NewControlPlane(Config{
			Backend:              backendlocal.New(),
			EntryActivationStore: NewMemoryEntryActivationStore(),
			EntryNamespaces:      []namespace.Namespace{""},
		})
		if err != nil {
			t.Fatalf("NewControlPlane: %v", err)
		}
		got := cp.EntryActivationReconciler().Namespaces()
		if len(got) != 1 || got[0] != namespace.Default {
			t.Fatalf("reconciler namespaces = %v, want [%s]: an all-empty list must fall back to default like an empty one", got, namespace.Default)
		}
	})
	t.Run("empty entries drop and duplicates collapse in first-seen order", func(t *testing.T) {
		cp, err := NewControlPlane(Config{
			Backend:              backendlocal.New(),
			EntryActivationStore: NewMemoryEntryActivationStore(),
			EntryNamespaces:      []namespace.Namespace{"local-b", "", "local-a", "local-b"},
		})
		if err != nil {
			t.Fatalf("NewControlPlane: %v", err)
		}
		got := cp.EntryActivationReconciler().Namespaces()
		want := []namespace.Namespace{"local-b", "local-a"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("reconciler namespaces = %v, want %v", got, want)
		}
	})
}

// stubSupplies is the minimal store.Supplies: the hinter's assembly condition
// is "a non-nil source", and this test pins namespace arrival, not storage
// behavior.
type stubSupplies struct{}

func (stubSupplies) GetSupply(context.Context, string, string) (*store.SupplyResource, error) {
	return nil, store.ErrNotFound
}

func (stubSupplies) PutSupply(_ context.Context, rec *store.SupplyResource, _ *uint64) (*store.SupplyResource, error) {
	return rec, nil
}

// The heartbeat supply hinter is the second consumer of the same normalized
// namespace capture (one config field, two readers). Pin two things: the
// normalization reaches the hinter too (a pre-normalization capture would
// hand it the raw list, empty entries and all), and the hinter sees the very
// set the reconciler scans — the equality is asserted against the
// reconciler's own accessor, so the two cannot silently drift apart. Hints
// computed over the wrong namespace set go missing for exactly the workflows
// the reconciler was just configured to activate, and a missing hint is
// silent — the runner keeps running on last-good content until the TTL poll.
func TestEntryNamespacesReachTheSupplyHinter(t *testing.T) {
	cp, err := NewControlPlane(Config{
		Backend:              backendlocal.New(),
		EntryActivationStore: NewMemoryEntryActivationStore(),
		Supplies:             stubSupplies{},
		EntryNamespaces:      []namespace.Namespace{"local-dev", "", "local-dev"},
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	hinter := cp.httpServer.core.supplyHinter
	if hinter == nil {
		t.Fatal("hinter not assembled although EntryActivationStore and Supplies were provided")
	}
	rec := cp.EntryActivationReconciler()
	if rec == nil {
		t.Fatal("reconciler not assembled although EntryActivationStore was provided")
	}
	got := hinter.namespaces
	if len(got) != 1 || got[0] != "local-dev" {
		t.Fatalf("hinter namespaces = %v, want [local-dev]: the normalized configured set did not reach the supply hinter", got)
	}
	if recGot := rec.Namespaces(); !reflect.DeepEqual(got, recGot) {
		t.Fatalf("hinter namespaces = %v but reconciler namespaces = %v: the two consumers must enumerate the same set", got, recGot)
	}
}
