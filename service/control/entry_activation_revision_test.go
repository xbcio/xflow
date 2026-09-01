package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func revisionTestActivation(unit string, revision uint64) engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:        namespace.Default,
		WorkflowID:       "wf-revision",
		WorkflowVersion:  "v1",
		EntryUnitID:      unit,
		NodeType:         "kafka.source",
		PackageHash:      "pkg-current",
		Desired:          true,
		RegistryRevision: revision,
	}
}

func TestMemoryEntryActivationStoreRevisionOrdering(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	current := revisionTestActivation("entry", 2)
	if err := store.Upsert(ctx, current); err != nil {
		t.Fatalf("Upsert revision 2: %v", err)
	}

	for _, revision := range []uint64{1, 0} {
		stale := current
		stale.RegistryRevision = revision
		stale.PackageHash = "pkg-stale"
		stale.Desired = false
		if err := store.Upsert(ctx, stale); err != nil {
			t.Fatalf("Upsert stale revision %d: %v", revision, err)
		}
	}
	got, ok, err := store.Get(ctx, entryActivationKeyOf(current))
	if err != nil || !ok {
		t.Fatalf("Get current activation: ok=%v err=%v", ok, err)
	}
	if got.RegistryRevision != 2 || got.PackageHash != "pkg-current" || !got.Desired {
		t.Fatalf("stale write changed revision 2 record: %+v", got)
	}

	// Equal revisions are allowed so a projection can be retried idempotently.
	equal := current
	equal.PackageHash = "pkg-equal-retry"
	equal.Desired = false
	if err := store.Upsert(ctx, equal); err != nil {
		t.Fatalf("Upsert equal revision: %v", err)
	}
	got, _, _ = store.Get(ctx, entryActivationKeyOf(current))
	if got.RegistryRevision != 2 || got.PackageHash != "pkg-equal-retry" || got.Desired {
		t.Fatalf("equal revision was not applied: %+v", got)
	}
}

func TestMemoryEntryActivationStoreWorkflowWatermark(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	old := revisionTestActivation("old-entry", 1)
	if err := store.Upsert(ctx, old); err != nil {
		t.Fatalf("Upsert revision 1: %v", err)
	}
	if err := store.AdvanceWorkflowRevision(ctx, old.Namespace, old.WorkflowID, 2); err != nil {
		t.Fatalf("AdvanceWorkflowRevision: %v", err)
	}

	// No revision-2 Upsert follows. Advancing the workflow fence alone must make
	// the old desired record non-authoritative while preserving it for cleanup.
	got, ok, err := store.Get(ctx, entryActivationKeyOf(old))
	if err != nil || !ok {
		t.Fatalf("Get old activation: ok=%v err=%v", ok, err)
	}
	if got.Desired || got.RegistryRevision != 1 {
		t.Fatalf("old activation remained desired after watermark advance: %+v", got)
	}
	listed, err := store.List(ctx, old.Namespace)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0].Desired || listed[0].RegistryRevision != 1 {
		t.Fatalf("List exposed stale desired activation: %+v", listed)
	}

	for _, revision := range []uint64{1, 0} {
		stale := revisionTestActivation("absent-stale", revision)
		if err := store.Upsert(ctx, stale); err != nil {
			t.Fatalf("Upsert absent stale revision %d: %v", revision, err)
		}
		if _, exists, err := store.Get(ctx, entryActivationKeyOf(stale)); err != nil || exists {
			t.Fatalf("stale revision %d created absent key: exists=%v err=%v", revision, exists, err)
		}
	}
}

type activationOperationStore struct {
	operations []string
}

func (s *activationOperationStore) AdvanceWorkflowRevision(context.Context, namespace.Namespace, types.WorkflowID, uint64) error {
	s.operations = append(s.operations, "advance")
	return nil
}

func (s *activationOperationStore) Upsert(context.Context, engine.EntryActivation) error {
	s.operations = append(s.operations, "upsert")
	return nil
}

func (s *activationOperationStore) Get(context.Context, engine.EntryActivationKey) (engine.EntryActivation, bool, error) {
	s.operations = append(s.operations, "get")
	return engine.EntryActivation{}, false, nil
}

func (s *activationOperationStore) List(context.Context, namespace.Namespace) ([]engine.EntryActivation, error) {
	s.operations = append(s.operations, "list")
	return nil, nil
}

func (*activationOperationStore) Assign(context.Context, engine.EntryActivationKey, string, string, uint64, time.Time) (bool, error) {
	return false, nil
}

func (*activationOperationStore) Renew(context.Context, engine.EntryActivationKey, uint64, time.Time) (bool, error) {
	return false, nil
}

func (*activationOperationStore) Fence(context.Context, engine.EntryActivationKey, uint64) error {
	return nil
}

func TestEntryActivationManagerAdvancesRevisionBeforeStoreAccess(t *testing.T) {
	ctx := context.Background()
	g := groupTriggerGraph(t)

	t.Run("add or update", func(t *testing.T) {
		store := &activationOperationStore{}
		manager := NewEntryActivationManager(store)
		if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-order", "v1", 7, g); err != nil {
			t.Fatalf("AddOrUpdateWorkflowRevision: %v", err)
		}
		if len(store.operations) == 0 || store.operations[0] != "advance" {
			t.Fatalf("first store operation = %v, want advance", store.operations)
		}
	})

	t.Run("remove", func(t *testing.T) {
		store := &activationOperationStore{}
		manager := NewEntryActivationManager(store)
		if err := manager.RemoveWorkflowRevision(ctx, namespace.Default, "wf-order", "v1", 8, g); err != nil {
			t.Fatalf("RemoveWorkflowRevision: %v", err)
		}
		if len(store.operations) == 0 || store.operations[0] != "advance" {
			t.Fatalf("first store operation = %v, want advance", store.operations)
		}
	})
}

func TestEntryActivationManagerRevisionFencesVersionChanges(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	manager := NewEntryActivationManager(store)
	g := groupTriggerGraph(t)

	if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-version", "v1", 1, g); err != nil {
		t.Fatalf("Add v1 revision 1: %v", err)
	}
	if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-version", "v2", 2, g); err != nil {
		t.Fatalf("Add v2 revision 2: %v", err)
	}
	if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-version", "v1", 1, g); err != nil {
		t.Fatalf("stale Add v1 revision 1: %v", err)
	}
	if err := manager.RemoveWorkflowRevision(ctx, namespace.Default, "wf-version", "v2", 1, g); err != nil {
		t.Fatalf("stale Remove v2 revision 1: %v", err)
	}

	v1Key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-version",
		WorkflowVersion: "v1",
		EntryUnitID:     "grp1",
	}
	v1, ok, err := store.Get(ctx, v1Key)
	if err != nil || !ok {
		t.Fatalf("Get v1: ok=%v err=%v", ok, err)
	}
	if v1.Desired || v1.RegistryRevision != 2 {
		t.Fatalf("stale projection revived v1: %+v", v1)
	}

	v2Key := v1Key
	v2Key.WorkflowVersion = "v2"
	v2, ok, err := store.Get(ctx, v2Key)
	if err != nil || !ok {
		t.Fatalf("Get v2: ok=%v err=%v", ok, err)
	}
	if !v2.Desired || v2.RegistryRevision != 2 {
		t.Fatalf("stale removal cleared v2: %+v", v2)
	}

	// A stale version that had no previous key must not be created after the
	// newer projection completed.
	if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-absent-version", "v2", 2, g); err != nil {
		t.Fatalf("Add absent-version v2 revision 2: %v", err)
	}
	if err := manager.AddOrUpdateWorkflowRevision(ctx, namespace.Default, "wf-absent-version", "v1", 1, g); err != nil {
		t.Fatalf("stale Add absent-version v1 revision 1: %v", err)
	}
	absentKey := v1Key
	absentKey.WorkflowID = "wf-absent-version"
	if _, exists, err := store.Get(ctx, absentKey); err != nil || exists {
		t.Fatalf("stale version created absent activation: exists=%v err=%v", exists, err)
	}
}
