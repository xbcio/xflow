package workflowreg

import (
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

// A v1 record deployed before registry v2 has no name index entry until its
// first read imports it. These cases pin the two halves of that window: an Add
// or rename must not install a second live record beside the un-migrated one,
// and a lazy import that reaches an already-held name must keep serving the
// legacy record instead of failing the read.

func TestDistributedWorkflowRegistryAddRefusesUnMigratedLegacyName(t *testing.T) {
	ctx, reg, srv := newAtomicTestRegistry(t)
	ns := namespace.FromContext(ctx)
	legacy := distributedAtomicRecord("legacy-id", "tenant/foo@v1", "v1", "hash:legacy")
	legacy.Name = "foo"
	legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

	candidate := distributedAtomicRecord("new-id", "tenant/foo@v2", "v2", "hash:new")
	candidate.Name = "foo"
	_, err := reg.AddWorkflow(ctx, candidate)
	var conflict *backend.WorkflowNameConflictError
	if !errors.As(err, &conflict) || conflict.Holder != legacy.ID {
		t.Fatalf("AddWorkflow beside un-migrated legacy name = %v, want WorkflowNameConflictError holder %q", err, legacy.ID)
	}
	if !errors.Is(err, backend.ErrWorkflowNameConflict) {
		t.Fatalf("error %v does not unwrap to ErrWorkflowNameConflict", err)
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
	assertDistributedWorkflowNotFoundByID(t, ctx, reg, candidate.ID)
	assertDistributedWorkflowNotFoundByKey(t, ctx, reg, candidate.Key)
	if srv.Exists(workflowNameIndexKey(ns, "foo")) {
		t.Fatal("refused add wrote a name index entry")
	}

	// The refusal must not disturb the legacy record's own reads: it is still
	// served by key and by id, and the lazy import then claims the name index
	// the refusal never wrote.
	byKey, err := reg.GetWorkflowByKey(ctx, legacy.Key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey after refusal: %v", err)
	}
	if byKey.ID != legacy.ID || byKey.RegistryRevision == 0 {
		t.Fatalf("legacy record view = %#v, want the imported record", byKey)
	}
	byID, err := reg.GetWorkflow(ctx, legacy.ID)
	if err != nil {
		t.Fatalf("GetWorkflow after refusal: %v", err)
	}
	if byID.ID != legacy.ID || byID.RegistryRevision != byKey.RegistryRevision {
		t.Fatalf("by-id view = %#v, want the same import as by-key", byID)
	}
}

// A v1 record whose payload never carried a name has no name identity: it is
// exempt from every name check exactly as the import scripts exempt it, so a
// registration under the name its key encodes is allowed and both records stay
// readable.
func TestDistributedWorkflowRegistryAddCoexistsWithNamelessLegacyRecord(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	ns := namespace.FromContext(ctx)
	legacy := distributedAtomicRecord("legacy-id", "tenant/foo@v1", "v1", "hash:legacy")
	legacy.Name = ""
	legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

	candidate := distributedAtomicRecord("new-id", "tenant/foo@v2", "v2", "hash:new")
	candidate.Name = "foo"
	stored, err := reg.AddWorkflow(ctx, candidate)
	if err != nil {
		t.Fatalf("AddWorkflow beside nameless legacy record: %v", err)
	}
	if stored.ID != candidate.ID || stored.RegistryRevision == 0 {
		t.Fatalf("added record = %#v, want revision %q", stored, candidate.ID)
	}
	if holder, err := reg.rdb.Get(ctx, workflowNameIndexKey(ns, "foo")).Result(); err != nil || holder != string(candidate.ID) {
		t.Fatalf("name index = %q (err %v), want %q", holder, err, candidate.ID)
	}

	legacyView, err := reg.GetWorkflow(ctx, legacy.ID)
	if err != nil {
		t.Fatalf("GetWorkflow nameless legacy: %v", err)
	}
	if legacyView.ID != legacy.ID || legacyView.Name != "" || legacyView.RegistryRevision == 0 {
		t.Fatalf("nameless legacy view = %#v, want the imported unnamed record", legacyView)
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
}

// A lazy import that reaches a name held by another live record must not fail
// the read and must not touch any state: the legacy record is served as-is
// (revision zero) and the holder keeps the index.
func TestDistributedWorkflowRegistryLazyImportYieldsToNameHolder(t *testing.T) {
	ctx, reg, srv := newAtomicTestRegistry(t)
	ns := namespace.FromContext(ctx)
	first := distributedAtomicRecord("first-id", "tenant/foo@v1", "v1", "hash:first")
	first.Name = "foo"
	seedLegacyWorkflow(t, ctx, reg, first)
	if _, err := reg.GetWorkflow(ctx, first.ID); err != nil {
		t.Fatalf("import first legacy record: %v", err)
	}

	second := distributedAtomicRecord("second-id", "tenant/foo@v2", "v2", "hash:second")
	second.Name = "foo"
	secondSnapshot := seedLegacyWorkflow(t, ctx, reg, second)

	for name, get := range map[string]func() (backend.WorkflowRecord, error){
		"by id":  func() (backend.WorkflowRecord, error) { return reg.GetWorkflow(ctx, second.ID) },
		"by key": func() (backend.WorkflowRecord, error) { return reg.GetWorkflowByKey(ctx, second.Key) },
	} {
		record, err := get()
		if err != nil {
			t.Fatalf("Get(%s) beside the name holder: %v", name, err)
		}
		if record.ID != second.ID || record.Key != second.Key || record.RegistryRevision != 0 {
			t.Fatalf("Get(%s) = %#v, want the un-imported v1 record", name, record)
		}
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, secondSnapshot)
	if srv.Exists(workflowByKeyKey(ns, second.Key)) || srv.Exists(workflowByIDKey(ns, second.Key, second.ID)) {
		t.Fatal("name-conflicted import installed v2 keys")
	}
	if srv.Exists(workflowLegacyByKeyMarker(ns, second.Key)) || srv.Exists(workflowLegacyByIDMarker(ns, second.ID)) {
		t.Fatal("name-conflicted import claimed the legacy markers")
	}
	if holder, err := reg.rdb.Get(ctx, workflowNameIndexKey(ns, "foo")).Result(); err != nil || holder != string(first.ID) {
		t.Fatalf("name index = %q (err %v), want the original holder %q", holder, err, first.ID)
	}
}

// Add takes its idempotent fast path for an un-imported legacy record (same
// key and hash). The index repair on that path is v2-only: repairing for the
// legacy record would steal the name from the record that imported first and
// owns it.
func TestDistributedWorkflowRegistryAddDoesNotRepairIndexForUnImportedLegacy(t *testing.T) {
	ctx, reg, _ := newAtomicTestRegistry(t)
	ns := namespace.FromContext(ctx)
	first := distributedAtomicRecord("first-id", "tenant/foo@v1", "v1", "hash:first")
	first.Name = "foo"
	seedLegacyWorkflow(t, ctx, reg, first)
	if _, err := reg.GetWorkflow(ctx, first.ID); err != nil {
		t.Fatalf("import first legacy record: %v", err)
	}

	second := distributedAtomicRecord("second-id", "tenant/foo@v2", "v2", "hash:second")
	second.Name = "foo"
	secondSnapshot := seedLegacyWorkflow(t, ctx, reg, second)

	stored, err := reg.AddWorkflow(ctx, second)
	if err != nil {
		t.Fatalf("idempotent add of un-imported legacy record: %v", err)
	}
	if stored.ID != second.ID || stored.RegistryRevision != 0 {
		t.Fatalf("add result = %#v, want the un-imported v1 record", stored)
	}
	if holder, err := reg.rdb.Get(ctx, workflowNameIndexKey(ns, "foo")).Result(); err != nil || holder != string(first.ID) {
		t.Fatalf("name index = %q (err %v), want the original holder %q", holder, err, first.ID)
	}
	assertLegacyWorkflowUnchanged(t, ctx, reg, secondSnapshot)
}

func TestDistributedWorkflowRegistryRenameProbesUnMigratedLegacyName(t *testing.T) {
	t.Run("named legacy record refuses the rename", func(t *testing.T) {
		ctx, reg, srv := newAtomicTestRegistry(t)
		ns := namespace.FromContext(ctx)
		source := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("source-id", "tenant/bar@v1", "v1", "hash:source"))
		legacy := distributedAtomicRecord("legacy-id", "tenant/foo@v1", "v1", "hash:legacy")
		legacy.Name = "foo"
		legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

		replacement := distributedAtomicRecord("dest-id", "tenant/foo@v2", "v2", "hash:replacement")
		replacement.Name = "foo"
		beforeRevision := distributedRegistryRevision(t, ctx, reg)
		_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  "rename-onto-legacy-name",
			Expected:    backend.RevisionOfWorkflow(source),
			Replacement: replacement,
		})
		var conflict *backend.WorkflowNameConflictError
		if !errors.As(err, &conflict) || conflict.Holder != legacy.ID {
			t.Fatalf("rename onto un-migrated legacy name = %v, want WorkflowNameConflictError holder %q", err, legacy.ID)
		}
		if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
			t.Fatalf("refused rename advanced revision from %d to %d", beforeRevision, got)
		}
		if srv.Exists(workflowOperationKey(ns, "rename-onto-legacy-name")) {
			t.Fatal("refused rename wrote an operation ledger entry")
		}
		assertDistributedWorkflowIdentity(t, getDistributedAtomicWorkflow(t, ctx, reg, source.ID), source)
		assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
	})

	t.Run("nameless legacy record leaves the name free", func(t *testing.T) {
		ctx, reg, _ := newAtomicTestRegistry(t)
		source := addDistributedAtomicWorkflow(t, ctx, reg, distributedAtomicRecord("source-id", "tenant/bar@v1", "v1", "hash:source"))
		legacy := distributedAtomicRecord("legacy-id", "tenant/foo@v1", "v1", "hash:legacy")
		legacy.Name = ""
		legacySnapshot := seedLegacyWorkflow(t, ctx, reg, legacy)

		replacement := distributedAtomicRecord("dest-id", "tenant/foo@v2", "v2", "hash:replacement")
		replacement.Name = "foo"
		result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  "rename-beside-nameless-legacy",
			Expected:    backend.RevisionOfWorkflow(source),
			Replacement: replacement,
		})
		if err != nil {
			t.Fatalf("rename beside nameless legacy record: %v", err)
		}
		if result.Status != backend.WorkflowReplaceReplaced || result.Current.ID != replacement.ID {
			t.Fatalf("replace result = %+v, want replaced by %q", result, replacement.ID)
		}
		// The nameless record keeps working: its import stays exempt from the
		// name check the rename just installed.
		legacyView, err := reg.GetWorkflow(ctx, legacy.ID)
		if err != nil {
			t.Fatalf("GetWorkflow nameless legacy after rename: %v", err)
		}
		if legacyView.ID != legacy.ID || legacyView.RegistryRevision == 0 {
			t.Fatalf("nameless legacy view = %#v, want the imported record", legacyView)
		}
		assertLegacyWorkflowUnchanged(t, ctx, reg, legacySnapshot)
	})
}
