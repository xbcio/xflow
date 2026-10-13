// Package workflowregtest holds the contract every backend.WorkflowRegistry
// implementation that also implements backend.WorkflowVersionCapability must
// satisfy: retired versions become immutable history addressed by workflow id,
// re-publishing a label with a different definition is a conflict, and a
// compare-and-replace that fails leaves no history entry behind.
//
// The cases below are shared by the in-memory registry in
// backend/providers/local and the Redis registry in
// backend/providers/distributed (which runs them against a real Redis when
// XFLOW_TEST_REDIS_ADDR is set). A registry that keeps no history must be
// exercised by its own tests instead; this suite exists so the two
// implementations cannot drift.
package workflowregtest

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Registry is the capability set the version-history contract requires.
type Registry interface {
	backend.WorkflowRegistry
	backend.WorkflowReplaceCapability
	backend.WorkflowVersionCapability
}

// Factory returns a fresh registry for one sub-test and the context that
// carries its namespace. retention is always a positive limit to configure,
// never "use the default": the suite normalizes before calling.
type Factory func(t *testing.T, retention int) (Registry, context.Context)

// RunVersionHistoryContract executes every version-history contract case
// against the given factory. The factory is called once per case, so no state
// can leak across them.
func RunVersionHistoryContract(t *testing.T, newRegistry Factory) {
	t.Helper()
	t.Run("RetiredVersionBecomesHistory", func(t *testing.T) {
		retiredVersionBecomesHistoryCase(t, newRegistry)
	})
	t.Run("SameVersionSameHashIsIdempotent", func(t *testing.T) {
		sameVersionSameHashIsIdempotentCase(t, newRegistry)
	})
	t.Run("SameVersionDifferentHashIsImmutable", func(t *testing.T) {
		sameVersionDifferentHashIsImmutableCase(t, newRegistry)
	})
	t.Run("RetiredLabelIsImmutableOnReplace", func(t *testing.T) {
		retiredLabelIsImmutableOnReplaceCase(t, newRegistry)
	})
	t.Run("FailedReplaceLeavesNoHistory", func(t *testing.T) {
		failedReplaceLeavesNoHistoryCase(t, newRegistry)
	})
	t.Run("HistoryFollowsTheSurvivingID", func(t *testing.T) {
		historyFollowsTheSurvivingIDCase(t, newRegistry)
	})
	t.Run("RollbackReinstallsArchivedVersion", func(t *testing.T) {
		rollbackReinstallsArchivedVersionCase(t, newRegistry)
	})
	t.Run("RetentionEvictsOldestRetired", func(t *testing.T) {
		retentionEvictsOldestRetiredCase(t, newRegistry)
	})
	t.Run("UnknownVersionIsNotFound", func(t *testing.T) {
		unknownVersionIsNotFoundCase(t, newRegistry)
	})
}

// retiredVersionBecomesHistoryCase: installing v2 retires v1 into history,
// where it stays resolvable by its own label while its key stops being live.
func retiredVersionBecomesHistoryCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("checkout"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)
	v2 := nextVersionRecord(t, created, "v2", "")
	current := replaceVersionRecord(t, ctx, reg, created, v2)

	if current.Version != "v2" {
		t.Fatalf("replaced version = %q, want v2", current.Version)
	}
	archived, err := reg.GetWorkflowVersion(ctx, created.ID, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1) error = %v", err)
	}
	if archived.ID != created.ID || archived.Version != "v1" || archived.DefinitionHash != created.DefinitionHash {
		t.Fatalf("archived v1 = id=%q version=%q hash=%q, want id=%q version=v1 hash=%q",
			archived.ID, archived.Version, archived.DefinitionHash, created.ID, created.DefinitionHash)
	}
	if archived.Key != created.Key {
		t.Fatalf("archived v1 key = %q, want %q", archived.Key, created.Key)
	}

	live, err := reg.GetWorkflowVersion(ctx, created.ID, "v2")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v2) error = %v", err)
	}
	if live.Key != current.Key || live.DefinitionHash != current.DefinitionHash {
		t.Fatalf("current version = key %q hash %q, want key %q hash %q", live.Key, live.DefinitionHash, current.Key, current.DefinitionHash)
	}

	assertVersionRows(t, reg, ctx, created.ID, []backend.WorkflowVersionInfo{
		{Version: "v2", DefinitionHash: current.DefinitionHash, RegistryRevision: current.RegistryRevision, Current: true},
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: created.RegistryRevision, Archived: true},
	})

	// History is not a second live record: the retired key must not resolve.
	if _, err := reg.GetWorkflowByKey(ctx, created.Key); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowByKey(retired key) error = %v, want ErrWorkflowNotFound", err)
	}
}

// sameVersionSameHashIsIdempotentCase: re-publishing a live label with the
// definition it was published with is a no-op, not a conflict.
func sameVersionSameHashIsIdempotentCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("ledger"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)

	again, err := reg.AddWorkflow(ctx, v1)
	if err != nil {
		t.Fatalf("AddWorkflow(same version, same hash) error = %v", err)
	}
	if again.ID != created.ID {
		t.Fatalf("idempotent add id = %q, want %q", again.ID, created.ID)
	}
	if again.DefinitionHash != created.DefinitionHash {
		t.Fatalf("idempotent add hash = %q, want %q", again.DefinitionHash, created.DefinitionHash)
	}
	assertVersionRows(t, reg, ctx, created.ID, []backend.WorkflowVersionInfo{
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: created.RegistryRevision, Current: true},
	})
}

// sameVersionDifferentHashIsImmutableCase: re-publishing a live label with a
// different definition is a conflict that leaves the stored record untouched.
func sameVersionDifferentHashIsImmutableCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("billing"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)

	changed := versionRecord(t, created.Name, "v1", "different-definition")
	_, err := reg.AddWorkflow(ctx, changed)
	if !errors.Is(err, backend.ErrWorkflowVersionImmutable) {
		t.Fatalf("AddWorkflow(same version, different hash) error = %v, want ErrWorkflowVersionImmutable", err)
	}
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow(same version, different hash) error = %v, want it to also satisfy ErrWorkflowConflict", err)
	}

	stored, err := reg.GetWorkflow(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflow error = %v", err)
	}
	if stored.DefinitionHash != created.DefinitionHash || stored.RegistryRevision != created.RegistryRevision {
		t.Fatalf("stored record = hash %q revision %d, want hash %q revision %d",
			stored.DefinitionHash, stored.RegistryRevision, created.DefinitionHash, created.RegistryRevision)
	}
	assertVersionRows(t, reg, ctx, created.ID, []backend.WorkflowVersionInfo{
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: created.RegistryRevision, Current: true},
	})
}

// retiredLabelIsImmutableOnReplaceCase: a replace may re-install an archived
// label only with the payload it was retired with.
func retiredLabelIsImmutableOnReplaceCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("search"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)
	v2 := nextVersionRecord(t, created, "v2", "")
	current := replaceVersionRecord(t, ctx, reg, created, v2)

	tampered := nextVersionRecord(t, current, "v1", "tampered")
	_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  uuid.NewString(),
		Expected:    backend.RevisionOfWorkflow(current),
		Replacement: tampered,
	})
	if !errors.Is(err, backend.ErrWorkflowVersionImmutable) {
		t.Fatalf("replace with a tampered archived version error = %v, want ErrWorkflowVersionImmutable", err)
	}

	// The failed replace changed nothing the caller can observe.
	stored, err := reg.GetWorkflow(ctx, current.ID)
	if err != nil {
		t.Fatalf("GetWorkflow error = %v", err)
	}
	if stored.Version != "v2" || stored.DefinitionHash != current.DefinitionHash || stored.RegistryRevision != current.RegistryRevision {
		t.Fatalf("stored record after conflict = version %q hash %q revision %d, want version v2 hash %q revision %d",
			stored.Version, stored.DefinitionHash, stored.RegistryRevision, current.DefinitionHash, current.RegistryRevision)
	}
}

// failedReplaceLeavesNoHistoryCase: a rejected compare-and-replace must not
// retire anything, whether it lost the source CAS or the destination key.
func failedReplaceLeavesNoHistoryCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("inventory"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)
	v2 := nextVersionRecord(t, created, "v2", "")
	current := replaceVersionRecord(t, ctx, reg, created, v2)

	stale := nextVersionRecord(t, current, "v3", "")
	_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID: uuid.NewString(),
		Expected: backend.WorkflowRevision{
			ID:               created.ID,
			Key:              created.Key,
			Version:          created.Version,
			DefinitionHash:   created.DefinitionHash,
			RegistryRevision: created.RegistryRevision,
		},
		Replacement: stale,
	})
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("stale compare-and-replace error = %v, want ErrWorkflowConflict", err)
	}

	occupied := versionRecord(t, current.Name, "v3", "")
	occupied.Key = current.Key
	occupied.ID = current.ID
	conflict := versionRecord(t, current.Name, "v3", "other")
	conflict.ID = current.ID
	conflict.Key = current.Key
	// A destination key held by another live record must be rejected before the
	// version history is touched. The occupying record carries its own name so
	// only the key dimension is under test.
	other := addVersionRecord(t, ctx, reg, versionRecord(t, uniqueName("inventory-other"), "v9", ""))
	conflict.Key = other.Key
	_, err = reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  uuid.NewString(),
		Expected:    backend.RevisionOfWorkflow(occupied),
		Replacement: conflict,
	})
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("occupied destination error = %v, want ErrWorkflowConflict", err)
	}
	if other.ID == "" {
		t.Fatal("second live record has empty id")
	}

	assertVersionRows(t, reg, ctx, current.ID, []backend.WorkflowVersionInfo{
		{Version: "v2", DefinitionHash: current.DefinitionHash, RegistryRevision: current.RegistryRevision, Current: true},
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: created.RegistryRevision, Archived: true},
	})
}

// historyFollowsTheSurvivingIDCase: a replace that installs a new id takes the
// retired versions with it, so history stays reachable through the live record.
func historyFollowsTheSurvivingIDCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("shipment"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)
	v2 := nextVersionRecord(t, created, "v2", "")
	current := replaceVersionRecord(t, ctx, reg, created, v2)

	moved := nextVersionRecord(t, current, "v3", "")
	moved.ID = ""
	result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  uuid.NewString(),
		Expected:    backend.RevisionOfWorkflow(current),
		Replacement: moved,
	})
	if err != nil {
		t.Fatalf("id-changing compare-and-replace error = %v", err)
	}
	if result.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("id-changing compare-and-replace status = %v, want replaced", result.Status)
	}
	if result.Current.ID == current.ID {
		t.Fatalf("id-changing replace kept id %q", current.ID)
	}

	archived, err := reg.GetWorkflowVersion(ctx, result.Current.ID, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1) after id change error = %v", err)
	}
	if archived.DefinitionHash != created.DefinitionHash {
		t.Fatalf("archived v1 hash = %q, want %q", archived.DefinitionHash, created.DefinitionHash)
	}
	assertVersionRows(t, reg, ctx, result.Current.ID, []backend.WorkflowVersionInfo{
		{Version: "v3", DefinitionHash: result.Current.DefinitionHash, RegistryRevision: result.Current.RegistryRevision, Current: true},
		{Version: "v2", DefinitionHash: current.DefinitionHash, RegistryRevision: current.RegistryRevision, Archived: true},
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: created.RegistryRevision, Archived: true},
	})
}

// rollbackReinstallsArchivedVersionCase: re-installing an archived version
// makes it current again and retires the version it displaced.
func rollbackReinstallsArchivedVersionCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("payout"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)
	v2 := nextVersionRecord(t, created, "v2", "")
	second := replaceVersionRecord(t, ctx, reg, created, v2)
	v3 := nextVersionRecord(t, second, "v3", "")
	third := replaceVersionRecord(t, ctx, reg, second, v3)

	archived, err := reg.GetWorkflowVersion(ctx, created.ID, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1) error = %v", err)
	}
	rolledBack := nextVersionRecord(t, third, archived.Version, "")
	rolledBack.DefinitionHash = archived.DefinitionHash
	live := replaceVersionRecord(t, ctx, reg, third, rolledBack)

	if live.DefinitionHash != created.DefinitionHash {
		t.Fatalf("rollback hash = %q, want the archived hash %q", live.DefinitionHash, created.DefinitionHash)
	}
	assertVersionRows(t, reg, ctx, live.ID, []backend.WorkflowVersionInfo{
		{Version: "v1", DefinitionHash: created.DefinitionHash, RegistryRevision: live.RegistryRevision, Current: true},
		{Version: "v3", DefinitionHash: third.DefinitionHash, RegistryRevision: third.RegistryRevision, Archived: true},
		{Version: "v2", DefinitionHash: second.DefinitionHash, RegistryRevision: second.RegistryRevision, Archived: true},
	})

	// The rolled-back-to version is live again, so it is no longer history.
	current, err := reg.GetWorkflowVersion(ctx, live.ID, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1) after rollback error = %v", err)
	}
	if current.RegistryRevision != live.RegistryRevision {
		t.Fatalf("v1 revision after rollback = %d, want the live revision %d", current.RegistryRevision, live.RegistryRevision)
	}
}

// retentionEvictsOldestRetiredCase: retention bounds how far back history
// reaches, and eviction drops the oldest retirements first.
func retentionEvictsOldestRetiredCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 2)
	v1 := versionRecord(t, uniqueName("report"), "v1", "")
	current := addVersionRecord(t, ctx, reg, v1)
	fourth := backend.WorkflowRecord{}
	for _, version := range []string{"v2", "v3", "v4"} {
		next := nextVersionRecord(t, current, version, "")
		fourth = replaceVersionRecord(t, ctx, reg, current, next)
		current = fourth
	}

	if _, err := reg.GetWorkflowVersion(ctx, current.ID, "v1"); !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("GetWorkflowVersion(evicted v1) error = %v, want ErrWorkflowVersionNotFound", err)
	}
	for _, version := range []string{"v2", "v3"} {
		if _, err := reg.GetWorkflowVersion(ctx, current.ID, version); err != nil {
			t.Fatalf("GetWorkflowVersion(%s) error = %v, want the two newest retirements to stay", version, err)
		}
	}
	rows, err := reg.ListWorkflowVersions(ctx, current.ID)
	if err != nil {
		t.Fatalf("ListWorkflowVersions error = %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("version rows = %d, want 3 (current plus the two retained retirements)", len(rows))
	}
}

// unknownVersionIsNotFoundCase: an unknown label and an unknown workflow are
// distinct answers.
func unknownVersionIsNotFoundCase(t *testing.T, newRegistry Factory) {
	reg, ctx := newRegistry(t, 0)
	v1 := versionRecord(t, uniqueName("auth"), "v1", "")
	created := addVersionRecord(t, ctx, reg, v1)

	if _, err := reg.GetWorkflowVersion(ctx, created.ID, "v9"); !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("GetWorkflowVersion(v9) error = %v, want ErrWorkflowVersionNotFound", err)
	}
	if _, err := reg.GetWorkflowVersion(ctx, types.WorkflowID(uuid.NewString()), "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowVersion(unknown workflow) error = %v, want ErrWorkflowNotFound", err)
	}
	if _, err := reg.ListWorkflowVersions(ctx, types.WorkflowID(uuid.NewString())); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ListWorkflowVersions(unknown workflow) error = %v, want ErrWorkflowNotFound", err)
	}
}

// versionRecord builds one version of a workflow named name. marker changes the
// definition, and therefore its runtime hash, without changing the label.
func versionRecord(t *testing.T, name, version, marker string) backend.WorkflowRecord {
	t.Helper()

	def := &types.WorkflowDef{
		Namespace: string(namespace.Default),
		Name:      name,
		Version:   version,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "xflow.function", Parameters: map[string]any{"marker": marker}},
		},
		Connections: types.Connections{
			"start": {
				"main": types.PortConnections{Targets: []types.Connection{{Node: "review", Input: "main"}}},
			},
		},
	}
	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	hash, err := workflowhash.Runtime(def, nil)
	if err != nil {
		t.Fatalf("Runtime() error = %v", err)
	}
	return backend.WorkflowRecord{
		Key:            def.Namespace + "/" + def.Name + "@" + def.Version,
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: hash,
		Definition:     def,
		Graph:          compiled,
	}
}

// nextVersionRecord builds the next version of the same workflow, keeping the
// live id unless the caller clears it.
func nextVersionRecord(t *testing.T, current backend.WorkflowRecord, version, marker string) backend.WorkflowRecord {
	t.Helper()

	rec := versionRecord(t, current.Name, version, marker)
	rec.ID = current.ID
	return rec
}

func addVersionRecord(t *testing.T, ctx context.Context, reg Registry, rec backend.WorkflowRecord) backend.WorkflowRecord {
	t.Helper()

	created, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow(%s) error = %v", rec.Key, err)
	}
	t.Cleanup(func() {
		// A record replaced onto a different id is already gone; the cleanup is
		// best effort and exists so a real backend does not accumulate records.
		_ = reg.RemoveWorkflow(ctx, created.ID)
	})
	return created
}

func replaceVersionRecord(t *testing.T, ctx context.Context, reg Registry, current, replacement backend.WorkflowRecord) backend.WorkflowRecord {
	t.Helper()

	result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  uuid.NewString(),
		Expected:    backend.RevisionOfWorkflow(current),
		Replacement: replacement,
	})
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow(%s) error = %v", replacement.Key, err)
	}
	if result.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("CompareAndReplaceWorkflow(%s) status = %v, want replaced", replacement.Key, result.Status)
	}
	return result.Current
}

func assertVersionRows(t *testing.T, reg Registry, ctx context.Context, id types.WorkflowID, want []backend.WorkflowVersionInfo) {
	t.Helper()

	rows, err := reg.ListWorkflowVersions(ctx, id)
	if err != nil {
		t.Fatalf("ListWorkflowVersions error = %v", err)
	}
	if len(rows) != len(want) {
		t.Fatalf("ListWorkflowVersions = %+v, want %+v", rows, want)
	}
	for i, row := range rows {
		expected := want[i]
		if row.Version != expected.Version || row.Current != expected.Current || row.Archived != expected.Archived {
			t.Fatalf("ListWorkflowVersions[%d] = %+v, want %+v", i, row, expected)
		}
		if row.DefinitionHash != expected.DefinitionHash {
			t.Fatalf("ListWorkflowVersions[%d] hash = %q, want %q", i, row.DefinitionHash, expected.DefinitionHash)
		}
		if row.RegistryRevision != expected.RegistryRevision {
			t.Fatalf("ListWorkflowVersions[%d] revision = %d, want %d", i, row.RegistryRevision, expected.RegistryRevision)
		}
	}
}

func uniqueName(prefix string) string {
	return prefix + "-" + uuid.NewString()
}
