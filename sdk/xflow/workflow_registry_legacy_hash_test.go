package xflow

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// seedLegacyRecord injects a record with a pre-F0.3 bare "sha256:" hash format
// into the engine's registry, simulating a workflow that was registered before
// the runtime/audit hash split. Returns the workflow ID and key.
func seedLegacyRecord(t *testing.T, eng *Engine, def *types.WorkflowDef, legacyHash string) (types.WorkflowID, string) {
	t.Helper()

	key := workflowKey(def)
	rec := backend.WorkflowRecord{
		Key:            key,
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: legacyHash,
		Definition:     def,
	}
	created, err := eng.workflowRegistry.AddWorkflow(context.Background(), rec)
	if err != nil {
		t.Fatalf("seed legacy record: %v", err)
	}
	return created.ID, key
}

// TestAddWorkflow_UpgradesLegacyHashOnSemanticMatch exercises the F0-A4
// backward-compatibility path: a workflow registered with a pre-F0.3 bare
// "sha256:" hash can be re-registered through Engine.AddWorkflow after the
// runtime-hash split. The Engine reconciles by recomputing the runtime hash
// from the stored Definition, and when it matches the freshly-computed hash,
// atomically upgrades the stored hash to "runtime-sha256:v1:".
func TestAddWorkflow_UpgradesLegacyHashOnSemanticMatch(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	wf := Workflow("legacy-upgrade")
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	expectedHash, err := runtimeHash(def)
	if err != nil {
		t.Fatalf("runtimeHash: %v", err)
	}

	// Seed the registry with a legacy-format hash, as if the workflow had
	// been registered before commit 3ef36d9.
	legacyID, _ := seedLegacyRecord(t, eng, def, "sha256:deadbeef")

	// Re-register the same workflow through Engine.AddWorkflow. Before F0-A4
	// this would surface ErrWorkflowConflict because "sha256:deadbeef" never
	// equals the new runtime-sha256:v1: hash. With reconciliation, the
	// engine recomputes from the stored Definition, finds a semantic match,
	// and upgrades the record.
	gotID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow after legacy seed: %v", err)
	}
	if gotID != legacyID {
		t.Fatalf("AddWorkflow returned id %q, want legacy id %q (idempotent)", gotID, legacyID)
	}

	// The stored record must now carry the new-format runtime hash.
	upgraded, err := eng.workflowRegistry.GetWorkflow(context.Background(), legacyID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if upgraded.DefinitionHash != expectedHash {
		t.Fatalf("DefinitionHash = %q, want upgraded %q", upgraded.DefinitionHash, expectedHash)
	}

	// A subsequent re-registration is now a no-op idempotent success
	// (no reconcile path taken) because the stored hash is in current format.
	if _, err := eng.AddWorkflow(context.Background(), wf); err != nil {
		t.Fatalf("AddWorkflow after upgrade: %v", err)
	}
}

// TestAddWorkflow_LegacyHashRealConflictSurfacesError ensures that when the
// stored legacy hash corresponds to a DIFFERENT semantic definition, the
// reconciliation path does NOT silently upgrade — it surfaces
// ErrWorkflowConflict to the caller.
func TestAddWorkflow_LegacyHashRealConflictSurfacesError(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	wfStored := Workflow("legacy-conflict")
	wfStored.Node("start", node.Start())

	storedDef, err := wfStored.build()
	if err != nil {
		t.Fatalf("build stored: %v", err)
	}
	seedLegacyRecord(t, eng, storedDef, "sha256:deadbeef")

	// Build a different workflow under the SAME key (namespace/name/version)
	// but with a different node type. The runtime hash will differ, so
	// reconcile must surface ErrWorkflowConflict.
	wfConflict := Workflow("legacy-conflict")
	wfConflict.Node("start", node.Merge(node.MergeWaitAll))

	if _, err := eng.AddWorkflow(context.Background(), wfConflict); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow real conflict = %v, want ErrWorkflowConflict", err)
	}
}
