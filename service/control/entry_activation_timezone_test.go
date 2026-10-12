package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func cronTriggerGraph(t *testing.T, tz string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "tz-trigger",
		Settings: &types.WorkflowSettings{Timezone: tz},
		Nodes: []types.NodeDef{
			{Name: "tick", Type: "xflow.trigger.cron", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"expression": "* * * * *"}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestEntryActivation_CarriesWorkflowTimezone: settings.timezone reaches the
// stored desired state and the activate directive the runner receives, and a
// timezone change moves the package hash so the activation is re-delivered.
func TestEntryActivation_CarriesWorkflowTimezone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-tz", "v1", cronTriggerGraph(t, "Asia/Shanghai")); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	acts := workflowActivations(t, store, "wf-tz", "v1")
	if len(acts) != 1 {
		t.Fatalf("activation count = %d, want 1", len(acts))
	}
	if acts[0].WorkflowTimezone != "Asia/Shanghai" {
		t.Fatalf("stored WorkflowTimezone = %q, want Asia/Shanghai", acts[0].WorkflowTimezone)
	}

	r := &EntryActivationReconciler{}
	d := r.activateDirectiveFor(ctx, &acts[0], 1)
	if d.WorkflowTimezone != "Asia/Shanghai" {
		t.Fatalf("directive WorkflowTimezone = %q, want Asia/Shanghai", d.WorkflowTimezone)
	}

	units, err := DeriveEntryActivations(cronTriggerGraph(t, ""))
	if err != nil {
		t.Fatalf("DeriveEntryActivations: %v", err)
	}
	if units[0].WorkflowTimezone != "" || units[0].PackageHash == acts[0].PackageHash {
		t.Fatalf("unset timezone: tz=%q hash=%q (shanghai hash %q); want empty tz and a different hash",
			units[0].WorkflowTimezone, units[0].PackageHash, acts[0].PackageHash)
	}
}
