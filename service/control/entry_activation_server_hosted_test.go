package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// webhookTriggerGraph compiles a workflow whose entry unit is a
// xflow.trigger.webhook node carrying a RunnerSelector, i.e. a unit that WOULD
// be remote-hosted were it any other trigger kind.
func webhookTriggerGraph(t *testing.T, replicas uint32) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "hook",
		Nodes: []types.NodeDef{
			{
				Name: "in", Type: "xflow.trigger.webhook", Version: 1, Kind: types.NodeKindTrigger,
				ActivationReplicas: replicas,
				Parameters:         map[string]any{"method": "POST", "path": "/in"},
			},
			{Name: "work", Type: "xflow.function", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"in": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestDeriveEntryActivations_MarksWebhookEntryUnitServerHosted asserts the
// derivation flags the one trigger kind the control plane serves itself. The
// flag is what keeps the unit off the runner-directive path everywhere it is
// consumed, so it has to be set at the source rather than inferred per caller.
func TestDeriveEntryActivations_MarksWebhookEntryUnitServerHosted(t *testing.T) {
	units, err := DeriveEntryActivations(webhookTriggerGraph(t, 0))
	if err != nil {
		t.Fatalf("DeriveEntryActivations: %v", err)
	}
	var found bool
	for _, eu := range units {
		if eu.EntryUnitID != "in" {
			continue
		}
		found = true
		if !eu.ServerHosted {
			t.Fatal("the webhook entry unit was not marked ServerHosted")
		}
		if eu.NodeType != webhookTriggerType {
			t.Fatalf("node type = %q, want %q", eu.NodeType, webhookTriggerType)
		}
	}
	if !found {
		t.Fatalf("the webhook node was not derived as an entry unit: %+v", units)
	}

	// The rule is about the webhook type, not about triggers in general: a
	// timer entry unit in the same shape must stay remote-hosted.
	units, err = DeriveEntryActivations(groupTriggerGraph(t))
	if err != nil {
		t.Fatalf("DeriveEntryActivations(group): %v", err)
	}
	for _, eu := range units {
		if eu.ServerHosted {
			t.Fatalf("entry unit %q of another trigger kind was marked ServerHosted", eu.EntryUnitID)
		}
	}
}

// TestAddOrUpdateWorkflow_WebhookUnitProducesNoActivation asserts no runner
// activation is written for a server-hosted unit: with no record there is no
// directive to deliver, which is the property the whole routing change rests on.
func TestAddOrUpdateWorkflow_WebhookUnitProducesNoActivation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-hook", "v1", webhookTriggerGraph(t, 3)); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	acts, err := store.List(ctx, namespace.Default)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(acts) != 0 {
		t.Fatalf("a server-hosted entry unit produced %d activation(s): %+v", len(acts), acts)
	}
}

// TestAddOrUpdateWorkflow_RetiresAnExistingWebhookActivation asserts the
// transition, which is the case a real deployment hits on upgrade: a record
// created by a release that still remote-hosted webhooks must be swept
// non-desired so the reconciler deactivates the runner that holds it. The owner
// fields are deliberately left intact — clearing them here would orphan the
// runner's subscription, so the reconciler has to be the one to fence.
func TestAddOrUpdateWorkflow_RetiresAnExistingWebhookActivation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEntryActivationStore()
	mgr := NewEntryActivationManager(store)

	stale := engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-hook",
		WorkflowVersion: "v1",
		EntryUnitID:     "in",
		NodeType:        webhookTriggerType,
		PackageHash:     "pkg-old",
		Desired:         true,
		RunnerID:        "runner-1",
		SessionID:       "sess-1",
		Generation:      7,
	}
	if err := store.Upsert(ctx, stale); err != nil {
		t.Fatalf("Upsert stale: %v", err)
	}

	if err := mgr.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-hook", "v1", webhookTriggerGraph(t, 0)); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	got, ok, err := store.Get(ctx, entryActivationKeyOf(stale))
	if err != nil || !ok {
		t.Fatalf("Get swept activation: ok=%v err=%v", ok, err)
	}
	if got.Desired {
		t.Fatal("the webhook activation is still desired; no Deactivate would ever be delivered")
	}
	if got.RunnerID != "runner-1" || got.SessionID != "sess-1" || got.Generation != 7 {
		t.Fatalf("the sweep cleared the owner fields the reconciler needs to fence: %+v", got)
	}
}
