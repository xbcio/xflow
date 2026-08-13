package control

import (
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// A trigger node's parameters never pass through execution/params.go's boundary
// evaluation layer: they are not executed as a task at all. The control plane
// copies them onto the EntryActivation, the reconciler ships them in the
// directive, and the runner hands them straight to handler.Activate. So a
// template in a trigger parameter reaches the Kafka consumer as the literal
// string "${{ $config.topic }}" and it subscribes to a topic by that name --
// a silent wrong answer, not a failure.
//
// These probes pin the evaluated outcome for both derivation paths.

// triggerParamGraph compiles a workflow whose single remote-hosted Kafka
// trigger reads its topic and one nested value from workflow-level $config /
// $vars.
func triggerParamGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "trigger-params",
		Context: &types.WorkflowContext{
			Config: map[string]any{"topic": "sas-traffic", "workers": 4},
			Vars:   map[string]any{"env": "prod"},
		},
		Nodes: []types.NodeDef{
			{
				Name:    "kafka",
				Type:    "xflow.trigger.kafka",
				Version: 1,
				Kind:    types.NodeKindTrigger,
				Parameters: map[string]any{
					"topic":          "${{ $config.topic }}",
					"group_id":       "xflow-{{ $vars.env }}",
					"brokers":        []any{"broker-1:9092"},
					"consumer_count": "${{ $config.workers }}",
				},
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"zone": "a"}},
			},
			{Name: "sink", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"kafka": {"main": types.PortConnections{Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestDeriveEntryActivations_EvaluatesTriggerParams pins that a standalone
// trigger's activation params reach the store already evaluated.
func TestDeriveEntryActivations_EvaluatesTriggerParams(t *testing.T) {
	units, err := DeriveEntryActivations(triggerParamGraph(t))
	if err != nil {
		t.Fatalf("DeriveEntryActivations: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("expected 1 entry unit, got %d", len(units))
	}
	params := units[0].Params

	if got := params["topic"]; got != "sas-traffic" {
		t.Errorf("topic = %#v, want %q -- the consumer subscribes to whatever string this is", got, "sas-traffic")
	}
	if got := params["group_id"]; got != "xflow-prod" {
		t.Errorf("group_id = %#v, want %q", got, "xflow-prod")
	}
	// ${{ }} preserves the result type: a number stays a number, so the
	// handler's cast to int does not have to parse a template back out.
	if got := params["consumer_count"]; got != 4 {
		t.Errorf("consumer_count = %#v (%T), want int 4", got, got)
	}
	// A literal is untouched.
	brokers, ok := params["brokers"].([]any)
	if !ok || len(brokers) != 1 || brokers[0] != "broker-1:9092" {
		t.Errorf("brokers = %#v, want the literal list unchanged", params["brokers"])
	}
}

// TestDeriveEntryActivations_DoesNotMutateTheGraph pins that evaluation writes
// to a copy. The compiled Graph is shared process-wide (workflowreg hands the
// same *Graph to every reader) and is the source the package projection and the
// node-exec lease both read from; evaluating in place would burn the template
// into it after the first derivation, so a later $config change could never
// re-render.
func TestDeriveEntryActivations_DoesNotMutateTheGraph(t *testing.T) {
	g := triggerParamGraph(t)
	if _, err := DeriveEntryActivations(g); err != nil {
		t.Fatalf("DeriveEntryActivations: %v", err)
	}
	idx, ok := g.NodeIndex("kafka")
	if !ok {
		t.Fatal("node kafka missing from graph")
	}
	if got := g.NodeAt(idx).Parameters["topic"]; got != "${{ $config.topic }}" {
		t.Errorf("graph node param topic = %#v, want the source template unchanged", got)
	}
}

// groupTriggerParamGraph is the same shape hosted inside a co-location group,
// which is the form SAS traffic tagging deploys: the trigger is a group member
// and its params travel inside the projected package, not on
// EntryActivation.Params.
func groupTriggerParamGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "grp-trigger-params",
		Context: &types.WorkflowContext{
			Config: map[string]any{"topic": "sas-traffic"},
		},
		Nodes: []types.NodeDef{
			{
				Name:    "kafka",
				Type:    "xflow.trigger.kafka",
				Version: 1,
				Kind:    types.NodeKindTrigger,
				Parameters: map[string]any{
					"topic":   "${{ $config.topic }}",
					"brokers": []any{"broker-1:9092"},
				},
			},
			{Name: "tagger", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Groups: []types.GroupDef{{
			Name:           "grp",
			Members:        []string{"kafka", "tagger"},
			RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired},
		}},
		Connections: types.Connections{
			"kafka": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tagger", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestGroupPackage_EvaluatesEntryTriggerParams pins the group path. The runner's
// activateGroup reads the entry member's Parameters straight out of the package
// (withGroupEntrySeedParams) and hands them to handler.Activate, so the package
// is where the evaluated value has to be.
func TestGroupPackage_EvaluatesEntryTriggerParams(t *testing.T) {
	g := groupTriggerParamGraph(t)
	pkg, _, err := graph.ProjectGroupPackage(g, 0)
	if err != nil {
		t.Fatalf("ProjectGroupPackage: %v", err)
	}
	var entry *types.NodeDef
	for i := range pkg.Def.Nodes {
		if pkg.Def.Nodes[i].Name == pkg.EntryNode {
			entry = &pkg.Def.Nodes[i]
		}
	}
	if entry == nil {
		t.Fatalf("entry node %q not in package", pkg.EntryNode)
	}
	if got := entry.Parameters["topic"]; got != "sas-traffic" {
		t.Errorf("package entry topic = %#v, want %q", got, "sas-traffic")
	}
}
