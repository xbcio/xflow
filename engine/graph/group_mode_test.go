package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// transientGroupDef returns mkGroupDef's workflow with the group marked
// transient, so each test below only has to state its one extra shape.
func transientGroupDef(members []string) *types.WorkflowDef {
	def := mkGroupDef(members)
	def.Groups[0].Mode = types.GroupModeTransient
	return def
}

// TestGroupModeRejectsUnknownValues pins the first compile-time rule for
// groups[].mode.
//
// Mode was a free-form string the compiler copied into GroupMeta and nothing
// read, so `mode: "Transient"` (or any other typo) compiled clean and ran with
// durable retention -- the exact silent downgrade the mode exists to prevent.
func TestGroupModeRejectsUnknownValues(t *testing.T) {
	for _, mode := range []string{"Transient", "TRANSIENT", "fire_and_forget", "durable", "ephemeral"} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].Mode = mode
		_, err := Compile(def)
		if err == nil {
			t.Errorf("mode=%q compiled clean; unknown modes must be rejected instead of "+
				"silently running as durable", mode)
			continue
		}
		msg := err.Error()
		for _, want := range []string{"edge", "mode", mode} {
			if !strings.Contains(msg, want) {
				t.Errorf("mode=%q: error %q does not mention %q", mode, msg, want)
			}
		}
	}
}

// TestGroupModeAcceptsDurableAndTransient is the other half: rejecting unknown
// values must not reject the two legal ones.
func TestGroupModeAcceptsDurableAndTransient(t *testing.T) {
	for _, mode := range []string{types.GroupModeDurable, types.GroupModeTransient} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].Mode = mode
		g, err := Compile(def)
		if err != nil {
			t.Fatalf("mode=%q must compile: %v", mode, err)
		}
		if got := g.Groups()[0].Mode; got != mode {
			t.Errorf("GroupMeta.Mode = %q, want %q", got, mode)
		}
	}
}

// TestTransientGroupRejectsSuspendingMember pins the second rule: a transient
// group may not contain a suspending node.
//
// A suspended member parks a sub-execution the group's own lease cannot resume
// (which is why the runtime disables suspend inside every group), and a
// transient group's recovery story is "replay from the entry" instead. A
// compile error is the cheaper place to learn that than a group that hangs.
func TestTransientGroupRejectsSuspendingMember(t *testing.T) {
	for _, nodeType := range []string{"xflow.wait", "xflow.approval"} {
		def := transientGroupDef([]string{"ingest", "analyze"})
		def.Nodes = append(def.Nodes, types.NodeDef{Name: "gate", Type: nodeType})
		def.Connections["analyze"] = map[string]types.PortConnections{"main": {Targets: []types.Connection{{Node: "gate", Input: "main"}}}}
		def.Groups[0].Members = []string{"ingest", "analyze", "gate"}

		_, err := Compile(def)
		if err == nil {
			t.Fatalf("transient group containing %s compiled clean", nodeType)
		}
		msg := err.Error()
		for _, want := range []string{"edge", "gate", nodeType} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s: error %q does not mention %q", nodeType, msg, want)
			}
		}
	}
}

// TestDurableGroupStillAcceptsSuspendingMember pins that the rejection is
// scoped to transient groups: a durable group with a suspending member is
// rejected at RUNTIME (suspend is disabled inside a group), and compile-time
// rejection would break every already-deployed durable workflow that has one.
func TestDurableGroupStillAcceptsSuspendingMember(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Nodes = append(def.Nodes, types.NodeDef{Name: "gate", Type: "xflow.wait"})
	def.Connections["analyze"] = map[string]types.PortConnections{"main": {Targets: []types.Connection{{Node: "gate", Input: "main"}}}}
	def.Groups[0].Members = []string{"ingest", "analyze", "gate"}

	if _, err := Compile(def); err != nil {
		t.Fatalf("durable group with a suspending member must still compile: %v", err)
	}
}

// TestTransientGroupRejectsSuspendingNodeInNestedBody covers the walk the rule
// needs to survive: a member whose own body contains a suspending node is
// still a group member that can park.
func TestTransientGroupRejectsSuspendingNodeInNestedBody(t *testing.T) {
	def := transientGroupDef([]string{"ingest", "mapper"})
	def.Nodes = []types.NodeDef{
		{Name: "ingest", Type: "kafka", Kind: types.NodeKindTrigger},
		{Name: "mapper", Type: "xflow.map", Kind: types.NodeKindAction, Parameters: map[string]any{
			"items": "$input.items",
			"body": map[string]any{
				"type": "xflow.subgraph",
				"parameters": map[string]any{
					"nodes": []any{
						map[string]any{"name": "worker", "type": "test.echo"},
						map[string]any{"name": "waiter", "type": "xflow.wait"},
					},
					// worker must dominate waiter, or the body fails its own
					// entry validation before the suspend rule is reached.
					"connections": map[string]any{
						"worker": map[string]any{
							"main": map[string]any{"targets": []any{map[string]any{"node": "waiter", "input": "main"}}},
						},
					},
				},
			},
		}},
	}
	def.Connections = types.Connections{
		"ingest": {"main": types.PortConnections{Targets: []types.Connection{{Node: "mapper", Input: "main"}}}},
	}

	_, err := Compile(def)
	if err == nil {
		t.Fatal("transient group whose member body contains xflow.wait compiled clean")
	}
	msg := err.Error()
	for _, want := range []string{"edge", "mapper", "waiter", "xflow.wait"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestTransientGroupRejectsReplicatedTriggerEntry pins the conservative rule
// for activation replicas: sibling replicas admit independent executions from
// the same consumer group, so a single transient retention window would not
// cover them.
func TestTransientGroupRejectsReplicatedTriggerEntry(t *testing.T) {
	def := transientGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].ActivationReplicas = 3

	_, err := Compile(def)
	if err == nil {
		t.Fatal("transient group with activation_replicas=3 compiled clean")
	}
	msg := err.Error()
	for _, want := range []string{"edge", "activation_replicas"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestTransientGroupAllowsSingleActivationTriggerEntry is the boundary the
// rule must not overreach: replicas 0 and 1 are the legacy single-activation
// shapes and stay legal.
func TestTransientGroupAllowsSingleActivationTriggerEntry(t *testing.T) {
	for _, replicas := range []uint32{0, 1} {
		def := transientGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].ActivationReplicas = replicas
		if _, err := Compile(def); err != nil {
			t.Errorf("activation_replicas=%d must compile: %v", replicas, err)
		}
	}
}

// TestTransientGroupUnderTransientWorkflowCompiles pins the documented
// non-interaction: a workflow-level transient option alongside a transient
// group is redundant, not contradictory, so it must not fail the compile.
func TestTransientGroupUnderTransientWorkflowCompiles(t *testing.T) {
	def := transientGroupDef([]string{"ingest", "analyze"})
	def.Options = &types.WorkflowOptions{Transient: true}

	g, err := Compile(def)
	if err != nil {
		t.Fatalf("transient workflow with a transient group must compile: %v", err)
	}
	if !g.Transient() {
		t.Fatal("compiled graph lost the workflow-level transient flag")
	}
	if g.Groups()[0].Mode != types.GroupModeTransient {
		t.Fatalf("compiled GroupMeta.Mode = %q, want %q", g.Groups()[0].Mode, types.GroupModeTransient)
	}
}
