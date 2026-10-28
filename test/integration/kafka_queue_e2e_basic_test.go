//go:build integration

package integration

import (
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// e2eBasicDef is start -> calc -> branch -> (big | small) -> end_*. It uses only
// builtin nodes that need no external system: xflow.function with an inline
// expression and xflow.if. n=7 doubles to 14 and takes the true branch, n=2
// doubles to 4 and takes the false branch.
func e2eBasicDef() *types.WorkflowDef {
	fn := func(name, code string) types.NodeDef {
		return types.NodeDef{Name: name, Type: "xflow.function", Parameters: map[string]any{"code": code}}
	}
	link := func(to string) types.PortConnections {
		return types.PortConnections{Targets: []types.Connection{{Node: to, Input: "main"}}}
	}
	return &types.WorkflowDef{
		Name: "kafka-e2e-basic",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			fn("calc", `{"n": $input.n, "doubled": $input.n * 2}`),
			{Name: "branch", Type: "xflow.if", Parameters: map[string]any{"condition": "$input.doubled > 10"}},
			fn("big", `{"label": "big", "doubled": $input.doubled}`),
			fn("small", `{"label": "small", "doubled": $input.doubled}`),
			{Name: "end_big", Type: "xflow.end"},
			{Name: "end_small", Type: "xflow.end"},
		},
		Connections: types.Connections{
			"start":  {"main": link("calc")},
			"calc":   {"main": link("branch")},
			"branch": {"true": link("big"), "false": link("small")},
			"big":    {"main": link("end_big")},
			"small":  {"main": link("end_small")},
		},
	}
}

// e2eBasicOutcome is what a run of e2eBasicDef is compared on.
type e2eBasicOutcome struct {
	Status types.ExecutionStatus
	Nodes  map[string]types.NodeStatus
	Output map[string]any
}

// runE2EBasic submits e2eBasicDef with {n: n} through node and waits for the
// terminal state, collecting each node's status and the surviving end output.
func runE2EBasic(t *testing.T, env *kafkaE2EEnv, node *kafkaE2ENode, n int) e2eBasicOutcome {
	t.Helper()
	g, err := graph.Compile(e2eBasicDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := env.ctx(t, 60*time.Second)
	id, err := node.eng.Submit(ctx, g, map[string]any{"n": n})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res := waitForCompletion(ctx, t, node.b.State(), id, "big", "small")
	out := e2eBasicOutcome{Status: res.Status, Nodes: map[string]types.NodeStatus{}, Output: res.Output}
	for _, d := range e2eBasicDef().Nodes {
		snap, err := node.b.State().GetNode(ctx, id, d.Name)
		if err != nil || snap == nil {
			t.Fatalf("GetNode(%s): %v", d.Name, err)
		}
		out.Nodes[d.Name] = snap.Status
	}
	return out
}

// TestKafkaQueueE2E_Basic runs the basic workflow over Kafka and checks the
// terminal state, each node's status and the chosen branch's output. It also
// asserts that the tasks really travelled through the default topic.
func TestKafkaQueueE2E_Basic(t *testing.T) {
	env := newKafkaE2EEnv(t)
	node := startKafkaE2ENode(t, env, kafkaE2EOpts{})
	topic := env.topics()[0]

	t.Run("true_branch", func(t *testing.T) {
		before := topicRecordCount(t, env, topic)
		got := runE2EBasic(t, env, node, 7)
		want := map[string]types.NodeStatus{
			"start": types.NodeStatusSuccess, "calc": types.NodeStatusSuccess, "branch": types.NodeStatusSuccess,
			"big": types.NodeStatusSuccess, "end_big": types.NodeStatusSuccess,
			"small": types.NodeStatusSkipped, "end_small": types.NodeStatusSkipped,
		}
		if got.Status != types.ExecutionStatusSuccess || !reflect.DeepEqual(got.Nodes, want) {
			t.Fatalf("status=%s nodes=%v, want success %v", got.Status, got.Nodes, want)
		}
		assertBranchOutput(t, got.Output["big"], "big", 14)
		if _, ok := got.Output["small"]; ok {
			t.Fatalf("skipped branch produced output: %v", got.Output["small"])
		}
		// Seven nodes, five of which run: each runnable node is one task.
		if delta := topicRecordCount(t, env, topic) - before; delta < 5 {
			t.Fatalf("default topic received %d records, want at least 5", delta)
		}
	})

	t.Run("false_branch", func(t *testing.T) {
		got := runE2EBasic(t, env, node, 2)
		if got.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status=%s nodes=%v, want success", got.Status, got.Nodes)
		}
		if got.Nodes["small"] != types.NodeStatusSuccess || got.Nodes["big"] != types.NodeStatusSkipped {
			t.Fatalf("nodes=%v, want small success and big skipped", got.Nodes)
		}
		assertBranchOutput(t, got.Output["small"], "small", 4)
	})
}

// assertBranchOutput checks the {label, doubled} map one branch node produced.
func assertBranchOutput(t *testing.T, v any, label string, doubled int) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%s output is %T (%v), want map", label, v, v)
	}
	if m["label"] != label || e2eInt(m["doubled"]) != doubled {
		t.Fatalf("%s output = %v, want label=%s doubled=%d", label, m, label, doubled)
	}
}

// e2eInt reads a JSON-decoded number (float64) or a native int as int, -1 if neither.
func e2eInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	}
	return -1
}
