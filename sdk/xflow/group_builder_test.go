package xflow

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestBuilderGroupAssembly(t *testing.T) {
	wf := Workflow("traffic-analyze")
	edge := wf.Group("edge").
		RunnerSelector(RequiredRunnerSelector(map[string]string{"cloud": "tencent"})).
		OnError(types.OnErrorStop).
		Timeout(30 * time.Second)

	ingest := wf.LocalNode("ingest", nil)
	analyze := wf.LocalNode("analyze", nil)
	ingest.Group(edge)
	analyze.Group(edge)
	wf.Connect(ingest, analyze)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(def.Groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(def.Groups))
	}
	g := def.Groups[0]
	if g.Name != "edge" {
		t.Fatalf("name: %q", g.Name)
	}
	if len(g.Members) != 2 || g.Members[0] != "ingest" || g.Members[1] != "analyze" {
		t.Fatalf("members: %v", g.Members)
	}
	if g.RunnerSelector == nil || g.RunnerSelector.Mode != types.RunnerSelectorModeRequired {
		t.Fatalf("selector: %+v", g.RunnerSelector)
	}
	if g.OnError != string(types.OnErrorStop) || g.Timeout != 30*time.Second {
		t.Fatalf("onError/timeout: %q %v", g.OnError, g.Timeout)
	}
}

// TestBuilderGroupOnErrorOutputRejected proves the compile-time rejection of
// group-level error_output is reachable from the SDK, not just from a direct
// graph.Compile call.
//
// GroupRef.OnError takes a types.OnError, so types.OnErrorOutput is a
// type-legal argument — nothing in the builder can refuse it. The gate has to
// live in graph.Compile (validateGroupOnError), and AddWorkflow is the only
// production path that reaches it. Without this test, "the graph package
// rejects it" is true while the surface every author actually uses still
// silently degrades the policy to `stop`.
func TestBuilderGroupOnErrorOutputRejected(t *testing.T) {
	for _, policy := range []types.OnError{types.OnErrorOutput, types.OnErrorMainOutput} {
		wf := Workflow("traffic-analyze")
		edge := wf.Group("edge").OnError(policy)
		ingest := wf.LocalNode("ingest", nil)
		analyze := wf.LocalNode("analyze", nil)
		ingest.Group(edge)
		analyze.Group(edge)
		wf.Connect(ingest, analyze)

		// build() is the pure-assembly half and must stay permissive: the
		// value is type-legal and the builder does no graph analysis.
		def, err := wf.build()
		if err != nil {
			t.Fatalf("on_error=%q: build must not reject a type-legal value: %v", policy, err)
		}
		if _, err := graph.Compile(def); err == nil {
			t.Fatalf("on_error=%q reached a compiled graph from the SDK; "+
				"it would run as `stop` and fail the whole execution", policy)
		}
	}
}
