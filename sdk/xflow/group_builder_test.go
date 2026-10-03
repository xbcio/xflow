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

// TestBuilderGroupOnErrorMainOutputRejected proves the compile-time
// rejection of group-level main_output is reachable from the SDK, not just
// from a direct graph.Compile call.
//
// GroupRef.OnError takes a types.OnError, so types.OnErrorMainOutput is a
// type-legal argument — nothing in the builder can refuse it. The gate has to
// live in graph.Compile (validateGroupOnError), and AddWorkflow is the only
// production path that reaches it.
func TestBuilderGroupOnErrorMainOutputRejected(t *testing.T) {
	wf := Workflow("traffic-analyze")
	edge := wf.Group("edge").OnError(types.OnErrorMainOutput)
	ingest := wf.LocalNode("ingest", nil)
	analyze := wf.LocalNode("analyze", nil)
	ingest.Group(edge)
	analyze.Group(edge)
	wf.Connect(ingest, analyze)

	// build() is the pure-assembly half and must stay permissive: the
	// value is type-legal and the builder does no graph analysis.
	def, err := wf.build()
	if err != nil {
		t.Fatalf("on_error=main_output: build must not reject a type-legal value: %v", err)
	}
	if _, err := graph.Compile(def); err == nil {
		t.Fatal("on_error=main_output reached a compiled graph from the SDK")
	}
}

// TestBuilderGroupErrorOutputWithoutTargetsRejected proves that
// on_error=error_output with no ErrorOutputs targets is rejected through the
// SDK path, mirroring graph.TestGroupOnErrorOutputRequiresErrorOutputs.
func TestBuilderGroupErrorOutputWithoutTargetsRejected(t *testing.T) {
	wf := Workflow("traffic-analyze")
	edge := wf.Group("edge").OnError(types.OnErrorOutput)
	ingest := wf.LocalNode("ingest", nil)
	analyze := wf.LocalNode("analyze", nil)
	ingest.Group(edge)
	analyze.Group(edge)
	wf.Connect(ingest, analyze)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build must not reject a type-legal value: %v", err)
	}
	if _, err := graph.Compile(def); err == nil {
		t.Fatal("on_error=error_output with no ErrorOutputs targets reached a compiled graph from the SDK")
	}
}

// TestBuilderGroupErrorOutputAssembly proves the SDK's positive path end to
// end: GroupRef.ErrorOutputs assembles into GroupDef.ErrorOutputs, and the
// resulting definition compiles cleanly through graph.Compile — the same
// production path AddWorkflow uses.
func TestBuilderGroupErrorOutputAssembly(t *testing.T) {
	wf := Workflow("traffic-analyze")
	edge := wf.Group("edge").
		OnError(types.OnErrorOutput).
		ErrorOutputs(types.Connection{Node: "notify"})

	ingest := wf.LocalNode("ingest", nil)
	analyze := wf.LocalNode("analyze", nil)
	_ = wf.LocalNode("notify", nil)
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
	if g.OnError != string(types.OnErrorOutput) {
		t.Fatalf("OnError = %q, want %q", g.OnError, types.OnErrorOutput)
	}
	if len(g.ErrorOutputs) != 1 || g.ErrorOutputs[0].Node != "notify" {
		t.Fatalf("ErrorOutputs = %+v, want one target naming %q", g.ErrorOutputs, "notify")
	}

	compiled, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	cgm := compiled.Groups()[0]
	notifyIdx, ok := compiled.NodeIndex("notify")
	if !ok {
		t.Fatal("notify node not found in compiled graph")
	}
	if len(cgm.ErrorOutputs) != 1 || cgm.ErrorOutputs[0].NodeIdx != notifyIdx {
		t.Fatalf("resolved ErrorOutputs = %+v, want NodeIdx %d", cgm.ErrorOutputs, notifyIdx)
	}
}
