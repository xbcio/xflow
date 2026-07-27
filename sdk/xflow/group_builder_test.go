package xflow

import (
	"testing"
	"time"

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
