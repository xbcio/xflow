package xflow

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestNodeRefTimeoutReachesNodeDef guards the transient-field lesson: a field
// that the compiler reads but no builder can set is a switch nobody can press.
func TestNodeRefTimeoutReachesNodeDef(t *testing.T) {
	wf := Workflow("timeout-builder")
	n := wf.LocalNode("worker", nil)
	n.Timeout(90 * time.Second)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var found bool
	for _, nd := range def.Nodes {
		if nd.Name == "worker" {
			found = true
			if nd.Timeout != 90*time.Second {
				t.Fatalf("NodeDef.Timeout = %v, want 90s", nd.Timeout)
			}
		}
	}
	if !found {
		t.Fatal("node \"worker\" not in built def")
	}
}

// TestNodeRefTimeoutNegative confirms the escape hatch survives the builder:
// a negative value must reach NodeDef verbatim, not be normalized to zero.
func TestNodeRefTimeoutNegative(t *testing.T) {
	wf := Workflow("timeout-builder-neg")
	wf.LocalNode("unbounded", nil).Timeout(-1)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, nd := range def.Nodes {
		if nd.Name == "unbounded" && nd.Timeout != -1 {
			t.Fatalf("NodeDef.Timeout = %v, want -1", nd.Timeout)
		}
	}
}

// TestWorkflowBuilderTimeoutSetsSettings pins the workflow-level switch: the
// value must reach settings.timeout, not only the builder field. A
// timeout-only workflow must also leave settings.on_error empty rather than
// acquiring a default.
func TestWorkflowBuilderTimeoutSetsSettings(t *testing.T) {
	const budget = 90 * time.Second
	wf := Workflow("workflow-timeout-builder").Timeout(budget)
	wf.Node("start", node.Start())

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if def.Settings == nil {
		t.Fatal("Settings = nil, want settings.timeout to be carried")
	}
	if def.Settings.Timeout != budget {
		t.Fatalf("Settings.Timeout = %v, want %v", def.Settings.Timeout, budget)
	}
	if def.Settings.OnError != "" {
		t.Fatalf("Settings.OnError = %q, want empty when only Timeout is set", def.Settings.OnError)
	}
}

// TestWorkflowBuilderTimeoutAndOnErrorCoexist guards the assembly against one
// switch displacing the other: both fields must be present when both are set.
func TestWorkflowBuilderTimeoutAndOnErrorCoexist(t *testing.T) {
	const budget = 45 * time.Second
	wf := Workflow("workflow-timeout-on-error").
		Timeout(budget).
		OnError(types.OnErrorContinue)
	wf.Node("start", node.Start())

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if def.Settings == nil || def.Settings.Timeout != budget || def.Settings.OnError != string(types.OnErrorContinue) {
		t.Fatalf("Settings = %+v, want timeout %v and on_error %q", def.Settings, budget, types.OnErrorContinue)
	}
}

// TestWorkflowBuilderTimeoutZeroIsUnset covers both spellings of "no budget":
// no Timeout call and Timeout(0) must leave Settings nil, the same shape the
// builder produced before the method existed.
func TestWorkflowBuilderTimeoutZeroIsUnset(t *testing.T) {
	for _, tc := range []struct {
		name string
		wf   *WorkflowBuilder
	}{
		{"unset", Workflow("workflow-timeout-unset")},
		{"explicit-zero", Workflow("workflow-timeout-zero").Timeout(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.wf.Node("start", node.Start())
			def, err := tc.wf.Definition()
			if err != nil {
				t.Fatalf("Definition: %v", err)
			}
			if def.Settings != nil {
				t.Fatalf("Settings = %+v, want nil without a budget", def.Settings)
			}
		})
	}
}

// TestWorkflowBuilderTimeoutNegativeRejectedByCompile proves the builder does
// not duplicate the guard: the negative value travels verbatim and the
// compiler's existing settings.timeout check rejects it.
func TestWorkflowBuilderTimeoutNegativeRejectedByCompile(t *testing.T) {
	wf := Workflow("workflow-timeout-negative").Timeout(-time.Second)
	wf.Node("start", node.Start())

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if _, err := graph.Compile(def); err == nil || !strings.Contains(err.Error(), "settings.timeout must be >= 0") {
		t.Fatalf("Compile error = %v, want the settings.timeout rejection", err)
	}
}

// TestWorkflowBuilderTimeoutFAFRejectedByCompile is the other compile-time
// rejection: a FAF execution persists nothing, so a non-zero budget fails
// closed instead of running without it.
func TestWorkflowBuilderTimeoutFAFRejectedByCompile(t *testing.T) {
	wf := Workflow("workflow-timeout-faf").FAF().Timeout(time.Minute)
	wf.LocalNode("only", nil)

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if _, err := graph.Compile(def); err == nil || !strings.Contains(err.Error(), "faf workflow cannot define settings.timeout") {
		t.Fatalf("Compile error = %v, want the faf settings.timeout rejection", err)
	}
}

// TestWorkflowBuilderTimeoutNegativeRejectedByAddWorkflow pins the same
// rejection on the path callers actually take: NewLocal plus AddWorkflow builds
// and compiles the definition, and the compiler's settings.timeout error must
// surface from registration rather than from a Definition-only caller. It is a
// pure registration case: nothing is invoked.
func TestWorkflowBuilderTimeoutNegativeRejectedByAddWorkflow(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("workflow-timeout-negative-add-workflow").Timeout(-time.Second)
	wf.Node("start", node.Start())

	if _, err := eng.AddWorkflow(context.Background(), wf); err == nil || !strings.Contains(err.Error(), "settings.timeout must be >= 0") {
		t.Fatalf("AddWorkflow() error = %v, want the settings.timeout rejection", err)
	}
}

// TestWorkflowBuilderTimeoutFAFRejectedByAddWorkflow is the FAF rejection on
// the public registration path, added for the same reason as the negative case
// above: an error only a direct graph.Compile caller sees is not a guard users
// meet.
func TestWorkflowBuilderTimeoutFAFRejectedByAddWorkflow(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("workflow-timeout-faf-add-workflow").FAF().Timeout(time.Minute)
	wf.LocalNode("only", nil)

	if _, err := eng.AddWorkflow(context.Background(), wf); err == nil || !strings.Contains(err.Error(), "faf workflow cannot define settings.timeout") {
		t.Fatalf("AddWorkflow() error = %v, want the faf settings.timeout rejection", err)
	}
}
