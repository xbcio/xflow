package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func onErrorDef(settings, node string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "on-error",
		Settings: &types.WorkflowSettings{OnError: settings},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "worker", Type: "test.worker", OnError: node},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
		},
	}
}

func workerOnError(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	idx, _ := g.NodeIndex("worker")
	return g.NodeAt(idx).OnError
}

func TestSettingsOnErrorInherited(t *testing.T) {
	if got := workerOnError(t, onErrorDef("continue", "")); got != "continue" {
		t.Fatalf("OnError = %q, want inherited continue", got)
	}
}

func TestSettingsOnErrorNodeOverride(t *testing.T) {
	if got := workerOnError(t, onErrorDef("continue", "error_output")); got != "error_output" {
		t.Fatalf("OnError = %q, want node error_output", got)
	}
}

func TestSettingsOnErrorInvalid(t *testing.T) {
	_, err := Compile(onErrorDef("retry", ""))
	if err == nil || !strings.Contains(err.Error(), "settings.on_error") {
		t.Fatalf("err = %v, want settings.on_error rejection", err)
	}
}

// TestNodeOnErrorInvalidRejected pins the node-level check: a misspelled
// node on_error used to compile clean and silently become "stop" through
// ApplyOnError's default branch. The rejection must name the node and the
// invalid value so the author can find the line that never took effect.
func TestNodeOnErrorInvalidRejected(t *testing.T) {
	_, err := Compile(onErrorDef("", "retry"))
	if err == nil {
		t.Fatal(`node on_error "retry" compiled clean`)
	}
	msg := err.Error()
	for _, want := range []string{`node "worker"`, "on_error", `"retry"`} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestNodeOnErrorValidValuesAccepted is the positive half: every policy the
// engine implements compiles at node level, including the empty value that
// means "inherit settings.on_error".
func TestNodeOnErrorValidValuesAccepted(t *testing.T) {
	valid := []string{
		"",
		string(types.OnErrorStop),
		string(types.OnErrorOutput),
		string(types.OnErrorMainOutput),
		string(types.OnErrorContinue),
	}
	for _, policy := range valid {
		t.Run("policy="+policy, func(t *testing.T) {
			g, err := Compile(onErrorDef("", policy))
			if err != nil {
				t.Fatalf("Compile(on_error=%q): %v", policy, err)
			}
			idx, _ := g.NodeIndex("worker")
			if got := g.NodeAt(idx).OnError; got != policy {
				t.Fatalf("OnError = %q, want %q", got, policy)
			}
		})
	}
}

func TestSettingsOnErrorFAFRejected(t *testing.T) {
	def := validFAFDef()
	def.Settings = &types.WorkflowSettings{OnError: "continue"}
	if _, err := Compile(def); err == nil || !strings.Contains(err.Error(), "on_error") {
		t.Fatalf("err = %v, want faf on_error rejection", err)
	}
	def.Settings.OnError = "stop"
	if _, err := Compile(def); err != nil {
		t.Fatalf("faf with settings.on_error stop: %v", err)
	}
}

func TestSettingsOnErrorGroupsDoNotInherit(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Settings = &types.WorkflowSettings{OnError: "main_output"}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	for _, gm := range g.Groups() {
		if gm.OnError != "" {
			t.Fatalf("group %q OnError = %q, want empty", gm.Name, gm.OnError)
		}
	}
}
