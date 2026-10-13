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
