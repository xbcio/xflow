package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func tzDef(tz string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "tz",
		Settings: &types.WorkflowSettings{Timezone: tz},
		Nodes:    []types.NodeDef{{Name: "a", Type: "test.echo"}},
	}
}

func TestCompile_SettingsTimezone(t *testing.T) {
	g, err := Compile(tzDef("Asia/Shanghai"))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if g.Timezone() != "Asia/Shanghai" {
		t.Fatalf("Timezone = %q, want Asia/Shanghai", g.Timezone())
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Graph
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Timezone() != "Asia/Shanghai" {
		t.Fatalf("round-tripped Timezone = %q", back.Timezone())
	}

	unset, err := Compile(tzDef(""))
	if err != nil {
		t.Fatalf("Compile unset: %v", err)
	}
	if unset.Timezone() != "" {
		t.Fatalf("unset Timezone = %q, want empty", unset.Timezone())
	}
	if unset.Hash() == g.Hash() {
		t.Fatal("settings.timezone must be part of the graph hash")
	}
}

func TestCompile_RejectsInvalidSettingsTimezone(t *testing.T) {
	_, err := Compile(tzDef("Mars/Olympus"))
	if err == nil || !strings.Contains(err.Error(), "settings.timezone") {
		t.Fatalf("err = %v, want settings.timezone error", err)
	}
}
