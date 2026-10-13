package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func credDef(nodeType, param string, value any, declared ...string) *types.WorkflowDef {
	creds := map[string]types.CredentialDef{}
	for _, n := range declared {
		creds[n] = types.CredentialDef{Name: n}
	}
	return &types.WorkflowDef{
		Name:        "creds",
		Credentials: creds,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "worker", Type: nodeType, Parameters: map[string]any{param: value}},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
		},
	}
}

func TestCredentialRefUndeclaredRejected(t *testing.T) {
	cases := []*types.WorkflowDef{
		credDef("xflow.database", "credential", "missing_db"),
		credDef("xflow.http", "authentication", "missing_db", "other"),
		credDef("xflow.script", "credentials", []any{"other", "missing_db"}, "other"),
	}
	for _, def := range cases {
		_, err := Compile(def)
		if err == nil || !strings.Contains(err.Error(), `"worker"`) || !strings.Contains(err.Error(), "missing_db") {
			t.Fatalf("%s: err = %v, want naming worker and missing_db", def.Nodes[1].Type, err)
		}
	}
}

func TestCredentialRefDeclaredAndDynamicCompile(t *testing.T) {
	cases := []*types.WorkflowDef{
		credDef("xflow.database", "credential", "db", "db"),
		credDef("xflow.http", "authentication", "api", "api"),
		credDef("xflow.script", "credentials", []string{"a", "b"}, "a", "b"),
		credDef("xflow.database", "credential", "${{ $vars.cred }}"),
		credDef("xflow.script", "credentials", "${{ $vars.creds }}"),
		credDef("xflow.http", "authentication", 42),
	}
	for _, def := range cases {
		if _, err := Compile(def); err != nil {
			t.Fatalf("%s %v: %v", def.Nodes[1].Type, def.Nodes[1].Parameters, err)
		}
	}
}
