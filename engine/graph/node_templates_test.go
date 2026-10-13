package graph

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func templatedDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "tpl",
		NodeTemplates: map[string]types.NodeTemplate{
			"api": {Type: "xflow.http", Parameters: map[string]any{
				"method":  "GET",
				"url":     "https://a.example",
				"headers": map[string]any{"accept": "json", "x-trace": "on", "nested": map[string]any{"a": 1.0, "b": 2.0}},
				"retry":   []any{1.0, 2.0},
				"timeout": "5s",
			}},
		},
		Nodes: []types.NodeDef{{
			Name:     "call",
			Template: "api",
			Parameters: map[string]any{
				"method":  "POST",                                                                              // scalar override
				"headers": map[string]any{"accept": "xml", "x-trace": nil, "nested": map[string]any{"b": 3.0}}, // deep merge + nested delete
				"retry":   []any{9.0},                                                                          // list replace
				"timeout": nil,                                                                                 // top-level delete
			},
		}},
	}
}

func TestExpandNodeTemplatesMergeRules(t *testing.T) {
	out, err := ExpandNodeTemplates(templatedDef())
	if err != nil {
		t.Fatal(err)
	}
	nd := out.Nodes[0]
	if nd.Type != "xflow.http" || nd.Template != "" {
		t.Fatalf("type=%q template=%q", nd.Type, nd.Template)
	}
	want := map[string]any{
		"method":  "POST",
		"url":     "https://a.example",
		"headers": map[string]any{"accept": "xml", "nested": map[string]any{"a": 1.0, "b": 3.0}},
		"retry":   []any{9.0},
	}
	if !reflect.DeepEqual(nd.Parameters, want) {
		t.Fatalf("parameters = %#v\nwant %#v", nd.Parameters, want)
	}
}

func TestExpandNodeTemplatesErrors(t *testing.T) {
	cases := map[string]func(*types.WorkflowDef){
		"mutually exclusive": func(d *types.WorkflowDef) { d.Nodes[0].Type = "xflow.http" },
		"unknown template":   func(d *types.WorkflowDef) { d.Nodes[0].Template = "nope" },
		"template no type": func(d *types.WorkflowDef) {
			d.NodeTemplates["api"] = types.NodeTemplate{Parameters: map[string]any{"x": 1}}
		},
	}
	wants := map[string]string{"mutually exclusive": "mutually exclusive", "unknown template": "unknown node template", "template no type": "has no type"}
	for name, mutate := range cases {
		d := templatedDef()
		mutate(d)
		if _, err := ExpandNodeTemplates(d); err == nil || !strings.Contains(err.Error(), wants[name]) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if _, err := Compile(d); err == nil {
			t.Fatalf("%s: Compile accepted", name)
		}
	}
}

func TestCompileExpandsTemplatesWithoutMutatingDef(t *testing.T) {
	def := templatedDef()
	before, _ := json.Marshal(def)
	g, err := Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(def)
	if string(before) != string(after) {
		t.Fatalf("Compile mutated def:\n%s\n%s", before, after)
	}
	idx, ok := g.NodeIndex("call")
	n := g.NodeAt(idx)
	if !ok || n.Type != "xflow.http" || n.Parameters["method"] != "POST" {
		t.Fatalf("compiled node = %+v", n)
	}
	if g.ExpandedDefinition().Nodes[0].Type != "xflow.http" {
		t.Fatal("ExpandedDefinition not expanded")
	}
}

func TestExpandNodeTemplatesNoTemplateReturnsDef(t *testing.T) {
	def := &types.WorkflowDef{Nodes: []types.NodeDef{{Name: "a", Type: "x"}}}
	out, err := ExpandNodeTemplates(def)
	if err != nil || out != def {
		t.Fatalf("out=%p def=%p err=%v", out, def, err)
	}
}
