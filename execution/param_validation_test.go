package execution_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	_ "github.com/xbcio/xflow/node" // registers the builtin node types
	"github.com/xbcio/xflow/types"
)

func fakeLookup(descs map[string]types.Descriptor) execution.DescriptorLookup {
	return func(nodeType string, version int) (types.Descriptor, bool) {
		if version != 0 {
			d, ok := descs[nodeType+"@"+string(rune('0'+version))]
			return d, ok
		}
		d, ok := descs[nodeType]
		return d, ok
	}
}

var requiredURL = types.Descriptor{Type: "t.http", Params: []types.ParamSpec{{Name: "url", Required: true}}}

func issueNodes(issues []graph.ParamIssue) []string {
	out := make([]string, len(issues))
	for i, is := range issues {
		out[i] = is.Node + " " + is.Path + " " + is.Code
	}
	sort.Strings(out)
	return out
}

func bodyParam(members ...map[string]any) map[string]any {
	nodes := make([]any, len(members))
	for i, m := range members {
		nodes[i] = m
	}
	return map[string]any{
		"type":       types.SubgraphNodeType,
		"parameters": map[string]any{"nodes": nodes},
	}
}

func TestValidateWorkflowParamsPerNode(t *testing.T) {
	def := &types.WorkflowDef{Nodes: []types.NodeDef{
		{Name: "A", Type: "t.http", Parameters: map[string]any{}},
		{Name: "B", Type: "t.http", Parameters: map[string]any{"url": "x"}},
		{Name: "Off", Type: "t.http", Disabled: true, Parameters: map[string]any{}},
		{Name: "V2", Type: "t.http", Version: 2, Parameters: map[string]any{}},
	}}
	v2 := types.Descriptor{Type: "t.http", Params: []types.ParamSpec{{Name: "endpoint", Required: true}}}
	got := execution.ValidateWorkflowParams(def, fakeLookup(map[string]types.Descriptor{"t.http": requiredURL, "t.http@2": v2}))
	want := []string{"A /parameters/url required", "V2 /parameters/endpoint required"}
	if !reflect.DeepEqual(issueNodes(got), want) {
		t.Fatalf("issues = %q, want %q", issueNodes(got), want)
	}
}

func TestValidateWorkflowParamsUnknownTypeSkipped(t *testing.T) {
	def := &types.WorkflowDef{Nodes: []types.NodeDef{
		{Name: "C", Type: "custom.thing", Version: 3, Parameters: map[string]any{}},
		{Name: "A", Type: "t.http", Parameters: map[string]any{"url": "x"}},
	}}
	var skipped []string
	got := execution.ValidateWorkflowParamsWithOptions(def, fakeLookup(map[string]types.Descriptor{"t.http": requiredURL}),
		execution.ParamValidationOptions{OnUnknownType: func(nodeType string, version int, node string) {
			skipped = append(skipped, nodeType+"@"+string(rune('0'+version))+" "+node)
		}})
	if len(got) != 0 {
		t.Fatalf("unknown type must not produce issues, got %q", issueNodes(got))
	}
	if !reflect.DeepEqual(skipped, []string{"custom.thing@3 C"}) {
		t.Fatalf("skipped = %q", skipped)
	}
}

func TestValidateWorkflowParamsRecursesIntoBodies(t *testing.T) {
	def := &types.WorkflowDef{Nodes: []types.NodeDef{
		{Name: "Loop", Type: "t.map", Parameters: map[string]any{
			"items": "{{ $input.items }}",
			"body": bodyParam(
				map[string]any{"name": "Fetch", "type": "t.http", "parameters": map[string]any{}},
				map[string]any{"name": "Skip", "type": "t.http", "disabled": true, "parameters": map[string]any{}},
				map[string]any{"name": "Ok", "type": "t.http", "parameters": map[string]any{"url": "u"}},
			),
		}},
		// A request-payload "body" is not a sub-graph: not recursed into.
		{Name: "Post", Type: "t.http", Parameters: map[string]any{"url": "u", "body": map[string]any{
			"nodes": []any{map[string]any{"name": "X", "type": "t.http"}},
		}}},
	}}
	mapDesc := types.Descriptor{Type: "t.map", Params: []types.ParamSpec{{Name: "items", Required: true}}}
	var skipped []string
	got := execution.ValidateWorkflowParamsWithOptions(def,
		fakeLookup(map[string]types.Descriptor{"t.http": requiredURL, "t.map": mapDesc}),
		execution.ParamValidationOptions{OnUnknownType: func(nodeType string, _ int, node string) { skipped = append(skipped, node) }})
	want := []string{"Loop/Fetch /parameters/url required"}
	if !reflect.DeepEqual(issueNodes(got), want) {
		t.Fatalf("issues = %q, want %q", issueNodes(got), want)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skips %q", skipped)
	}
}

func TestValidateWorkflowParamsDoesNotMutate(t *testing.T) {
	def := &types.WorkflowDef{Nodes: []types.NodeDef{{Name: "W", Type: "xflow.wait", Parameters: map[string]any{"mode": "timer"}}}}
	_ = execution.ValidateWorkflowParams(def, execution.RegistryDescriptorLookup)
	if !reflect.DeepEqual(def.Nodes[0].Parameters, map[string]any{"mode": "timer"}) {
		t.Fatalf("params mutated: %v", def.Nodes[0].Parameters)
	}
}

func TestRegistryDescriptorLookup(t *testing.T) {
	for _, typ := range []string{"xflow.http", "xflow.trigger.cron"} {
		desc, ok := execution.RegistryDescriptorLookup(typ, 0)
		if !ok || desc.Type != typ {
			t.Fatalf("lookup %s latest = (%q, %v)", typ, desc.Type, ok)
		}
	}
	if _, ok := execution.RegistryDescriptorLookup("xflow.http", 99); ok {
		t.Fatal("an unregistered version must not resolve")
	}
	if _, ok := execution.RegistryDescriptorLookup("no.such.type", 0); ok {
		t.Fatal("an unregistered type must not resolve")
	}

	// A builtin end to end: xflow.wait in timer mode needs duration or until.
	def := &types.WorkflowDef{Nodes: []types.NodeDef{{Name: "W", Type: "xflow.wait", Parameters: map[string]any{"mode": "timer"}}}}
	got := issueNodes(execution.ValidateWorkflowParams(def, execution.RegistryDescriptorLookup))
	want := []string{"W /parameters/duration required", "W /parameters/until required"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("issues = %q, want %q", got, want)
	}
}
