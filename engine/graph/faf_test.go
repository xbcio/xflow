package graph

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func validFAFDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "faf",
		Options: &types.WorkflowOptions{
			FAF: true,
		},
		Context: &types.WorkflowContext{
			Vars:   map[string]any{"request_id": "req-1"},
			Config: map[string]any{"attempt": 1},
		},
		Params: map[string]types.ParamDef{
			"message": {Type: "string"},
		},
		Nodes: []types.NodeDef{{
			Name:       "only",
			Type:       "xflow.noop",
			Kind:       types.NodeKindAction,
			Parameters: map[string]any{"message": "hello"},
			Timeout:    time.Second,
		}},
	}
}

func TestCompileFAFPropagatesThroughGraphSnapshotAndHash(t *testing.T) {
	g, err := Compile(validFAFDef())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if !g.FAF() {
		t.Fatal("FAF() = false, want true")
	}
	if g.Transient() {
		t.Fatal("Transient() = true, want false")
	}

	wire, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	if !strings.Contains(string(wire), `"faf":true`) {
		t.Fatalf("serialized graph omits faf=true: %s", wire)
	}
	var decoded Graph
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal graph: %v", err)
	}
	if !decoded.FAF() {
		t.Fatal("decoded FAF() = false, want true")
	}
	if decoded.Hash() != g.Hash() {
		t.Fatalf("hash after round-trip = %q, want %q", decoded.Hash(), g.Hash())
	}

	durable := validFAFDef()
	durable.Options = nil
	withoutFAF, err := Compile(durable)
	if err != nil {
		t.Fatalf("compile durable graph: %v", err)
	}
	if g.Hash() == withoutFAF.Hash() {
		t.Fatal("FAF must participate in the graph hash")
	}
}

func TestCompileFAFAllowsOnlyActionKinds(t *testing.T) {
	for _, kind := range []types.NodeKind{"", types.NodeKindAction} {
		t.Run(string(kind), func(t *testing.T) {
			def := validFAFDef()
			def.Nodes[0].Kind = kind
			if _, err := Compile(def); err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
		})
	}
}

func TestCompileFAFRejectsTriggerAndSupplyKinds(t *testing.T) {
	for _, kind := range []types.NodeKind{types.NodeKindTrigger, types.NodeKindSupply} {
		t.Run(string(kind), func(t *testing.T) {
			def := validFAFDef()
			def.Nodes[0].Kind = kind
			_, err := Compile(def)
			if err == nil || !strings.Contains(err.Error(), "must be an action") {
				t.Fatalf("Compile() error = %v, want action-only FAF rejection", err)
			}
		})
	}
}

func TestCompileFAFRejectsUnsupportedDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*types.WorkflowDef)
		want   string
	}{
		{
			name: "transient",
			mutate: func(def *types.WorkflowDef) {
				def.Options.Transient = true
			},
			want: "transient mode or transient TTLs",
		},
		{
			name: "transient active TTL",
			mutate: func(def *types.WorkflowDef) {
				def.Options.TransientTTL = time.Second
			},
			want: "transient mode or transient TTLs",
		},
		{
			name: "transient completion TTL",
			mutate: func(def *types.WorkflowDef) {
				def.Options.TransientCompletionTTL = time.Second
			},
			want: "transient mode or transient TTLs",
		},
		{
			name: "cycles",
			mutate: func(def *types.WorkflowDef) {
				def.Options.AllowCycles = true
			},
			want: "cannot allow cycles",
		},
		{
			name: "groups",
			mutate: func(def *types.WorkflowDef) {
				def.Groups = []types.GroupDef{{Name: "group"}}
			},
			want: "cannot define groups",
		},
		{
			name: "workflow runner selector",
			mutate: func(def *types.WorkflowDef) {
				def.RunnerSelector = &types.RunnerSelector{}
			},
			want: "workflow runner selector",
		},
		{
			name: "connections",
			mutate: func(def *types.WorkflowDef) {
				def.Connections = types.Connections{"only": {"main": {}}}
			},
			want: "connections or dataflow",
		},
		{
			name: "dependency edges",
			mutate: func(def *types.WorkflowDef) {
				def.DependencyEdges = []types.DependencyEdge{{Node: "only", Supply: "supply"}}
			},
			want: "dependency edges",
		},
		{
			name: "workflow outputs",
			mutate: func(def *types.WorkflowDef) {
				def.Outputs = map[string]types.WorkflowOutput{"result": {Value: "result"}}
			},
			want: "workflow outputs",
		},
		{
			name: "workflow retry",
			mutate: func(def *types.WorkflowDef) {
				def.Settings = &types.WorkflowSettings{Retry: &types.RetrySettings{MaxAttempts: 2}}
			},
			want: "workflow retries",
		},
		{
			name: "node retry",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Retry = &types.RetrySettings{MaxAttempts: 2}
			},
			want: "cannot define retries",
		},
		{
			name: "script file artifact marker",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Type = "xflow.script"
				def.Nodes[0].Parameters = map[string]any{"__artifact_file_path": "/tmp/script.js"}
			},
			want: "artifact-backed script code",
		},
		{
			name: "script artifact digest",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Type = "xflow.script"
				def.Nodes[0].Parameters = map[string]any{"artifact_digest": "sha256:abc"}
			},
			want: "artifact-backed script code",
		},
		{
			name: "multiple nodes",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes = append(def.Nodes, types.NodeDef{Name: "second", Type: "xflow.noop"})
			},
			want: "exactly one node",
		},
		{
			name: "unsupported node kind",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Kind = types.NodeKind("custom")
			},
			want: "must be an action",
		},
		{
			name: "node on error",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].OnError = string(types.OnErrorContinue)
			},
			want: "cannot define on_error",
		},
		{
			name: "node runner selector",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].RunnerSelector = &types.RunnerSelector{}
			},
			want: "cannot define a runner selector",
		},
		{
			name: "activation replicas",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].ActivationReplicas = 2
			},
			want: "activation replicas",
		},
		{
			name: "subgraph body",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Parameters["body"] = map[string]any{"type": types.SubgraphNodeType}
			},
			want: "subgraph body",
		},
		{
			name: "static nodes reference",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Parameters["value"] = "{{ $nodes['upstream'].value }}"
			},
			want: "reference $nodes statically",
		},
		{
			name: "dynamic nodes reference",
			mutate: func(def *types.WorkflowDef) {
				def.Nodes[0].Parameters["value"] = "{{ $nodes[$vars.which].value }}"
			},
			want: "reference $nodes dynamically",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(func() *types.WorkflowDef {
				def := validFAFDef()
				tt.mutate(def)
				return def
			}())
			if err == nil {
				t.Fatal("Compile() error = nil, want FAF rejection")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Compile() error = %q, want substring %q", err, tt.want)
			}
		})
	}
}
