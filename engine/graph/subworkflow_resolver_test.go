package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// fakeSubWorkflowResolver serves pre-built targets and records every ref it
// was asked about.
type fakeSubWorkflowResolver struct {
	targets map[string]SubWorkflowTarget
	err     error
	refs    []SubWorkflowRef
}

func (f *fakeSubWorkflowResolver) ResolveSubWorkflow(_ context.Context, ref SubWorkflowRef) (SubWorkflowTarget, error) {
	f.refs = append(f.refs, ref)
	if f.err != nil {
		return SubWorkflowTarget{}, f.err
	}
	t, ok := f.targets[ref.Name]
	if !ok {
		return SubWorkflowTarget{}, fmt.Errorf("workflow %q is not registered", ref.Name)
	}
	return t, nil
}

// subWorkflowTargetGraph compiles a minimal target workflow with an entry node
// nameable via entry.
func subWorkflowTargetGraph(t *testing.T, name string) *Graph {
	t.Helper()
	g, err := Compile(&types.WorkflowDef{
		Name:    name,
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "work"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile target %q: %v", name, err)
	}
	return g
}

func TestCompileWithResolverPinsSubWorkflowBinding(t *testing.T) {
	resolver := &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{
		"payment": {Version: "v1", DefinitionHash: "sha256:target", Graph: subWorkflowTargetGraph(t, "payment")},
	}}
	g, err := CompileWithResolver(context.Background(), subWorkflowDef(map[string]any{"workflow": " payment "}), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	if len(resolver.refs) != 1 {
		t.Fatalf("resolver saw %d refs, want 1", len(resolver.refs))
	}
	ref := resolver.refs[0]
	if ref.Namespace != "" || ref.Name != "payment" || ref.Version != "" || ref.DefinitionHash != "" {
		t.Fatalf("compile-time ref = %+v, want {Name: payment} with empty namespace/version/hash", ref)
	}
	idx, _ := g.NodeIndex("sub")
	binding := g.NodeAt(idx).SubWorkflow
	if binding == nil {
		t.Fatal("binding is nil after a successful resolution")
	}
	if binding.Name != "payment" || binding.Version != "v1" || binding.DefinitionHash != "sha256:target" {
		t.Fatalf("binding = %+v, want the target's version and hash", binding)
	}
}

func TestCompileWithoutResolverLeavesSubWorkflowUnresolved(t *testing.T) {
	def := subWorkflowDef(map[string]any{"workflow": "payment"})
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v; an unresolvable reference is an engine-dispatch refusal, not a compile error", err)
	}
	idx, _ := g.NodeIndex("sub")
	if b := g.NodeAt(idx).SubWorkflow; b != nil {
		t.Fatalf("binding = %+v, want nil without a resolver", b)
	}
}

// TestSubWorkflowBindingRoundTripsAndHashes pins the two properties the pin
// exists for: a snapshot-loaded graph keeps it, and a graph compiled against a
// different target revision is a different graph.
func TestSubWorkflowBindingRoundTripsAndHashes(t *testing.T) {
	compileWith := func(version, hash string) *Graph {
		resolver := &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{
			"payment": {Version: version, DefinitionHash: hash, Graph: subWorkflowTargetGraph(t, "payment")},
		}}
		g, err := CompileWithResolver(context.Background(), subWorkflowDef(map[string]any{"workflow": "payment"}), resolver)
		if err != nil {
			t.Fatalf("CompileWithResolver(%s/%s): %v", version, hash, err)
		}
		return g
	}
	v1 := compileWith("v1", "sha256:a")
	v2 := compileWith("v2", "sha256:b")

	wire, err := json.Marshal(v1)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Graph
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	idx, _ := decoded.NodeIndex("sub")
	if b := decoded.NodeAt(idx).SubWorkflow; b == nil || b.Version != "v1" || b.DefinitionHash != "sha256:a" {
		t.Fatalf("decoded binding = %+v, want the pinned v1/sha256:a; without it a snapshot-loaded "+
			"graph would re-resolve the child at dispatch", b)
	}
	if decoded.Hash() != v1.Hash() {
		t.Fatalf("decoded hash = %q, want %q", decoded.Hash(), v1.Hash())
	}
	if v1.Hash() == v2.Hash() {
		t.Fatal("graphs pinned to different target revisions share a hash: the pin is not part of the graph's identity")
	}
}

func TestCompileWithResolverRejectsBadTargets(t *testing.T) {
	target := subWorkflowTargetGraph(t, "payment")
	okTarget := SubWorkflowTarget{Version: "v1", DefinitionHash: "sha256:target", Graph: target}

	tests := []struct {
		name     string
		params   map[string]any
		resolver *fakeSubWorkflowResolver
		wantErr  string
	}{
		{
			name:     "resolver error",
			params:   map[string]any{"workflow": "payment"},
			resolver: &fakeSubWorkflowResolver{err: errors.New("store unavailable")},
			wantErr:  "resolve sub-workflow",
		},
		{
			name:     "missing target",
			params:   map[string]any{"workflow": "ghost"},
			resolver: &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{}},
			wantErr:  "not registered",
		},
		{
			name:     "resolver returned no graph",
			params:   map[string]any{"workflow": "payment"},
			resolver: &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{"payment": {Version: "v1"}}},
			wantErr:  "no graph",
		},
		{
			name:   "resolver returned no pin at all",
			params: map[string]any{"workflow": "payment"},
			resolver: &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{
				"payment": {Graph: target},
			}},
			wantErr: "neither a version nor a definition hash",
		},
		{
			name:     "unknown entry",
			params:   map[string]any{"workflow": "payment", "entry": "nope"},
			resolver: &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{"payment": okTarget}},
			wantErr:  `has no entry node "nope"`,
		},
		{
			name:     "known entry",
			params:   map[string]any{"workflow": "payment", "entry": "start"},
			resolver: &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{"payment": okTarget}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileWithResolver(context.Background(), subWorkflowDef(tt.params), tt.resolver)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("CompileWithResolver error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("CompileWithResolver accepted %v, want error %q", tt.params, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompileWithResolverPassesPinnedVersion(t *testing.T) {
	resolver := &fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{
		"payment": {Version: "v2", DefinitionHash: "sha256:target", Graph: subWorkflowTargetGraph(t, "payment")},
	}}
	_, err := CompileWithResolver(context.Background(),
		subWorkflowDef(map[string]any{"workflow": "payment", "version": "v2"}), resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	if len(resolver.refs) != 1 || resolver.refs[0].Version != "v2" {
		t.Fatalf("refs = %+v, want the author's pinned version v2", resolver.refs)
	}
}

// TestCompileWithResolverRejectsUnrunnableTargets pins the two target shapes a
// parent may never wait on: fire-and-forget (nothing persisted to report a
// conclusion through) and transient (the records that would resolve the wait
// can expire out from under it).
func TestCompileWithResolverRejectsUnrunnableTargets(t *testing.T) {
	fafGraph, err := Compile(validFAFDef())
	if err != nil {
		t.Fatalf("compile faf fixture: %v", err)
	}
	if !fafGraph.FAF() {
		t.Fatal("fixture: validFAFDef did not compile to a FAF graph")
	}
	transientGraph, err := Compile(&types.WorkflowDef{
		Name:    "ephemeral",
		Options: &types.WorkflowOptions{Transient: true},
		Nodes:   []types.NodeDef{{Name: "work", Type: "xflow.noop"}},
	})
	if err != nil {
		t.Fatalf("compile transient fixture: %v", err)
	}
	if !transientGraph.Transient() {
		t.Fatal("fixture: the transient definition did not compile to a transient graph")
	}

	tests := []struct {
		name    string
		ref     string
		graph   *Graph
		wantErr string
	}{
		{"faf target", "faf", fafGraph, "fire-and-forget"},
		{"transient target", "ephemeral", transientGraph, "transient"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileWithResolver(context.Background(), subWorkflowRefDef("parent", tt.ref, ""),
				&fakeSubWorkflowResolver{targets: map[string]SubWorkflowTarget{
					tt.ref: {Version: "v1", DefinitionHash: "sha256:target", Graph: tt.graph},
				}})
			if err == nil {
				t.Fatalf("a %s was accepted as a sub-workflow target", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompileTrustedRejectsSubWorkflowNode(t *testing.T) {
	_, err := compileTrusted(&types.WorkflowDef{
		Name: "pkg",
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: map[string]any{"workflow": "payment"},
		}},
	}, nil, nil, nil)
	if err == nil {
		t.Fatal("compileTrusted accepted a sub-workflow node: that path has its own NodeMeta copy " +
			"and no resolution pass, so the binding would be silently dropped")
	}
	if !strings.Contains(err.Error(), types.SubWorkflowNodeType) {
		t.Fatalf("error %q does not name %s", err, types.SubWorkflowNodeType)
	}
}
