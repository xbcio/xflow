package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// nsScopedRegistryStub mirrors the distributed registry's keying contract: a
// record is only visible to reads whose context carries its own namespace
// (registryPrefix derives the Redis key from namespace.FromContext). Reads in
// any other namespace miss with ErrWorkflowNotFound, exactly like production.
//
// The existing GroupActivateDirectiveCarriesPackage test cannot catch a
// namespace-blind read: it registers into local.New() (an in-memory registry
// that ignores the context) and runs with namespace.Default, so the
// FromContext fallback coincides with the truth. This stub removes both
// coincidences.
type nsScopedRegistryStub struct {
	backend.WorkflowRegistry
	record backend.WorkflowRecord
	reads  []namespace.Namespace
}

func (r *nsScopedRegistryStub) GetWorkflow(ctx context.Context, _ types.WorkflowID) (backend.WorkflowRecord, error) {
	ns := namespace.FromContext(ctx)
	r.reads = append(r.reads, ns)
	if ns != namespace.Namespace(r.record.Namespace) {
		return backend.WorkflowRecord{}, backend.ErrWorkflowNotFound
	}
	return r.record, nil
}

// TestGroupPackageProjectionReadsInTheActivationsNamespace pins the namespace
// the group-package projection resolves the workflow record in. The reconcile
// pass ctx is the control-plane loop's and carries NO namespace (FromContext
// falls back to Default), so projectPackageForGroup must attach act.Namespace
// itself before reading the namespace-scoped registry.
//
// Mutation: replace the wrap with a no-op alias (`nsCtx := ctx`) in
// projectPackageForGroup — the read lands in Default, misses, the projection
// returns nil, and every group activation in a dedicated namespace declines on
// the runner with "directive carries no package" (observed live: a local-jack
// control plane's collection-group activation loop).
func TestGroupPackageProjectionReadsInTheActivationsNamespace(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "grp-ns-pkg",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "trig", Kind: types.NodeKindTrigger, Type: "test.trig"},
			{Name: "member", Kind: types.NodeKindAction, Type: "test.member"},
		},
		Connections: types.Connections{
			"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "member", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{
			Name:           "g",
			Members:        []string{"trig", "member"},
			RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
		}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil || len(units) != 1 {
		t.Fatalf("DeriveEntryActivations: units=%d err=%v", len(units), err)
	}

	reg := &nsScopedRegistryStub{record: backend.WorkflowRecord{
		ID: "wf-ns-pkg", Key: "local-jack/grp-ns-pkg@v1", Namespace: "local-jack",
		Name: "grp-ns-pkg", Version: "v1", DefinitionHash: "sha256:test",
		Definition: def, Graph: g,
	}}
	act := engine.EntryActivation{
		Namespace: "local-jack", WorkflowID: "wf-ns-pkg", WorkflowVersion: "v1",
		EntryUnitID: units[0].EntryUnitID, NodeType: units[0].NodeType,
		PackageHash: units[0].PackageHash, Selector: units[0].Selector,
		Requirements: units[0].Requirements, Desired: true,
	}

	// context.Background() without a namespace — the control-plane reconcile
	// loop's context, which is what production actually passes in.
	r := &EntryActivationReconciler{cfg: EntryActivationReconcilerConfig{WorkflowRegistry: reg}}
	pkg := r.projectPackageForGroup(context.Background(), &act)

	if len(reg.reads) != 1 || reg.reads[0] != "local-jack" {
		t.Fatalf("registry reads = %v, want exactly one read in local-jack (the activation's namespace)", reg.reads)
	}
	if pkg == nil {
		t.Fatal("package projection returned nil; the runner would decline with 'directive carries no package'")
	}
}
