package runner

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// declaration builds a declaration-shaped binding for this fixture's supply.
func (f *wasmActivationFixture) declaration(workflow, nodeName string) engine.SupplyConsumerBinding {
	return engine.SupplyConsumerBinding{
		SupplyNode:   f.supplyName,
		WorkflowName: workflow,
		NodeName:     nodeName,
		DigestExpr:   "${{ $supplies.ptr.decode.digest }}",
	}
}

// TestActivationRecordsDeclaration: a declaration must land in the process-wide
// table Execute reads. Without it the executing node finds nothing declared, the
// §4.3.1 guard never engages, and the module runs on the legacy globals path
// against no rules.
func TestActivationRecordsDeclaration(t *testing.T) {
	f := newWasmActivationFixture(t)
	const workflow, nodeName = "TestActivationRecordsDeclaration-collect", "decode"
	t.Cleanup(func() {
		node.UndeclareWasmSupplyConsumers(workflow, nodeName, []string{f.supplyName})
	})

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.declaration(workflow, nodeName)},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if fh.gotInput == nil {
		t.Fatal("the trigger handler was never activated; this test would be asserting " +
			"the declaration table for the wrong reason")
	}
	got := node.WasmSupplyDeclarations(workflow, nodeName)
	if !reflect.DeepEqual(got, []string{f.supplyName}) {
		t.Fatalf("declarations for (%s,%s) = %v, want [%s]", workflow, nodeName, got, f.supplyName)
	}
}

// TestDeactivateDropsDeclaration: the mirror. A stale declaration would make a
// node that no longer runs here keep demanding a supply -- and worse, a
// redeployment that removes the supply would fail-close a node that is fine.
func TestDeactivateDropsDeclaration(t *testing.T) {
	f := newWasmActivationFixture(t)
	const workflow, nodeName = "TestDeactivateDropsDeclaration-collect", "decode"
	t.Cleanup(func() {
		node.UndeclareWasmSupplyConsumers(workflow, nodeName, []string{f.supplyName})
	})

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.declaration(workflow, nodeName)},
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := h.Deactivate(protocol.DeactivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig",
	}); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if got := node.WasmSupplyDeclarations(workflow, nodeName); got != nil {
		t.Fatalf("declarations survived deactivation: %v", got)
	}
}

// TestGenerationUpgradeKeepsIdenticalDeclaration mirrors
// TestGenerationUpgradeKeepsIdenticalBindingRegistered for the declaration
// shape. A generation upgrade re-sends the same bindings; "undeclare everything
// old then declare everything new" would, without reference counting, leave a
// window in which Execute sees nothing declared -- and with §4.3.1 in place that
// window drops messages.
func TestGenerationUpgradeKeepsIdenticalDeclaration(t *testing.T) {
	f := newWasmActivationFixture(t)
	const workflow, nodeName = "TestGenerationUpgradeKeepsIdenticalDeclaration-collect", "decode"
	t.Cleanup(func() {
		node.UndeclareWasmSupplyConsumers(workflow, nodeName, []string{f.supplyName})
	})

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	base := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake",
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.declaration(workflow, nodeName)},
	}
	base.Generation = 1
	if err := h.Activate(context.Background(), base); err != nil {
		t.Fatalf("activate gen 1: %v", err)
	}
	base.Generation = 2
	if err := h.Activate(context.Background(), base); err != nil {
		t.Fatalf("activate gen 2: %v", err)
	}
	got := node.WasmSupplyDeclarations(workflow, nodeName)
	if !reflect.DeepEqual(got, []string{f.supplyName}) {
		t.Fatalf("after a generation upgrade the declaration is %v, want [%s]",
			got, f.supplyName)
	}
}

// TestGenerationUpgradeThenDeactivateClearsDeclaration is the mutation-proof
// regression for the refcount leak: a generation upgrade that re-sends an
// IDENTICAL declaration binding must still let a single Deactivate bring the
// declaration's refcount to zero, not just decrement it once off an inflated
// count.
//
// Trace of the bug this pins: gen1 Activate declares -> count 1. gen2 Activate
// (same bindings) declares again -> count 2; if storeSubscription only
// unregisters the DIFFERENCE between old and next bindings, that difference is
// empty (identical bindings), so nothing is undeclared and the count stays at
// 2. Deactivate then undeclares once -> count 1, never 0. The declaration is
// stranded, and node/internal/code/script's execution-time guard keeps
// fail-closing messages for a node that is no longer activated here.
func TestGenerationUpgradeThenDeactivateClearsDeclaration(t *testing.T) {
	f := newWasmActivationFixture(t)
	const workflow, nodeName = "TestGenerationUpgradeThenDeactivateClearsDeclaration-collect", "decode"
	t.Cleanup(func() {
		node.UndeclareWasmSupplyConsumers(workflow, nodeName, []string{f.supplyName})
	})

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake",
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.declaration(workflow, nodeName)},
	}
	d.Generation = 1
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("activate gen 1: %v", err)
	}
	d.Generation = 2
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("activate gen 2: %v", err)
	}
	if err := h.Deactivate(protocol.DeactivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig",
	}); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if got := node.WasmSupplyDeclarations(workflow, nodeName); got != nil {
		t.Fatalf("declaration survived deactivation after a generation upgrade: %v "+
			"-- the refcount was stranded above zero", got)
	}
}

// TestLegacyDigestBindingStillCompilesAtActivation: an upgraded runner must keep
// serving a control plane that has not been upgraded. Dropping the legacy branch
// would make every pre-upgrade activation register nothing.
func TestLegacyDigestBindingStillCompilesAtActivation(t *testing.T) {
	f := newWasmActivationFixture(t)
	cleanupBindings(t, f.binding())
	obs := observeDefaultRegistry(t)

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: f.supplyName}},
		SupplyConsumers: []engine.SupplyConsumerBinding{f.binding()},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := obs.get(f.supplyName); got != 1 {
		t.Fatalf("consumers for %q = %d, want 1; the legacy digest path regressed",
			f.supplyName, got)
	}
}
