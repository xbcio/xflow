package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// fakeTriggerSubscription records whether Close was invoked.
type fakeTriggerSubscription struct {
	closed     bool
	closeCount int
}

func (f *fakeTriggerSubscription) Close(ctx context.Context) error {
	f.closed = true
	f.closeCount++
	return nil
}

// fakeTriggerHandler records the last TriggerActivateInput it received and
// returns a fake subscription.
type fakeTriggerHandler struct {
	gotInput *types.TriggerActivateInput
	sub      *fakeTriggerSubscription

	// activateErr, when non-nil, is returned by Activate instead of creating
	// a subscription. Used to test error paths.
	activateErr error

	// allSubs records every subscription returned by successive Activate calls
	// in order, enabling stale-close assertions.
	allSubs []*fakeTriggerSubscription
}

func (f *fakeTriggerHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "fake"}
}

func (f *fakeTriggerHandler) Activate(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	f.gotInput = input
	if f.activateErr != nil {
		return nil, f.activateErr
	}
	f.sub = &fakeTriggerSubscription{}
	f.allSubs = append(f.allSubs, f.sub)
	return f.sub, nil
}

// fakeLookup is a TriggerHandlerLookup backed by a map.
type fakeLookup struct {
	handlers map[string]types.TriggerHandler
}

func (l fakeLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	h, ok := l.handlers[nodeType]
	return h, ok
}

func TestTriggerActivationHandler_ActivateStampsSeedRuntimeAndParams(t *testing.T) {
	fh := &fakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}}
	h := NewTriggerActivationHandler("https://control.internal", "secret-bearer", lookup)

	d := protocol.ActivateDirective{
		Namespace:       "ns1",
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "t1",
		NodeType:        "fake",
		Params:          map[string]any{"topic": "orders", "brokers": "b:9092"},
		Generation:      9,
	}

	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("Activate returned error: %v", err)
	}

	if fh.gotInput == nil {
		t.Fatal("handler.Activate was not called")
	}
	if fh.gotInput.NodeName != "t1" {
		t.Errorf("NodeName = %q, want %q", fh.gotInput.NodeName, "t1")
	}
	if fh.gotInput.WorkflowID != types.WorkflowID("wf1") {
		t.Errorf("WorkflowID = %q, want %q", fh.gotInput.WorkflowID, "wf1")
	}

	p := fh.gotInput.Params
	// seed-selecting keys
	if p["entry_seed"] != true {
		t.Errorf("params[entry_seed] = %v, want true", p["entry_seed"])
	}
	if p["entry_unit_id"] != "t1" {
		t.Errorf("params[entry_unit_id] = %v, want %q", p["entry_unit_id"], "t1")
	}
	if p["workflow_version"] != "v1" {
		t.Errorf("params[workflow_version] = %v, want %q", p["workflow_version"], "v1")
	}
	// original params preserved
	if p["topic"] != "orders" {
		t.Errorf("params[topic] = %v, want %q", p["topic"], "orders")
	}
	if p["brokers"] != "b:9092" {
		t.Errorf("params[brokers] = %v, want %q", p["brokers"], "b:9092")
	}

	// original directive params must not be mutated
	if _, ok := d.Params["entry_seed"]; ok {
		t.Error("directive.Params was mutated (entry_seed leaked back)")
	}

	// Runtime must be a generation-stamped HTTPEntrySeedRuntime.
	rt, ok := fh.gotInput.Runtime.(*node.HTTPEntrySeedRuntime)
	if !ok {
		t.Fatalf("Runtime type = %T, want *node.HTTPEntrySeedRuntime", fh.gotInput.Runtime)
	}
	if rt.Generation != 9 {
		t.Errorf("Runtime.Generation = %d, want 9", rt.Generation)
	}
	if rt.BaseURL != "https://control.internal" {
		t.Errorf("Runtime.BaseURL = %q, want %q", rt.BaseURL, "https://control.internal")
	}
	if rt.Token != "secret-bearer" {
		t.Errorf("Runtime.Token = %q, want %q", rt.Token, "secret-bearer")
	}

	// Deactivate closes the stored subscription.
	dd := protocol.DeactivateDirective{
		Namespace:   "ns1",
		WorkflowID:  "wf1",
		EntryUnitID: "t1",
		Generation:  9,
	}
	if err := h.Deactivate(dd); err != nil {
		t.Fatalf("Deactivate returned error: %v", err)
	}
	if !fh.sub.closed {
		t.Error("subscription Close was not called on Deactivate")
	}
}

func TestTriggerActivationHandler_ActivateUnknownNodeTypeFailsClosed(t *testing.T) {
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{}}
	h := NewTriggerActivationHandler("https://control.internal", "secret", lookup)

	err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID:  "wf1",
		EntryUnitID: "t1",
		NodeType:    "does-not-exist",
		Generation:  1,
	})
	if err == nil {
		t.Fatal("Activate with unknown NodeType should return an error, got nil")
	}
}

func TestTriggerActivationHandler_DeactivateUnknownIsNoop(t *testing.T) {
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{}}
	h := NewTriggerActivationHandler("https://control.internal", "secret", lookup)

	// Deactivating something never activated must be a safe no-op.
	err := h.Deactivate(protocol.DeactivateDirective{
		WorkflowID:  "wf-unknown",
		EntryUnitID: "t-unknown",
		Generation:  1,
	})
	if err != nil {
		t.Fatalf("Deactivate of unknown activation should be no-op, got error: %v", err)
	}
}

func TestTriggerActivationHandler_StaleCloseOnGenerationUpgrade(t *testing.T) {
	fh := &fakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}}
	h := NewTriggerActivationHandler("https://control.internal", "token", lookup)

	d1 := protocol.ActivateDirective{
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "t1",
		NodeType:        "fake",
		Generation:      1,
	}
	if err := h.Activate(context.Background(), d1); err != nil {
		t.Fatalf("first Activate: %v", err)
	}
	firstSub := fh.allSubs[0]

	// Second Activate with a new generation for the same identity.
	d2 := d1
	d2.Generation = 2
	if err := h.Activate(context.Background(), d2); err != nil {
		t.Fatalf("second Activate: %v", err)
	}
	secondSub := fh.allSubs[1]

	// The first subscription must have been closed exactly once.
	if firstSub.closeCount != 1 {
		t.Errorf("first subscription closeCount = %d, want 1", firstSub.closeCount)
	}
	// The handler must store the second subscription.
	id := activationID{WorkflowID: "wf1", EntryUnitID: "t1"}
	h.mu.Lock()
	stored := h.subs[id]
	h.mu.Unlock()
	if stored.sub != secondSub {
		t.Error("handler.subs stores stale subscription instead of the new one")
	}
	// Second sub must not have been closed.
	if secondSub.closed {
		t.Error("second subscription should not be closed")
	}
}

func TestTriggerActivationHandler_StaleCloseNotCalledOnActivateError(t *testing.T) {
	fh := &fakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}}
	h := NewTriggerActivationHandler("https://control.internal", "token", lookup)

	// First Activate succeeds.
	d := protocol.ActivateDirective{
		WorkflowID:      "wf1",
		WorkflowVersion: "v1",
		EntryUnitID:     "t1",
		NodeType:        "fake",
		Generation:      1,
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("first Activate: %v", err)
	}
	firstSub := fh.allSubs[0]

	// Make the second Activate fail.
	fh.activateErr = errors.New("simulated handler failure")
	d.Generation = 2
	if err := h.Activate(context.Background(), d); err == nil {
		t.Fatal("second Activate should have returned an error")
	}

	// The first subscription must NOT have been closed because the new
	// Activate failed before reaching the stale-close logic.
	if firstSub.closed {
		t.Error("first subscription was closed despite second Activate failing")
	}
	// The stored subscription must still be the first one.
	id := activationID{WorkflowID: "wf1", EntryUnitID: "t1"}
	h.mu.Lock()
	stored := h.subs[id]
	h.mu.Unlock()
	if stored.sub != firstSub {
		t.Error("handler.subs should still hold the first subscription after failed second Activate")
	}
}

// groupExecFakeTriggerHandler is a fake trigger handler used only to capture
// the TriggerActivateInput a group activation constructs, so the test can
// assert on NodeName/Params/Runtime capability without a real Kafka
// consumer.
type groupExecFakeTriggerHandler struct {
	gotInput *types.TriggerActivateInput
}

func (f *groupExecFakeTriggerHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.grouplocal.trigger", Kind: types.NodeKindTrigger}
}
func (f *groupExecFakeTriggerHandler) Activate(_ context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	f.gotInput = input
	return &fakeTriggerSubscription{}, nil
}

// TestTriggerActivationHandler_GroupActivationResolvesEntryMemberAndInstallsGroupExecRuntime
// verifies the group-mode branch: it must (1) look up the trigger handler by
// the PACKAGE's entry node type, not by the synthetic "xflow.group" NodeType
// (which has no registered handler — that IS the bug this plan closes), and
// (2) install a Runtime exposing types.GroupExecRuntime so the trigger can
// run the group locally per batch.
func TestTriggerActivationHandler_GroupActivationResolvesEntryMemberAndInstallsGroupExecRuntime(t *testing.T) {
	fh := &groupExecFakeTriggerHandler{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{"test.grouplocal.trigger": fh}}

	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.echo", echoHandler{})
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	groupRT := NewGroupRuntime(reg, cache, WithSuspendDisabled())

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "trig",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "trig", Type: "test.grouplocal.trigger", Version: 1, Parameters: map[string]any{"topic": "t"}},
				{Name: "__collector_trig_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"trig": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_trig_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{{CollectorNode: "__collector_trig_main", SrcNode: "trig", Port: "main"}},
	}

	h := NewTriggerActivationHandler("http://control-plane", "tok", lookup, WithGroupRuntime(groupRT))

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "g",
		NodeType: "xflow.group", Generation: 3, PackageHash: "pkg-sha256:v1:x", Package: pkg,
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	if fh.gotInput == nil {
		t.Fatal("group entry's trigger handler Activate was never called — lookup by package entry type failed")
	}
	if fh.gotInput.NodeName != "trig" {
		t.Fatalf("NodeName = %q, want %q (the package's entry node, not the group name)", fh.gotInput.NodeName, "trig")
	}
	if fh.gotInput.Params["topic"] != "t" {
		t.Fatalf("Params = %+v, want the entry node's own Parameters merged in", fh.gotInput.Params)
	}
	if fh.gotInput.Params["entry_unit_id"] != "g" {
		t.Fatalf("Params[entry_unit_id] = %v, want the GROUP name %q (for admission-key construction)", fh.gotInput.Params["entry_unit_id"], "g")
	}
	if _, ok := fh.gotInput.Runtime.(types.GroupExecRuntime); !ok {
		t.Fatalf("Runtime = %T, want an implementation of types.GroupExecRuntime", fh.gotInput.Runtime)
	}
	if _, ok := fh.gotInput.Runtime.(types.EntrySeedRuntime); !ok {
		t.Fatalf("Runtime = %T, want an implementation of types.EntrySeedRuntime", fh.gotInput.Runtime)
	}
}

// TestTriggerActivationHandler_GroupActivationFailsClosedWithoutGroupRuntime
// verifies a runner with no GroupRuntime configured rejects a group
// activation instead of silently no-op'ing or falling through to the
// (nonexistent) "xflow.group" handler lookup.
func TestTriggerActivationHandler_GroupActivationFailsClosedWithoutGroupRuntime(t *testing.T) {
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{}}
	h := NewTriggerActivationHandler("http://control-plane", "tok", lookup) // no WithGroupRuntime

	d := protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "g", NodeType: "xflow.group",
		Package: &graph.SubgraphPackage{EntryNode: "trig", Def: &types.WorkflowDef{Nodes: []types.NodeDef{{Name: "trig", Type: "test.x"}}}},
	}
	if err := h.Activate(context.Background(), d); err == nil {
		t.Fatal("expected fail-closed error when GroupRuntime is not configured")
	}
}

// TestTriggerActivationHandler_GroupActivationFailsClosedWithoutPackage
// verifies a group directive with a nil Package (e.g. the control plane could
// not re-project it) is rejected rather than silently doing nothing.
func TestTriggerActivationHandler_GroupActivationFailsClosedWithoutPackage(t *testing.T) {
	reg := execution.NewRegistry()
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	groupRT := NewGroupRuntime(reg, cache, WithSuspendDisabled())
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{}}
	h := NewTriggerActivationHandler("http://control-plane", "tok", lookup, WithGroupRuntime(groupRT))

	d := protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "g", NodeType: "xflow.group", Package: nil}
	if err := h.Activate(context.Background(), d); err == nil {
		t.Fatal("expected fail-closed error when the directive carries no Package")
	}
}
