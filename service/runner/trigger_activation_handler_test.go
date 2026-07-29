package runner

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// fakeTriggerSubscription records whether Close was invoked.
type fakeTriggerSubscription struct {
	closed bool
}

func (f *fakeTriggerSubscription) Close(ctx context.Context) error {
	f.closed = true
	return nil
}

// fakeTriggerHandler records the last TriggerActivateInput it received and
// returns a fake subscription.
type fakeTriggerHandler struct {
	gotInput *types.TriggerActivateInput
	sub      *fakeTriggerSubscription
}

func (f *fakeTriggerHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "fake"}
}

func (f *fakeTriggerHandler) Activate(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	f.gotInput = input
	f.sub = &fakeTriggerSubscription{}
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
