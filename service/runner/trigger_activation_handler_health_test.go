package runner

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// fakeHealthSubscription is a TriggerSubscription that also exposes the
// optional health surface; the activation tracker's repair depends on the
// handler resolving exactly this channel for the directive that installed it.
type fakeHealthSubscription struct {
	done chan struct{}
}

func (s *fakeHealthSubscription) Close(context.Context) error { return nil }

func (s *fakeHealthSubscription) Done() <-chan struct{} { return s.done }

// TestTriggerActivationHandlerSubscriptionDone pins the production wiring
// between the handler's activation key and the tracker's lookup key.
//
// The tracker-side tests use a mock handler, so a mismatch here — different
// identity derivation, a missing map entry, a type assertion that never
// succeeds — would leave the whole repair silently inert while every other
// test stayed green.
func TestTriggerActivationHandlerSubscriptionDone(t *testing.T) {
	h := NewTriggerActivationHandler("", "", nil)
	ctx := context.Background()
	d := protocol.ActivateDirective{WorkflowID: "wf-health", EntryUnitID: "grp-a", Generation: 1}
	id := activationIDFromActivate(d)

	if _, ok := h.SubscriptionDone(d); ok {
		t.Fatal("SubscriptionDone reported health before any subscription was installed")
	}

	sub := &fakeHealthSubscription{done: make(chan struct{})}
	h.storeSubscription(ctx, id, sub, nil)

	got, ok := h.SubscriptionDone(d)
	if !ok {
		t.Fatalf("SubscriptionDone did not find the subscription installed for %+v", id)
	}
	if got != sub.done {
		t.Fatal("SubscriptionDone returned a different channel than the installed subscription exposes")
	}

	other := d
	other.EntryUnitID = "grp-b"
	if _, ok := h.SubscriptionDone(other); ok {
		t.Fatal("SubscriptionDone resolved another activation's entry")
	}

	// A subscription without the optional surface keeps the pre-existing
	// behaviour: the tracker must be told there is no health signal.
	plain := types.CloseFunc(func(context.Context) error { return nil })
	h.storeSubscription(ctx, id, plain, nil)
	if _, ok := h.SubscriptionDone(d); ok {
		t.Fatal("SubscriptionDone reported health for a subscription that does not implement types.SubscriptionHealth")
	}
}
