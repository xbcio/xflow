package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// stubFetcher serves canned content and counts calls so a test can assert the
// gate fetched exactly once per missing supply.
type stubFetcher struct {
	content map[string][]byte
	err     error
	calls   atomic.Int32
}

func (f *stubFetcher) Fetch(_ context.Context, name string) ([]byte, string, uint64, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, "", 0, f.err
	}
	c, ok := f.content[name]
	if !ok {
		return nil, "", 0, ErrSupplyNotFound
	}
	return c, "sha256:" + name, 7, nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAdmitFetchesMissingContentAndPasses(t *testing.T) {
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"rules":[]}`)}}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())

	err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	// The content must land in the registry under the SUPPLY NODE name, which is
	// what $supplies and the consumer registry key on — not the resource name.
	got, ok := reg.Get("rules")
	if !ok {
		t.Fatal("content must be cached under the supply node name")
	}
	if string(got.Content) != `{"rules":[]}` || got.Revision != 7 {
		t.Fatalf("cached snapshot = %+v", got)
	}
}

// Already-cached content must not trigger a fetch: every activation would
// otherwise re-download the full rule set.
func TestAdmitSkipsFetchWhenCached(t *testing.T) {
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`x`)}}
	reg := supply.NewRegistry()
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`x`), Hash: "h", Revision: 1,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	g := NewSupplyGate(f, reg, quietLogger())
	if err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("fetched %d times, want 0 (already cached)", n)
	}
}

// THE core gate assertion: require_ready + no content = refuse. Nothing may be
// registered, so no Kafka offset ever advances.
func TestAdmitRefusesWhenRequiredContentAbsent(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{content: map[string][]byte{}}, supply.NewRegistry(), quietLogger())
	err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	})
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("Admit err = %v, want *NotReadyError", err)
	}
	if len(nr.Missing) != 1 || nr.Missing[0] != "rules" {
		t.Fatalf("missing = %#v, want [rules]", nr.Missing)
	}
}

// A fetch transport error is the same decision as a 404: this runner has no
// content, so it must not take the activation.
func TestAdmitRefusesOnFetchError(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{err: errors.New("connection refused")},
		supply.NewRegistry(), quietLogger())
	err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	})
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("Admit err = %v, want *NotReadyError", err)
	}
}

// require_ready:false passes through with no content, which is the documented
// (and metric-visible) degraded mode.
func TestAdmitPassesWhenNotRequired(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{content: map[string][]byte{}}, supply.NewRegistry(), quietLogger())
	if err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "hints", Resource: "hints", RequireReady: false},
	}); err != nil {
		t.Fatalf("Admit with require_ready=false must pass: %v", err)
	}
}

// A mixed set must report every missing REQUIRED supply, not just the first:
// an operator fixing them one at a time otherwise needs one reconcile round per
// supply to discover the next.
func TestAdmitReportsAllMissingRequired(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{content: map[string][]byte{"ok": []byte(`1`)}},
		supply.NewRegistry(), quietLogger())
	err := g.Admit(context.Background(), []engine.SupplyRequirement{
		{Node: "a", Resource: "missing-a", RequireReady: true},
		{Node: "b", Resource: "ok", RequireReady: true},
		{Node: "c", Resource: "missing-c", RequireReady: true},
		{Node: "d", Resource: "missing-d", RequireReady: false},
	})
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("Admit err = %v, want *NotReadyError", err)
	}
	if len(nr.Missing) != 2 || nr.Missing[0] != "a" || nr.Missing[1] != "c" {
		t.Fatalf("missing = %#v, want [a c] sorted, excluding the not-required d", nr.Missing)
	}
}

func TestAdmitNoRequirementsIsNoop(t *testing.T) {
	f := &stubFetcher{}
	g := NewSupplyGate(f, supply.NewRegistry(), quietLogger())
	if err := g.Admit(context.Background(), nil); err != nil {
		t.Fatalf("Admit(nil): %v", err)
	}
	if f.calls.Load() != 0 {
		t.Fatal("no requirements must mean no fetch")
	}
}

// --- gate wired into the activation handler ---

// gateRecordingTrigger records whether Activate was ever called. Used to verify
// the gate runs before the trigger subscription starts.
type gateRecordingTrigger struct{ activated atomic.Bool }

func (r *gateRecordingTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "gate-test"}
}

func (r *gateRecordingTrigger) Activate(_ context.Context, _ *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	r.activated.Store(true)
	return &fakeTriggerSubscription{}, nil
}

// The gate must run BEFORE the trigger subscription starts. If it ran after,
// the Kafka consumer would already be reading and committing offsets.
func TestActivateIsRefusedBeforeSubscribing(t *testing.T) {
	rec := &gateRecordingTrigger{}
	lookup := fakeLookup{handlers: map[string]types.TriggerHandler{
		"kafka.source": rec,
	}}
	h := NewTriggerActivationHandler("http://server", "tok", lookup,
		WithSupplyGate(NewSupplyGate(&stubFetcher{content: map[string][]byte{}},
			supply.NewRegistry(), quietLogger())))

	err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf", EntryUnitID: "src", NodeType: "kafka.source",
		Supplies: []engine.SupplyRequirement{
			{Node: "rules", Resource: "shared-rules", RequireReady: true},
		},
	})
	if err == nil {
		t.Fatal("Activate must fail when a required supply is missing")
	}
	if rec.activated.Load() {
		t.Fatal("the trigger subscription must NOT start: it would commit Kafka offsets for messages nothing can process")
	}
}
