package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
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

	err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
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
	if err := g.Admit(context.Background(), "wf", nil); err != nil {
		t.Fatalf("Admit(nil): %v", err)
	}
	if f.calls.Load() != 0 {
		t.Fatal("no requirements must mean no fetch")
	}
}

// --- Fix Round 1: rejected content must keep declining ---

// rejectingConsumer rejects OnSupplyChanged until told otherwise.
type rejectingConsumer struct {
	mu     sync.Mutex
	reject bool
}

func (c *rejectingConsumer) setReject(v bool) {
	c.mu.Lock()
	c.reject = v
	c.mu.Unlock()
}

func (c *rejectingConsumer) OnSupplyChanged(_ context.Context, _ supply.Snapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reject {
		return errors.New("configure returned -1")
	}
	return nil
}

// The core regression: a rejecting consumer must cause Admit to decline on
// REPEATED calls, not just the first. Before the fix, the second Admit found
// the cached snapshot and skipped the gate entirely.
func TestAdmitDeclinesRepeatedlyWhenConsumerRejects(t *testing.T) {
	rc := &rejectingConsumer{reject: true}
	reg := supply.NewRegistry()
	reg.RegisterConsumer("rules", "wasm/clean", rc)

	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"rules":[]}`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	reqs := []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}

	// First Admit: fetches, Apply notifies consumer which rejects → decline.
	err := g.Admit(context.Background(), "wf", reqs)
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("first Admit: err = %v, want *NotReadyError", err)
	}

	// Second Admit: must STILL decline (the regression was: it admitted here).
	err = g.Admit(context.Background(), "wf", reqs)
	if !errors.As(err, &nr) {
		t.Fatalf("second Admit: err = %v, want *NotReadyError (the regression)", err)
	}

	// Third Admit for good measure: still declining.
	err = g.Admit(context.Background(), "wf", reqs)
	if !errors.As(err, &nr) {
		t.Fatalf("third Admit: err = %v, want *NotReadyError", err)
	}
}

// A supply with no registered consumer must admit normally when content is
// present. This is the common case: content is read only through $supplies
// expressions, no Consumer interface is registered.
func TestAdmitPassesWithNoConsumer(t *testing.T) {
	reg := supply.NewRegistry()
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`ok`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	reqs := []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}

	// First call fetches and admits (no consumer to reject).
	if err := g.Admit(context.Background(), "wf", reqs); err != nil {
		t.Fatalf("Admit with no consumer: %v", err)
	}

	// Second call: IsReady returns true, no fetch needed.
	if err := g.Admit(context.Background(), "wf", reqs); err != nil {
		t.Fatalf("second Admit with no consumer: %v", err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("fetched %d times, want 1 (second call should skip)", n)
	}
}

// Once a consumer starts accepting (after previously rejecting), Admit must
// admit and stop re-fetching.
func TestAdmitAdmitsAfterConsumerStartsAccepting(t *testing.T) {
	rc := &rejectingConsumer{reject: true}
	reg := supply.NewRegistry()
	reg.RegisterConsumer("rules", "wasm/clean", rc)

	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"rules":[]}`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	reqs := []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}

	// First Admit: consumer rejects → decline.
	err := g.Admit(context.Background(), "wf", reqs)
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("first Admit: err = %v, want *NotReadyError", err)
	}

	// Consumer is now fixed (starts accepting).
	rc.setReject(false)

	// Second Admit: IsReady is false (still rejected from last round), so it
	// re-fetches, Apply re-notifies (accepted was false), consumer accepts this
	// time → admits.
	if err := g.Admit(context.Background(), "wf", reqs); err != nil {
		t.Fatalf("Admit after consumer fixed: %v", err)
	}

	// Third Admit: IsReady is now true → no fetch needed.
	prevCalls := f.calls.Load()
	if err := g.Admit(context.Background(), "wf", reqs); err != nil {
		t.Fatalf("third Admit: %v", err)
	}
	if f.calls.Load() != prevCalls {
		t.Fatal("third Admit should not re-fetch (supply is now ready)")
	}
}

// After a rejecting consumer is unregistered, the supply becomes ready (no
// consumer left to reject) and Admit must admit without re-fetching.
func TestAdmitAdmitsAfterRejectingConsumerUnregistered(t *testing.T) {
	rc := &rejectingConsumer{reject: true}
	reg := supply.NewRegistry()
	reg.RegisterConsumer("rules", "wasm/clean", rc)

	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"rules":[]}`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	reqs := []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}

	// First Admit: consumer rejects → decline.
	err := g.Admit(context.Background(), "wf", reqs)
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("first Admit: err = %v, want *NotReadyError", err)
	}

	// Unregister the consumer: no consumers left, supply should become ready.
	reg.UnregisterConsumer("rules", "wasm/clean")

	// Second Admit: IsReady returns true (unregister marked it accepted) → admits
	// without fetching.
	prevCalls := f.calls.Load()
	if err := g.Admit(context.Background(), "wf", reqs); err != nil {
		t.Fatalf("Admit after unregister: %v", err)
	}
	if f.calls.Load() != prevCalls {
		t.Fatal("should not re-fetch after unregister made it ready")
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
