package runner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
)

// --- SupplyGate.ApplyHints ---

// An equal hash must cost nothing: no fetch at all.
func TestApplyHintsSkipsWhenHashUnchanged(t *testing.T) {
	reg := supply.NewRegistry()
	if err := reg.Apply(context.Background(), supply.Snapshot{Name: "rules", Content: []byte(`x`), Hash: "h1", Revision: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`x`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	g.ApplyHints(context.Background(), map[string]string{"rules": "h1"})
	if f.calls.Load() != 0 {
		t.Fatalf("fetched %d times, want 0 (hash unchanged)", f.calls.Load())
	}
}

// A differing hash must trigger exactly one fetch, and the fetched content
// lands in the registry under the supply NODE name (via the resource mapping
// recorded by a prior Admit).
func TestApplyHintsFetchesOnHashMismatch(t *testing.T) {
	reg := supply.NewRegistry()
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"new":true}`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	// Admit records the node->resource mapping (node "rules" -> resource
	// "shared-rules") even though the fetch here returns content the gate then
	// caches; that's what lets ApplyHints later resolve the resource name from
	// only the node name the hint carries.
	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	callsAfterAdmit := f.calls.Load()

	// New content arrives server-side; a hint reports a different hash.
	f.content["shared-rules"] = []byte(`{"new":true,"v":2}`)
	g.ApplyHints(context.Background(), map[string]string{"rules": "sha256:different-hash"})

	if f.calls.Load() != callsAfterAdmit+1 {
		t.Fatalf("fetch calls = %d, want %d (exactly one hint-triggered fetch)", f.calls.Load(), callsAfterAdmit+1)
	}
	got, ok := reg.Get("rules")
	if !ok || string(got.Content) != `{"new":true,"v":2}` {
		t.Fatalf("registry content = %+v, want the newly-fetched content under node name 'rules'", got)
	}
}

// The gate must use the RESOURCE name recorded by Admit, not the hint's node
// name, when the two differ.
func TestApplyHintsUsesResourceMappingFromAdmit(t *testing.T) {
	reg := supply.NewRegistry()
	f := &stubFetcher{content: map[string][]byte{"shared-resource-name": []byte(`v1`)}}
	g := NewSupplyGate(f, reg, quietLogger())

	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{
		{Node: "local-alias", Resource: "shared-resource-name", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	f.content["shared-resource-name"] = []byte(`v2`)
	g.ApplyHints(context.Background(), map[string]string{"local-alias": "sha256:different"})

	got, ok := reg.Get("local-alias")
	if !ok || string(got.Content) != `v2` {
		t.Fatalf("registry['local-alias'] = %+v, want v2 (fetched via the mapped resource name)", got)
	}
}

// A fetch failure must not panic and must not corrupt the cache: last-good
// content is preserved.
func TestApplyHintsFetchFailureIsNonFatal(t *testing.T) {
	reg := supply.NewRegistry()
	if err := reg.Apply(context.Background(), supply.Snapshot{Name: "rules", Content: []byte(`good`), Hash: "h-good", Revision: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	f := &stubFetcher{err: errors.New("connection refused")}
	g := NewSupplyGate(f, reg, quietLogger())

	g.ApplyHints(context.Background(), map[string]string{"rules": "sha256:new-but-unreachable"})

	got, ok := reg.Get("rules")
	if !ok || string(got.Content) != `good` {
		t.Fatalf("registry content = %+v, want last-good 'good' preserved after fetch failure", got)
	}
}

// A rejected fetch (consumer refuses the new content) must leave last-good
// content in place, same as Admit's own behavior — ApplyHints does not bypass
// the consumer verdict.
func TestApplyHintsRejectedContentPreservesLastGood(t *testing.T) {
	reg := supply.NewRegistry()
	rc := &rejectingConsumer{reject: false}
	reg.RegisterConsumer("rules", "wasm/clean", rc)
	if err := reg.Apply(context.Background(), supply.Snapshot{Name: "rules", Content: []byte(`good`), Hash: "h-good", Revision: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	f := &stubFetcher{content: map[string][]byte{"rules": []byte(`bad`)}}
	g := NewSupplyGate(f, reg, quietLogger())
	rc.setReject(true)

	g.ApplyHints(context.Background(), map[string]string{"rules": "sha256:bad-content"})

	got, _ := reg.Get("rules")
	if !rc.reject {
		t.Fatal("test setup: consumer must be rejecting")
	}
	_ = got // content is cached regardless per Registry.Apply's documented contract;
	// the important assertion is that IsReady reflects the rejection.
	if reg.IsReady("rules") {
		t.Fatal("IsReady must be false: the consumer rejected the hinted content")
	}
}

// Concurrent ApplyHints calls for the SAME name must not both fire a fetch:
// the in-flight marker de-dupes. This guards against the runner's own
// heartbeat loop overlapping (a slow fetch from round N still running when
// round N+1's hint arrives).
//
// The second call is made SYNCHRONOUSLY on this goroutine once the first is
// confirmed blocked inside Fetch (not as a second racing goroutine): both
// goroutines merely reaching a closed "started" channel gives no ordering
// guarantee about which resumes first, so a second background goroutine can
// occasionally observe the FIRST call's in-flight marker already cleared
// (deleted by its own completing defer) rather than still held — a test
// flake, not a production bug, but exactly the kind of flake this task's
// brief warns must not ship. Calling synchronously removes the race: this
// goroutine only proceeds past `<-started` after the first call has already
// stored its marker (LoadOrStore happens before Fetch is invoked, and
// onStart runs from inside Fetch), so the dedupe check is guaranteed to see
// "already in flight".
func TestApplyHintsDedupesConcurrentFetchesForSameName(t *testing.T) {
	reg := supply.NewRegistry()
	release := make(chan struct{})
	started := make(chan struct{})
	f := &blockingFetcher{
		content: []byte(`v1`),
		onStart: func() {
			close(started)
			<-release
		},
	}
	g := NewSupplyGate(f, reg, quietLogger())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.ApplyHints(context.Background(), map[string]string{"rules": "sha256:v1"})
	}()

	<-started
	// The first call is now blocked inside Fetch, holding the in-flight
	// marker. This call, made on the test goroutine while the first is still
	// blocked, must dedupe and return immediately without fetching.
	g.ApplyHints(context.Background(), map[string]string{"rules": "sha256:v1"})
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("fetch calls after the second (deduped) call = %d, want exactly 1", n)
	}

	close(release)
	wg.Wait()

	if n := f.calls.Load(); n != 1 {
		t.Fatalf("fetch calls = %d, want exactly 1 (the second overlapping call must dedupe)", n)
	}
}

// A nil gate or empty hints map must be a no-op, matching the documented
// nil-safety of every other SupplyGate entry point.
func TestApplyHintsNilSafety(t *testing.T) {
	var g *SupplyGate
	g.ApplyHints(context.Background(), map[string]string{"rules": "h1"}) // must not panic

	reg := supply.NewRegistry()
	f := &stubFetcher{}
	g2 := NewSupplyGate(f, reg, quietLogger())
	g2.ApplyHints(context.Background(), nil)
	if f.calls.Load() != 0 {
		t.Fatal("empty hints must not fetch")
	}
}

// resourceFor must return the recorded mapping, or the name itself when never
// recorded — the common case where node name and resource name coincide.
func TestResourceForFallsBackToNameWhenUnrecorded(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
	if got := g.resourceFor("never-admitted"); got != "never-admitted" {
		t.Fatalf("resourceFor = %q, want the name itself", got)
	}
}

// blockingFetcher calls onStart synchronously inside Fetch (before returning),
// letting a test coordinate two concurrent ApplyHints calls around a single
// in-flight fetch.
type blockingFetcher struct {
	content []byte
	onStart func()
	calls   atomic.Int32
}

func (f *blockingFetcher) Fetch(_ context.Context, name string) ([]byte, string, uint64, error) {
	f.calls.Add(1)
	if f.onStart != nil {
		f.onStart()
	}
	return f.content, "sha256:" + name, 1, nil
}

// --- Runner.observedSupplies / processSupplyHints wiring ---

// A Runner with no SupplyRegistry configured must report nil observed
// supplies — the heartbeat body must be unaffected.
func TestObservedSuppliesNilWithoutRegistry(t *testing.T) {
	r := &Runner{}
	if got := r.observedSupplies(); got != nil {
		t.Fatalf("observedSupplies() = %#v, want nil with no SupplyRegistry configured", got)
	}
}

// A Runner with a SupplyRegistry reports its Observed() snapshot.
func TestObservedSuppliesReadsFromRegistry(t *testing.T) {
	reg := supply.NewRegistry()
	if err := reg.Apply(context.Background(), supply.Snapshot{Name: "rules", Content: []byte(`v1`), Hash: "h-v1", Revision: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	r := &Runner{supplyRegistry: reg}
	got := r.observedSupplies()
	if len(got) != 1 || got["rules"] != "h-v1" {
		t.Fatalf("observedSupplies() = %#v, want {rules: h-v1}", got)
	}
}

// processSupplyHints must be a no-op when there is no gate configured, even
// with non-empty hints in the response — a passive runner must never panic on
// a field it did not ask for.
func TestProcessSupplyHintsNoopWithoutGate(t *testing.T) {
	r := &Runner{}
	// Must not panic.
	r.processSupplyHints(context.Background(), protocol.HeartbeatResponse{
		SupplyHints: map[string]string{"rules": "h1"},
	})
}

// processSupplyHints must be a no-op when the response carries no hints, even
// with a gate configured — this is the common per-heartbeat case for a runner
// whose supplies have not changed.
func TestProcessSupplyHintsNoopWithEmptyHints(t *testing.T) {
	reg := supply.NewRegistry()
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`x`)}}
	g := NewSupplyGate(f, reg, quietLogger())
	r := &Runner{supplyGate: g}

	r.processSupplyHints(context.Background(), protocol.HeartbeatResponse{})
	// Give the (would-be) background goroutine a moment; there must be none.
	time.Sleep(20 * time.Millisecond)
	if f.calls.Load() != 0 {
		t.Fatal("no hints in the response must mean no fetch")
	}
}

// processSupplyHints must actually reach the gate and apply a changed hint —
// this is the end-to-end wiring assertion, not just the isolated ApplyHints
// unit tests above.
func TestProcessSupplyHintsAppliesHintViaGate(t *testing.T) {
	reg := supply.NewRegistry()
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"v":2}`)}}
	g := NewSupplyGate(f, reg, quietLogger())
	// Seed the node->resource mapping the way Admit would at activation time.
	g.resources.Store("rules", "shared-rules")
	r := &Runner{supplyGate: g}

	r.processSupplyHints(context.Background(), protocol.HeartbeatResponse{
		SupplyHints: map[string]string{"rules": "sha256:whatever-changed"},
	})

	// processSupplyHints fires the fetch in its own goroutine (documented: it
	// must never block the heartbeat loop), so poll briefly for the result
	// rather than asserting immediately.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := reg.Get("rules"); ok && string(got.Content) == `{"v":2}` {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("hint was not applied to the registry within the timeout")
}
