package wasm

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
)

// TestInstanceGaugeCountsAllEngines pins that the ready-instance gauge reports
// the host's TOTAL resident instances rather than the last engine to report.
//
// The defect this pins: OnInstanceCount was called once per engine with that
// engine's own pool size, and SupplyMetrics.OnInstanceCount Sets a single
// series carrying only a "state" label. With two modules resident the second
// engine's Set overwrote the first's, so the gauge read one pool's width while
// two pools' worth of instances existed. A live run read 8 with 16 resident.
//
// Why not fix it by adding a module label instead: Observer's contract forbids
// it in as many words — implementations "must never use content, hashes, or
// execution IDs as labels". A module is identified here by its sha256, so
// there is no low-cardinality module identity to label with. Reporting the sum
// is what a gauge named _total should have meant anyway.
//
// The assertion is deliberately about the SUM and not about call count: a fix
// that reports per-engine deltas would also be correct, and pinning the
// mechanism rather than the observable would reject it for no reason.
func TestInstanceGaugeCountsAllEngines(t *testing.T) {
	ctx := context.Background()
	obs := &recordingObserver{}
	SetObserver(obs)
	t.Cleanup(func() { SetObserver(nil) })

	// One host with two engines is the live shape: reactorHost keys engines by
	// module sha256, and SAS resolves two modules (decode and clean). Two
	// separate hosts would not reproduce it — the overwrite happens because
	// both pools report into one process-wide series.
	h := newReactorHost()

	cfg, err := json.Marshal(ruleConfig([2]string{"r0", `request.uri startsWith "/api/"`}))
	if err != nil {
		t.Fatalf("marshal cfg: %v", err)
	}

	const width = 3
	// Two DISTINCT module blobs, so engineFor's sha256 dedup yields two engines
	// rather than handing back the same one twice. A custom section is skipped
	// by any conformant loader, so the second module differs only in its hash.
	// The payload must be non-empty: an empty one truncates the section and
	// wazero rejects the module while reading the section name.
	secondModule := appendCustomSection(reactorWasm, "xflow-test-engine-b", []byte{0x01})
	for i, mod := range [][]byte{reactorWasm, secondModule} {
		e, err := h.engineFor(ctx, mod)
		if err != nil {
			t.Fatalf("engineFor[%d]: %v", i, err)
		}
		if err := e.swapConfig(ctx, cfg, width, 1); err != nil {
			t.Fatalf("swapConfig[%d]: %v", i, err)
		}
	}

	// Last "ready" observation is what a scrape would see, since the metric is a
	// gauge: each report replaces the previous value.
	got, ok := lastInstanceCount(obs, "ready")
	if !ok {
		t.Fatal("no ready-instance observation; the gauge would report nothing at all")
	}
	if want := 2 * width; got != want {
		t.Errorf("ready instances reported as %d, want %d (two engines of width %d). "+
			"A gauge Set once per engine reports the LAST engine, not the total: "+
			"with two modules resident the second pool's report overwrites the "+
			"first's, so the series understates residency by a factor of the "+
			"module count. A live 5-minute run read 8 while stack dumps showed "+
			"14-20 goroutines inside wazero on an 8-core machine.",
			got, want, width)
	}
}

// lastInstanceCount returns the most recent count reported for state, which is
// what a gauge scrape observes.
func lastInstanceCount(obs *recordingObserver, state string) (int, bool) {
	for _, c := range slices.Backward(obs.instanceCalls()) {
		if c.state == state {
			return c.n, true
		}
	}
	return 0, false
}
