package node_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/supply"
)

// recordingWasmObserver implements node.WasmObserver (== wasm.Observer) fully,
// recording only the swap notifications this test cares about.
type recordingWasmObserver struct {
	swaps int
}

func (r *recordingWasmObserver) OnPoolSwap(context.Context, string, int, uint64, time.Duration) {
	r.swaps++
}
func (r *recordingWasmObserver) OnConfigAge(context.Context, time.Duration)   {}
func (r *recordingWasmObserver) OnInstanceCount(context.Context, string, int) {}
func (r *recordingWasmObserver) OnInstanceRecycled(context.Context, string)   {}
func (r *recordingWasmObserver) OnBorrowWait(context.Context, time.Duration)  {}
func (r *recordingWasmObserver) OnModuleCompile(context.Context, string)     {}

// SetWasmObserver must actually forward into the internal wasm package's
// global observer, not just satisfy the type alias: a config swap driven
// through the public RegisterWasmSupplyConsumer + supply.Default surface must
// reach the installed observer.
//
// This test uses supply.Default (the process-wide registry
// RegisterWasmSupplyConsumer wires against), so it must clean up its own
// registration to avoid bleeding into any other test that shares that
// registry — it uses a random module code string, so it cannot collide with
// content another test applies under a different name.
func TestSetWasmObserverForwardsPoolSwapNotifications(t *testing.T) {
	rec := &recordingWasmObserver{}
	node.SetWasmObserver(rec)
	defer node.SetWasmObserver(nil)

	name := randomSupplyName(t)
	code := "AGFzbQEAAAA=" // minimal (invalid-for-real-use) placeholder; never executed by this test

	if err := node.RegisterWasmSupplyConsumer(code, name); err != nil {
		t.Fatalf("RegisterWasmSupplyConsumer: %v", err)
	}
	// No public Unregister wrapper exists at this layer; the random per-test
	// name means the leftover registration cannot collide with any other
	// test's content.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name: name, Content: []byte(`{"rules":[]}`), Hash: "h1", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		// The placeholder module cannot actually build a pool, so OnSupplyChanged
		// is expected to return an error (buildPool fails against a bogus wasm
		// module) — the notification still had to fire before that failure for
		// this assertion to pass, and swapConfig notifies OnPoolSwap("rejected",
		// ...) on exactly that failure path.
		_ = err
	}

	if rec.swaps == 0 {
		t.Fatal("expected at least one OnPoolSwap notification via node.SetWasmObserver")
	}
}

func randomSupplyName(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return "wasm-observer-test-" + string(b)
}
