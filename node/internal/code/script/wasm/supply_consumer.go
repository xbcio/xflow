package wasm

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/node/supply"
)

// supplyConsumer adapts one wasm module to supply.Consumer: a content change
// rebuilds the module's instance pool through the existing swapConfig path.
//
// wasm is the first consumer, not a special case. Everything wasm-specific lives
// behind this adapter; a second node type that needs to pre-compile against
// content adds its own implementation without touching the distribution
// skeleton.
type supplyConsumer struct {
	code string
	host *reactorHost
}

var _ supply.Consumer = (*supplyConsumer)(nil)

// OnSupplyChanged rebuilds the pool for the new content. Returning an error means
// the content was rejected and the module keeps its last-good pool — a bad rule
// set is caught by buildPool, which discards the whole batch on any failure.
//
// It creates no execution and dispatches no task: a content change is a lookup
// change, never a trigger. Messages already in flight finish on the old pool;
// only messages after the swap see the new rules.
//
// Content that is already active is a no-op. The registry notifies with content
// this module may already be serving — RegisterConsumer notifies immediately on
// every workflow re-activation, and Apply re-notifies whenever any consumer's
// verdict for the current content is unresolved.
//
// The cost of not checking is not just a wasted rebuild. A redundant swap also
// resets lastSwapAt and sourceFailures, and lastSwapAt is what drives the Stale
// tier and xflow_supply_age_seconds — the one signal that exposes a source which
// silently stopped updating (gen stays put when a source keeps failing, so only
// elapsed time reveals it). Reactivation churn would keep stamping that
// timestamp fresh, so an operator would read a healthy age for content that had
// not actually refreshed in hours. The check protects the signal, not just the
// CPU.
func (c *supplyConsumer) OnSupplyChanged(ctx context.Context, snap supply.Snapshot) error {
	e, err := c.host.engineForCode(ctx, c.code)
	if err != nil {
		return fmt.Errorf("wasm supply consumer: %w", err)
	}
	// Compare the CONTENT, not snap.Hash: this compares against the bytes the
	// pool was actually built from, so a pool built via any other path (legacy
	// globals, a config loader) is recognised as already-current too.
	//
	// A revision bump with identical bytes deliberately does NOT swap. activePool
	// is published read-only and readers load p.revision without a lock
	// (pool.go:464), so updating it in place would race them; rebuilding a whole
	// pool to carry a number is worse. The reported config_generation therefore
	// stays at the revision whose bytes are actually loaded, which is the honest
	// answer to "which content produced this row".
	if p := e.active.Load(); p != nil && configHash(p.cfg) == configHash(snap.Content) {
		return nil
	}
	// The revision travels into the pool so eval results can report which content
	// version produced them, comparably across runners.
	return e.swapConfig(ctx, snap.Content, defaultPoolSize(), snap.Revision)
}

// RegisterSupplyConsumer makes a wasm module a consumer of the given supply node,
// so a content change rebuilds its pool. It may be called at any time — the
// activation path calls it when a workflow arrives, long after warmup.
//
// The key is (code, supplyNode) so re-activating the same workflow replaces the
// registration instead of accumulating duplicates.
func RegisterSupplyConsumer(code string, supplyNode string, reg *supply.Registry) error {
	if code == "" || supplyNode == "" {
		return fmt.Errorf("wasm: RegisterSupplyConsumer requires both code and supply node name")
	}
	if reg == nil {
		reg = supply.Default
	}
	c := &supplyConsumer{code: code, host: sharedReactorHost}
	// Mark the module source-driven BEFORE registering: RegisterConsumer notifies
	// immediately when content is already cached, and that notification builds the
	// pool. If the flag were set after, a message arriving in between would take
	// the globals path and eval against no rules.
	sharedReactorHost.markConfigFromSourceOrSeed(code)
	reg.RegisterConsumer(supplyNode, consumerKey(code, supplyNode), c)
	return nil
}

// UnregisterSupplyConsumer removes the registration. Idempotent.
func UnregisterSupplyConsumer(code string, supplyNode string, reg *supply.Registry) {
	if reg == nil {
		reg = supply.Default
	}
	reg.UnregisterConsumer(supplyNode, consumerKey(code, supplyNode))
}

// consumerKey identifies one (module, supply) registration. The code string can
// be megabytes of base64, so the key uses its content hash — the same identity
// engineFor dedups on.
func consumerKey(code, supplyNode string) string {
	return supplyNode + "@" + configHash([]byte(code))
}
