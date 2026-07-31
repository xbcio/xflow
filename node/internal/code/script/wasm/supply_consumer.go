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
func (c *supplyConsumer) OnSupplyChanged(ctx context.Context, snap supply.Snapshot) error {
	e, err := c.host.engineForCode(ctx, c.code)
	if err != nil {
		return fmt.Errorf("wasm supply consumer: %w", err)
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
