package wasm

import (
	"context"
	"fmt"
	"strings"

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
	// (Generation, pool.go:492), so updating it in place would race them;
	// rebuilding a whole
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
// The key is (module, supplyNode) so re-activating the same workflow replaces the
// registration instead of accumulating duplicates.
//
// An undecodable code is rejected here rather than registered. Unlike the prewarm
// and loader registries — whose entries warm-up later consumes, so a decode
// failure surfaces there — nothing ever consumes a source-driven marking in a way
// that could report an error. Registering under an unmatchable key would leave the
// module permanently on the legacy globals path, evaluating against no rules and
// passing every record through untagged and uncleansed, with no diagnostic
// anywhere. This function can return an error, so it does.
func RegisterSupplyConsumer(code string, supplyNode string, reg *supply.Registry) error {
	if code == "" || supplyNode == "" {
		return fmt.Errorf("wasm: RegisterSupplyConsumer requires both code and supply node name")
	}
	key, err := moduleKey(code)
	if err != nil {
		return fmt.Errorf("wasm: RegisterSupplyConsumer: %w", err)
	}
	if reg == nil {
		reg = supply.Default
	}
	c := &supplyConsumer{code: code, host: sharedReactorHost}
	// Mark the module source-driven BEFORE registering: RegisterConsumer notifies
	// immediately when content is already cached, and that notification builds the
	// pool. If the flag were set after, a message arriving in between would take
	// the globals path and eval against no rules.
	sharedReactorHost.seedSourceDrivenByKey(key)
	reg.RegisterConsumer(supplyNode, consumerKeyFor(key, supplyNode), c)
	return nil
}

// UnregisterSupplyConsumer removes the registration. Idempotent. An undecodable
// code is a no-op: RegisterSupplyConsumer rejected it, so nothing is installed.
func UnregisterSupplyConsumer(code string, supplyNode string, reg *supply.Registry) {
	key, err := moduleKey(code)
	if err != nil {
		return
	}
	if reg == nil {
		reg = supply.Default
	}
	reg.UnregisterConsumer(supplyNode, consumerKeyFor(key, supplyNode))
}

// consumerKeyFor builds the registration key from a module identity. Register and
// unregister must derive it the same way, or an unregister silently leaves the
// consumer installed and content changes keep rebuilding the pool of a module
// nothing executes any more.
func consumerKeyFor(moduleKey, supplyNode string) string {
	return supplyNode + "@" + moduleKey
}

// supplyConsumerByDigest is a variant of supplyConsumer for modules identified
// by their artifact digest (sha256 hex) rather than a base64 code string. The
// digest IS the moduleKey, so no decode is needed. The engine must already exist
// (compiled on first Execute via the artifact path); if it doesn't,
// OnSupplyChanged returns an error and the supply registry retries on the next
// Apply (which is after Execute has created the engine).
type supplyConsumerByDigest struct {
	moduleKey string // sha256 hex (= digest without "sha256:" prefix)
	host      *reactorHost
}

var _ supply.Consumer = (*supplyConsumerByDigest)(nil)

func (c *supplyConsumerByDigest) OnSupplyChanged(ctx context.Context, snap supply.Snapshot) error {
	c.host.mu.Lock()
	e, ok := c.host.engines[c.moduleKey]
	c.host.mu.Unlock()
	if !ok {
		// Engine not yet compiled — the first Execute (or resolveArtifacts
		// prewarm) will create it. Return nil: the supply registry marks this
		// content as accepted so IsReady passes, and RegisterConsumer's
		// immediate re-notify (which fires on every activation) will deliver the
		// content once the engine exists. Returning an error would block the
		// readiness gate and prevent traffic entirely.
		return nil
	}
	if p := e.active.Load(); p != nil && configHash(p.cfg) == configHash(snap.Content) {
		return nil
	}
	return e.swapConfig(ctx, snap.Content, defaultPoolSize(), snap.Revision)
}

// RegisterSupplyConsumerByDigest is RegisterSupplyConsumer for modules
// identified by their artifact store digest (e.g. "sha256:<64 hex>"). The
// digest hex (without the "sha256:" prefix) is used directly as the moduleKey,
// avoiding a multi-MB base64 decode + sha256 that the code-string path needs.
//
// The function strips the "sha256:" prefix itself; callers pass the full digest.
func RegisterSupplyConsumerByDigest(digest string, supplyNode string, reg *supply.Registry) error {
	if digest == "" || supplyNode == "" {
		return fmt.Errorf("wasm: RegisterSupplyConsumerByDigest requires both digest and supply node name")
	}
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return fmt.Errorf("wasm: RegisterSupplyConsumerByDigest: invalid digest %q (want sha256:<64 hex>)", digest)
	}
	if reg == nil {
		reg = supply.Default
	}
	c := &supplyConsumerByDigest{moduleKey: key, host: sharedReactorHost}
	sharedReactorHost.seedSourceDrivenByKey(key)
	reg.RegisterConsumer(supplyNode, consumerKeyFor(key, supplyNode), c)
	return nil
}

// UnregisterSupplyConsumerByDigest removes a digest-keyed registration.
func UnregisterSupplyConsumerByDigest(digest string, supplyNode string, reg *supply.Registry) {
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return
	}
	if reg == nil {
		reg = supply.Default
	}
	reg.UnregisterConsumer(supplyNode, consumerKeyFor(key, supplyNode))
}
