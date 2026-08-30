package wasm

import (
	"context"
	"fmt"
	"strings"
	"sync"

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
	// globals) is recognised as already-current too.
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
// registry — whose entries warm-up later consumes, so a decode failure surfaces
// there — nothing ever consumes a source-driven marking in a way
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
	if ok {
		// Stamp lastUsed in the SAME h.mu hold as the lookup, matching every
		// other resolution path (engineForCode, engineForKey, engineForSource).
		// This is the one path that used to skip it: the sweep reads lastUsed
		// under this same lock, so touching it here is what makes the reclaim
		// decision atomic with this lookup — the engine cannot be selected for
		// reclamation in the same pass that just handed it to us. It narrows the
		// race window from "however long swapConfig takes" down to "however much
		// of engineIdleTTL elapses while swapConfig runs", which is why the
		// second check below (after swapConfig returns) still exists.
		c.host.touchLocked(e)
	}
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
	if err := e.swapConfig(ctx, snap.Content, defaultPoolSize(), snap.Revision); err != nil {
		return err
	}
	if e.reclaimed.Load() {
		// The touchLocked stamp above narrows the race but does not close it:
		// engineIdleTTL can be configured down to milliseconds (tests do exactly
		// this) and swapConfig can take longer than that building a whole pool.
		// If a reclaim pass ran and won while we were mid-build, e is no longer
		// in h.engines — nothing will ever scan it again, so the pool we just
		// installed on it would be a permanent orphan: invisible to
		// reclaimIdleEngines (it only walks h.engines) and to
		// readyInstanceTotal() (it only sums h.engines), i.e. exactly the
		// unbounded growth this whole feature exists to close, reopened by this
		// one path. Tear down what we just built instead of leaving it resident.
		if old := e.active.Swap(nil); old != nil {
			e.drainPoolWithCause(ctx, old, "engine_reclaimed")
		}
	}
	// Returning nil either way: an engine that vanished mid-swap is not a
	// failure to report upstream — it would trip the readiness gate and cut off
	// traffic — and script.ensureWasmSupplyConsumers rebuilds and re-registers
	// everything on the very next message, the same self-heal the !ok branch
	// above already relies on.
	return nil
}

// consumerOwners tracks, per (registry, moduleKey+supplyNode) registration slot,
// the SET of opaque owner identities that currently hold it registered. It
// exists because supply.Registry.RegisterConsumer/UnregisterConsumer store
// exactly ONE consumer per (name, key) — there is no reference counting inside
// the registry itself — while the digest-keyed registration has THREE
// independent call sites racing to install that one slot (spec Z.4 / Z.8):
// the legacy activation-time binding, the warm-up declaration consumer, and
// the execution-time guard. Without an owner set, any one of the three
// releasing (Deactivate) deletes the registry entry outright, silently
// erasing the other two's still-live registrations with no self-heal (see
// SupplyConfiguredByDigest's doc comment for why the short-circuit in
// script.ensureWasmSupplyConsumers cannot recover from this).
//
// This is a SET, deliberately NOT a refcount (map[string]int). A refcount
// cannot tell "the same owner registered again" (idempotent retry: activation
// redelivery, a pointer supply flipping back and forth, or
// ensureWasmSupplyConsumers running once per message) apart from "a second,
// distinct owner registered" — both increment a count, so the same owner
// retrying N times requires N releases to reach zero, and two of this
// path's three call sites (the warm-up consumer and the execution-time
// guard) never issue a matching release at all. A set's membership test
// makes the same owner registering twice a true no-op: the set's size does
// not change, and a single release from that owner empties it.
var (
	consumerOwnersMu sync.Mutex
	consumerOwners   = map[consumerOwnerKey]map[string]struct{}{}
)

// consumerOwnerKey identifies one registration slot. reg is part of the key,
// not just supplyNode+moduleKey, because tests (and potentially future
// callers) register against registries other than supply.Default; scoping by
// registry keeps their owner sets independent, matching the fact that the
// underlying supply.Registry.consumers map is itself per-instance.
type consumerOwnerKey struct {
	reg *supply.Registry
	key string // consumerKeyFor(moduleKey, supplyNode)
}

// RegisterSupplyConsumerByDigest is RegisterSupplyConsumer for modules
// identified by their artifact store digest (e.g. "sha256:<64 hex>"). The
// digest hex (without the "sha256:" prefix) is used directly as the moduleKey,
// avoiding a multi-MB base64 decode + sha256 that the code-string path needs.
//
// The function strips the "sha256:" prefix itself; callers pass the full digest.
//
// owner identifies the caller establishing this registration (spec Z.4 / Z.8).
// It must be a stable, non-empty string that is unique to the caller's own
// lifecycle — an activation identity (including ReplicaIndex) for the
// legacy activation-time binding, or a "{workflow}/{node}" pair for the
// warm-up consumer and the execution-time guard — prefixed so the two
// namespaces cannot collide (see the two forwarders in
// service/runner/trigger_activation_handler.go and
// node/internal/code/script/script.go for the exact values). Registering the
// same owner twice against the same (digest, supplyNode) is idempotent: the
// underlying registry registration is installed at most once per key
// regardless of how many times the same owner calls this.
func RegisterSupplyConsumerByDigest(digest string, supplyNode string, owner string, reg *supply.Registry) error {
	if digest == "" || supplyNode == "" {
		return fmt.Errorf("wasm: RegisterSupplyConsumerByDigest requires both digest and supply node name")
	}
	if owner == "" {
		return fmt.Errorf("wasm: RegisterSupplyConsumerByDigest requires a non-empty owner")
	}
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return fmt.Errorf("wasm: RegisterSupplyConsumerByDigest: invalid digest %q (want sha256:<64 hex>)", digest)
	}
	if reg == nil {
		reg = supply.Default
	}
	ownerKey := consumerOwnerKey{reg: reg, key: consumerKeyFor(key, supplyNode)}

	consumerOwnersMu.Lock()
	set, exists := consumerOwners[ownerKey]
	if !exists {
		set = make(map[string]struct{}, 1)
		consumerOwners[ownerKey] = set
	}
	_, alreadyOwner := set[owner]
	set[owner] = struct{}{}
	consumerOwnersMu.Unlock()

	if alreadyOwner {
		// This owner already holds the slot registered — the underlying
		// registry entry is already installed (by this owner or, if it were
		// added earlier by another owner, by that one; either way the entry
		// exists). Re-registering here would be harmless but pointless: it
		// would only re-run RegisterConsumer's immediate notify against
		// content this consumer has typically already accepted.
		return nil
	}
	c := &supplyConsumerByDigest{moduleKey: key, host: sharedReactorHost}
	sharedReactorHost.seedSourceDrivenByKey(key)
	reg.RegisterConsumer(supplyNode, ownerKey.key, c)
	return nil
}

// UnregisterSupplyConsumerByDigest removes owner's hold on a digest-keyed
// registration. It is idempotent: releasing an owner that never registered
// (or already released) is a no-op. The underlying supply.Registry
// registration is only actually removed (reg.UnregisterConsumer) when the
// LAST owner releases — see consumerOwners for why this must be set
// membership rather than a count.
func UnregisterSupplyConsumerByDigest(digest string, supplyNode string, owner string, reg *supply.Registry) {
	if owner == "" {
		// No identity to release — cannot be anyone's registration under the
		// owner-set contract, so there is nothing safe to do but no-op. See
		// RegisterSupplyConsumerByDigest, which refuses to register under an
		// empty owner in the first place.
		return
	}
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return
	}
	if reg == nil {
		reg = supply.Default
	}
	ownerKey := consumerOwnerKey{reg: reg, key: consumerKeyFor(key, supplyNode)}

	consumerOwnersMu.Lock()
	set, exists := consumerOwners[ownerKey]
	if !exists {
		consumerOwnersMu.Unlock()
		return
	}
	delete(set, owner)
	empty := len(set) == 0
	if empty {
		delete(consumerOwners, ownerKey)
	}
	consumerOwnersMu.Unlock()

	if !empty {
		// At least one other owner still holds this slot registered — the
		// registry entry must survive this release untouched.
		return
	}
	reg.UnregisterConsumer(supplyNode, ownerKey.key)
}

// SupplyConfiguredByDigest reports whether the module named by digest is both
// marked source-driven AND currently serving a configuration that came from a
// SupplyResource.
//
// This is the criterion spec §4.3.1 requires, and it is deliberately stronger
// than "registration returned no error". Registration succeeding proves nothing:
// supplyConsumerByDigest.OnSupplyChanged returns nil when the engine does not
// exist yet (so the registry records the content as accepted and never
// redelivers it), and seedSourceDrivenByKey sets the source-driven marker
// unconditionally. The dangerous state is exactly "marked source-driven, nothing
// configured" -- the module refuses traffic, or worse, an ensurePool from the
// legacy globals path fills the slot with a config no supply ever produced.
//
// The provenance test is activePool.revision != 0, whose own comment states the
// contract: "Zero means config did not come from a SupplyResource (legacy
// globals path)". Byte length is NOT usable in its place: an empty rule set is a
// legitimate supply payload (see TestSupplyConsumerAcceptsEmptyRuleset) and
// keying on length would fail-close that deployment permanently.
func SupplyConfiguredByDigest(digest string) bool {
	key, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(key) != 64 {
		return false
	}
	sharedReactorHost.mu.Lock()
	e, exists := sharedReactorHost.engines[key]
	sharedReactorHost.mu.Unlock()
	if !exists || !e.configFromSource.Load() {
		return false
	}
	p := e.active.Load()
	return p != nil && p.revision != 0
}
