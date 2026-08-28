package runner

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/store"
)

// wasmWarmupConsumer pre-compiles the module a pointer supply currently names,
// synchronously, inside supply.Registry.Apply.
//
// Synchronous is not an oversight, it is where the reporting semantics come
// from. Apply notifies inline (node/supply/registry.go:245 calls safeNotify in a
// plain loop, not a goroutine) and counts the consumer in-flight under the lock
// before releasing it, so IsReady cannot read a verdict this consumer has not
// given yet. That makes the supply's readiness mean "the new module compiled
// successfully HERE" rather than merely "the bytes arrived" -- spec §4.4.2's
// L1'. Without it the pointer supply has no consumers at all, isReadyLocked's
// loop body is empty, and readiness is true the moment a snapshot lands.
//
// SCOPE LIMIT, stated rather than hidden: DigestExpr is rendered against an
// environment containing only $supplies, because Apply has no types.Input. An
// expression referencing any other root cannot be rendered here; this consumer
// then returns nil (a rejection would leave a legitimately-shaped node
// permanently not-ready) and logs why. That node's reporting falls back to bare
// L1 and its first message pays the compile. Correctness is unaffected: the
// execution-time guard in node/internal/code/script still refuses to run a
// module with no supply-borne configuration.
type wasmWarmupConsumer struct {
	digestExpr string
	supplyNode string
	workflow   string
	node       string
	artifact   func(ctx context.Context, digest string) ([]byte, error)
}

var _ supply.Consumer = (*wasmWarmupConsumer)(nil)

func (c *wasmWarmupConsumer) OnSupplyChanged(ctx context.Context, _ supply.Snapshot) error {
	// Rendered against the registry's decoded view rather than the snapshot
	// passed in, so an expression may reference any supply -- and so warm-up and
	// boundary evaluation read the same source. Decoded() is one atomic load of
	// a whole-table view, so a single render is internally consistent.
	rendered, err := exprx.RenderTemplate(c.digestExpr, exprx.SuppliesEnv(supply.Default))
	if err != nil {
		// err is deliberately NOT logged: exprx's underlying compiler echoes the
		// expression source verbatim in its error text (e.g. "unknown name $input
		// (1:1)\n | $input.<whatever the node's params contain> ..."), and
		// DigestExpr is operator-authored node configuration -- the same class of
		// value the org's log-content policy and this task's own constraint both
		// forbid echoing. This branch is not a rare failure; it is the documented,
		// expected shape for any node whose artifact_digest references $input,
		// $env, or $credentials, so logging err here would put that node's template
		// text into the process log on every Apply to the pointer supply it names.
		// Workflow, node, and supply names are enough for an operator who
		// configured the node to identify which expression is affected without
		// echoing it.
		slog.Warn("wasm warm-up skipped: the artifact_digest expression must be rooted "+
			"at $supplies alone to be warmed here; this node reports bare supply "+
			"delivery instead of module readiness, and its first message will pay "+
			"the module compile",
			"workflow", c.workflow, "node", c.node, "supply", c.supplyNode)
		return nil
	}
	digest, _ := rendered.(string)
	if err := store.ValidateDigest(digest); err != nil {
		// The pointer content is operator-supplied. A malformed digest is a real
		// failure: the node will fail closed on its next message, so reporting it
		// as not-ready here is exactly right.
		return fmt.Errorf("warm-up for supply %q resolved a malformed artifact digest: %w",
			c.supplyNode, err)
	}
	if node.WasmSupplyConfigured(digest) {
		return nil
	}
	raw, err := c.artifact(ctx, digest)
	if err != nil {
		return fmt.Errorf("warm-up fetch of wasm module %s for supply %q: %w", digest, c.supplyNode, err)
	}
	if err := node.CompileWasmModuleBytes(ctx, raw); err != nil {
		return fmt.Errorf("warm-up compile of wasm module %s for supply %q: %w", digest, c.supplyNode, err)
	}
	// Register AFTER compiling, for the reason registerSupplyConsumers documents:
	// registering first marks the module source-driven while the notify handler
	// silently succeeds against a non-existent engine, and the registry then never
	// redelivers the content.
	if err := node.RegisterWasmSupplyConsumerByDigest(digest, c.supplyNode); err != nil {
		return fmt.Errorf("warm-up registration of wasm module %s for supply %q: %w", digest, c.supplyNode, err)
	}
	return nil
}

// warmupConsumerKey namespaces warm-up registrations away from the wasm
// package's own {supplyNode}@{moduleKey} keys, so the two never collide -- a
// collision would silently replace one with the other (registry.go's
// RegisterConsumer overwrites a same-key entry unconditionally).
//
// This key does NOT include activation or replica identity, so every replica
// of the same (workflow, node, supply) declaration computes the identical
// key -- see warmupConsumerRegistry for why that makes the registration a
// refcount rather than a plain register/unregister pair.
func warmupConsumerKey(b engine.SupplyConsumerBinding) string {
	return "warmup/" + b.WorkflowName + "/" + b.NodeName + "@" + b.SupplyNode
}

// warmupConsumerRegistry refcounts the warm-up consumer's registration on
// supply.Default, keyed by warmupConsumerKey, mirroring
// node/internal/code/script's supplyDeclarationTable (which refcounts the
// execution-time declaration for exactly the same reason).
//
// WHY refcounted, not a plain register/unregister pair: one activation may be
// hosted at several replicas on one runner (activationID differs only by
// ReplicaIndex -- service/runner/activation_tracker.go), each declaring the
// identical (workflow, node, supply) pair and therefore computing the
// identical warmupConsumerKey. supply.Registry.RegisterConsumer/
// UnregisterConsumer key on that string alone; if this package called them
// directly, replica 0's Deactivate would call UnregisterConsumer and delete
// the ONE entry at that key outright, stripping the warm-up consumer replica
// 1 still depends on. The pointer supply would then have zero registered
// consumers, isReadyLocked's documented "no consumers = always ready" rule
// would make IsReady read true unconditionally, and the next pointer flip on
// the still-live replica would compile nothing -- the exact silent L1
// fallback this warm-up consumer exists to prevent.
type warmupConsumerRegistry struct {
	mu     sync.Mutex
	counts map[string]int
}

// warmupConsumers is the process-wide registry, mirroring supply.Default's own
// process-wide scope: registerSupplyConsumers/unregisterSupplyConsumers run
// against supply.Default from any activation on this runner, so the refcount
// they share must be equally global.
var warmupConsumers = &warmupConsumerRegistry{counts: map[string]int{}}

// acquire registers c under key against supplyNode on the 0->1 transition
// only; every other call just increments the refcount, leaving the
// already-registered consumer (and its already-resolved warm-up verdict, if
// any) untouched. Mirrors supplyDeclarationTable.declare.
func (r *warmupConsumerRegistry) acquire(key, supplyNode string, c supply.Consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[key]++
	if r.counts[key] == 1 {
		supply.Default.RegisterConsumer(supplyNode, key, c)
	}
}

// release decrements the refcount for key and calls UnregisterConsumer only
// on the 1->0 transition. A release with no matching acquire (refcount
// already zero, or key never seen) is a no-op -- mirrors
// supplyDeclarationTable.undeclare's floor-at-zero behavior, and keeps this
// safe to call from a full-teardown path that does not track whether it has
// already run for this key.
func (r *warmupConsumerRegistry) release(key, supplyNode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.counts[key]
	if !ok || n <= 0 {
		return
	}
	if n == 1 {
		delete(r.counts, key)
		supply.Default.UnregisterConsumer(supplyNode, key)
		return
	}
	r.counts[key] = n - 1
}
