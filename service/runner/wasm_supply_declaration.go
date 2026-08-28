package runner

import (
	"context"
	"fmt"
	"log/slog"

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
		slog.Warn("wasm warm-up skipped: artifact_digest expression could not be rendered "+
			"against $supplies alone; this node reports bare supply delivery instead of "+
			"module readiness, and its first message will pay the module compile",
			"workflow", c.workflow, "node", c.node, "supply", c.supplyNode, "error", err)
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
// This key registers into supply.Registry's consumer map as a SET, exactly
// like the legacy {digest, supplyNode} shape: registering twice at the same
// key is idempotent, and unregistering it removes it outright. That is a
// THIRD reconciliation semantic layered on top of the declaration shape's
// refcounted declaration table -- see storeSubscription for why the two must
// be reconciled by different rules even though both live on the same
// engine.SupplyConsumerBinding.
func warmupConsumerKey(b engine.SupplyConsumerBinding) string {
	return "warmup/" + b.WorkflowName + "/" + b.NodeName + "@" + b.SupplyNode
}
