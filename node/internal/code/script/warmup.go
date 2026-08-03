package script

import (
	"context"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/supply"
)

// Warmup absorbs script-engine cold-start cost before traffic arrives. It runs
// every engine that registered a warmer: js/qjs pays a ~330 ms QuickJS-wasm
// compile on its first use, and wasm/wazero-reactor opens the wazero runtime
// (resolving the on-disk compilation cache) plus warms a pool for any module
// registered via PrewarmWasm.
//
// Servers and runners should call this once at startup. It is safe to call more
// than once — warmers are idempotent. A returned error means one engine failed
// to warm; log it and continue, because every engine also warms lazily on its
// first request.
func Warmup(ctx context.Context) error { return engine.Warmup(ctx) }

// PrewarmWasm registers a base64 wasm module (the same string a ScriptNode
// carries as its code) and its reactor config so Warmup compiles the module and
// builds its instance pool at startup rather than under the first request's
// deadline. Call it before Warmup; re-registering the same module replaces its
// config. Only the wasm/wazero-reactor runtime uses this.
func PrewarmWasm(code string, cfg any) { wasm.Prewarm(code, cfg) }

// RegisterWasmSupplyConsumer makes a wasm module consume the named supply, so a
// content change rebuilds its instance pool. Call it when the workflow arrives
// (activation time) — NOT before Warmup: at process start the runner does not yet
// know which workflows it will host.
func RegisterWasmSupplyConsumer(code string, supplyNode string) error {
	return wasm.RegisterSupplyConsumer(code, supplyNode, supply.Default)
}

// RegisterWasmSupplyConsumerByDigest is the artifact-digest variant: the module
// is identified by its sha256 digest rather than a base64 code string.
func RegisterWasmSupplyConsumerByDigest(digest string, supplyNode string) error {
	return wasm.RegisterSupplyConsumerByDigest(digest, supplyNode, supply.Default)
}

// CompileWasmModule eagerly compiles a wasm module (base64 code string) into the
// reactor engine cache. Used by resolveArtifacts so the engine exists before
// supply consumers attempt to configure it.
func CompileWasmModule(ctx context.Context, code string) error {
	return wasm.CompileModule(ctx, code)
}
