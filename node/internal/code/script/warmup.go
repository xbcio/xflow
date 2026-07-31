package script

import (
	"context"
	"time"

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

// RegisterWasmConfigLoader associates a ConfigLoader with a wasm module. During
// warmup the loader is invoked once to establish the initial pool; if ttl > 0 a
// background goroutine polls for config changes. See wasm.RegisterConfigLoader.
func RegisterWasmConfigLoader(code string, loader wasm.ConfigLoader, ttl time.Duration) {
	wasm.RegisterConfigLoader(code, loader, ttl)
}

// RegisterWasmSupplyConsumer makes a wasm module consume the named supply, so a
// content change rebuilds its instance pool. Call it when the workflow arrives
// (activation time) — NOT before Warmup: at process start the runner does not yet
// know which workflows it will host.
func RegisterWasmSupplyConsumer(code string, supplyNode string) error {
	return wasm.RegisterSupplyConsumer(code, supplyNode, supply.Default)
}
