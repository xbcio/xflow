package wasm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// DefaultWasmModuleCacheSize bounds the compiled-wasm-module LRU cache. wasm
// modules are static content keyed by sha256, so the working set is normally
// tiny; the bound protects high-cardinality deployments from unbounded growth.
const DefaultWasmModuleCacheSize = 256

// TODO(wasm host imports): currently guests communicate via stdin/stdout JSON
// only — they cannot call $helpers (base64, hmac, jsonPath...) mid-execution.
// Future work: register a host module (e.g. "xflow") that exports helpers via
// a WASI-like ABI:
//
//	(import "xflow" "base64_encode" (func (param i32 i32) (result i32 i32)))
//
// Requirements when this is implemented:
//  1. ABI version negotiation — guests declare which xflow ABI they target;
//     host refuses incompatible modules to avoid silent breakage on upgrade.
//  2. Per-call host fn dispatch must stay sandbox-safe (no FS/clock/random
//     slipping in via host imports; deterministic, IO-free helpers only).
//  3. Memory ownership convention — agree on host-allocates vs guest-allocates
//     for variable-size return values, document it once on the host module.
//  4. Parity test: the same logical helper invocation produces identical
//     bytes from goja, qjs, AND a wasm guest using the host import.
//
// Until then, guests must bundle equivalent functionality themselves (e.g.
// Go guests import encoding/base64).
func init() {
	engine.Register("wasm", "wazero", func() engine.Engine { return sharedWazero })
}

var sharedWazero = &wazeroEngine{compiled: newWasmModuleCache(DefaultWasmModuleCacheSize)}

type wazeroEngine struct {
	rt       wazero.Runtime
	rtOnce   sync.Once
	compiled *wasmModuleCache
}

type wasmModuleCache struct {
	c *lru.Cache[string, wazero.CompiledModule]
}

func newWasmModuleCache(size int) *wasmModuleCache {
	c, err := lru.NewWithEvict[string, wazero.CompiledModule](size, func(_ string, cm wazero.CompiledModule) {
		// Release the compiled module when evicted so wazero reclaims its
		// compiled code memory rather than leaking it for the process lifetime.
		_ = cm.Close(context.Background())
	})
	if err != nil {
		panic(err)
	}
	return &wasmModuleCache{c: c}
}

func (e *wazeroEngine) Name() string { return "wasm/wazero" }

func (e *wazeroEngine) runtime(ctx context.Context) wazero.Runtime {
	e.rtOnce.Do(func() {
		// WithCloseOnContextDone lets wazero abort in-flight guest execution
		// when ctx expires — even a tight CPU-bound loop with no host calls is
		// interrupted (spec §7.3). The up-front ctx.Err() guard in Execute still
		// short-circuits an already-expired ctx without starting execution.
		// WithMemoryLimitPages caps each module's linear memory at the
		// configured budget (each page is 64 KiB). Guests that exceed this
		// via memory.grow trap at the wazero boundary instead of consuming
		// unbounded host memory.
		// WithCompilationCache shares compiled machine code with the reactor
		// runtime and, when a directory is available, across process restarts
		// (design §1 constraints #5/#6) so a runner redeploy does not pay a
		// multi-second compile on its first request.
		e.rt = wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
			WithCloseOnContextDone(true).
			WithMemoryLimitPages(engine.DefaultWasmMemoryPages).
			WithCompilationCache(compilationCacheFor(ctx)))
		wasi_snapshot_preview1.MustInstantiate(ctx, e.rt)
	})
	return e.rt
}

func (e *wazeroEngine) compile(ctx context.Context, wasmBytes []byte) (wazero.CompiledModule, error) {
	sum := sha256.Sum256(wasmBytes)
	key := hex.EncodeToString(sum[:])

	if cm, ok := e.compiled.c.Get(key); ok {
		obs().OnModuleCompile(ctx, "hit")
		return cm, nil
	}

	cm, err := e.runtime(ctx).CompileModule(ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("wasm/wazero: compile module: %w", err)
	}
	// LRU.Add is thread-safe; concurrent compilers of the same module may both
	// compile but only one entry is retained (the other is closed on eviction).
	e.compiled.c.Add(key, cm)
	obs().OnModuleCompile(ctx, "miss")
	return cm, nil
}

func (e *wazeroEngine) Execute(ctx context.Context, src engine.Source, globals map[string]any, _ engine.Helpers) (any, error) {
	// Module cache hit/miss goes to obs().OnModuleCompile, the same hook the
	// reactor path reports through, so this cache is observable from outside
	// the package rather than only by reaching into e.compiled.
	//
	// TODO(metrics): the remaining three need an Observer method each before
	// they can be emitted; observability/metrics.ScriptMetrics is the sink.
	//   - script_wasm_compile_duration_seconds       (CompileModule only)
	//   - script_wasm_execute_duration_seconds       (InstantiateModule window)
	//   - script_wasm_exit_total{code=...}           (guest exit codes)
	if ctx == nil {
		ctx = context.Background()
	}
	// Fail fast on an already-cancelled/expired context. wazero's in-flight
	// cancellation is cooperative (checked at call boundaries), so a short
	// guest can finish before a checkpoint is hit — this guarantees a
	// cancelled context is always honored.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wasm/wazero: run module: %w", err)
	}
	// A known digest names the compiled module directly, so a warm call skips
	// the multi-MB base64 decode and sha256 entirely. On a miss the bytes are
	// needed anyway to compile, and compile() re-derives the key from them
	// rather than trusting the caller's — the cache key must always be the hash
	// of what was actually compiled.
	if src.Digest != "" {
		if key, ok := moduleKeyFromDigest(src.Digest); ok {
			if cm, hit := e.compiled.c.Get(key); hit {
				obs().OnModuleCompile(ctx, "hit")
				return e.run(ctx, cm, globals)
			}
		}
	}
	wasmBytes, err := decodeCode(src.Code)
	if err != nil {
		return nil, err
	}
	cm, err := e.compile(ctx, wasmBytes)
	if err != nil {
		return nil, err
	}
	return e.run(ctx, cm, globals)
}

// run instantiates one compiled module against globals and returns its decoded
// stdout. Split out of Execute so the digest and content paths share exactly one
// copy of the sandbox configuration and exit handling.
func (e *wazeroEngine) run(ctx context.Context, cm wazero.CompiledModule, globals map[string]any) (any, error) {
	stdin, err := encodeStdin(globals)
	if err != nil {
		return nil, err
	}

	var stdout bytes.Buffer
	// Sandbox: stdin/stdout only. No WithFSConfig, clock, random, or env.
	cfg := wazero.NewModuleConfig().
		WithStdin(bytes.NewReader(stdin)).
		WithStdout(&stdout).
		WithName("")

	mod, err := e.runtime(ctx).InstantiateModule(ctx, cm, cfg)
	if mod != nil {
		_ = mod.Close(ctx)
	}
	if err != nil {
		// A normal WASI command exits via os.Exit; wazero surfaces that as a
		// *sys.ExitError. Exit code 0 is success — read stdout. Any non-zero
		// code is a genuine guest runtime error.
		var exitErr *sys.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 0 {
			return decodeStdout(stdout.Bytes())
		}
		// Context cancellation (e.g. timeout) and non-zero exits are errors.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("wasm/wazero: run module: %w", ctxErr)
		}
		return nil, fmt.Errorf("wasm/wazero: run module: %w", err)
	}

	return decodeStdout(stdout.Bytes())
}
