package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// reactorHost owns the process-wide wazero runtime for reactor-model guests and
// the per-module reactor engines. It mirrors wazeroEngine's runtime setup
// (WithCloseOnContextDone + memory cap + WASI) but instantiates modules as
// resident reactors (_initialize) rather than one-shot commands (_start).
type reactorHost struct {
	rt     wazero.Runtime
	rtOnce sync.Once

	mu      sync.Mutex
	engines map[string]*reactorEngine // keyed by module sha256

	// codeCache fronts engines with the base64 code string as key so the hot
	// path skips the 5 ms base64-decode + sha256 of a multi-MB module on every
	// call. Go's string map hashing uses AES-NI (~µs for a 9 MB key) vs sha256's
	// 5 ms. A miss falls through to engineFor, whose sha256 dedup is the
	// correctness backstop. Bounded LRU: module working set is small.
	codeCache *lru.Cache[string, *reactorEngine]

	// prewarm holds modules registered via Prewarm to be compiled and pooled by
	// the warmer at startup. Keyed by base64 code so re-registering the same
	// module updates its config instead of adding a duplicate. Guarded by mu.
	prewarm map[string]prewarmEntry

	// sourceDriven records code strings whose module is meant to be
	// source-driven (a supply consumer or config loader) even when no engine has
	// been compiled for them yet. Activation-time registration usually precedes
	// the module's first Execute — the compiled-module cache is empty at that
	// point — so this is the only place the intent can be recorded until
	// engineForCode creates the engine and reads it. Guarded by mu.
	sourceDriven map[string]struct{}
}

// prewarmEntry is one module queued for startup warm-up.
type prewarmEntry struct {
	code string
	cfg  any
}

// defaultPoolSize is the resident instance count per config generation. One
// instance can serve only one goroutine at a time (constraint #2), so the pool
// width bounds wasm concurrency. GOMAXPROCS aligns it with the runner's compute
// parallelism (docs §8: "= runner CPU cores").
func defaultPoolSize() uint64 {
	return uint64(max(runtime.GOMAXPROCS(0), 1))
}

var sharedReactorHost = newReactorHost()

func newReactorHost() *reactorHost {
	c, err := lru.New[string, *reactorEngine](DefaultWasmModuleCacheSize)
	if err != nil {
		panic(err)
	}
	return &reactorHost{
		engines:      map[string]*reactorEngine{},
		codeCache:    c,
		prewarm:      map[string]prewarmEntry{},
		sourceDriven: map[string]struct{}{},
	}
}

// addPrewarm queues a module for startup warm-up. Safe for concurrent use.
func (h *reactorHost) addPrewarm(code string, cfg any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prewarm[code] = prewarmEntry{code: code, cfg: cfg}
}

// prewarmModules snapshots the queued modules so the warmer can compile them
// without holding the lock (compilation is slow).
func (h *reactorHost) prewarmModules() []prewarmEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]prewarmEntry, 0, len(h.prewarm))
	for _, e := range h.prewarm {
		out = append(out, e)
	}
	return out
}

func (h *reactorHost) runtime(ctx context.Context) wazero.Runtime {
	h.rtOnce.Do(func() {
		// WithCompilationCache shares compiled machine code with the legacy
		// command runtime and, when a directory is available, across process
		// restarts (design §1 constraints #5/#6).
		h.rt = wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
			WithCloseOnContextDone(true).
			WithMemoryLimitPages(engine.DefaultWasmMemoryPages).
			WithCompilationCache(compilationCacheFor(ctx)))
		wasi_snapshot_preview1.MustInstantiate(ctx, h.rt)
	})
	return h.rt
}

// engineForCode is the hot-path entry: it resolves the reactor engine straight
// from the base64 code string, skipping the multi-MB base64-decode + sha256 on
// a cache hit. On a miss it decodes once, delegates to engineFor (sha256
// dedup), and memoizes by code string.
func (h *reactorHost) engineForCode(ctx context.Context, code string) (*reactorEngine, error) {
	if e, ok := h.codeCache.Get(code); ok {
		return e, nil
	}
	wasmBytes, err := decodeCode(code)
	if err != nil {
		return nil, err
	}
	e, err := h.engineFor(ctx, wasmBytes)
	if err != nil {
		return nil, err
	}
	// Resolve the config source once, here, so Execute never takes the registry
	// lock. hasLoader is a locked read, but this runs once per code string.
	if hasLoader(code) || h.isSourceDriven(code) {
		e.configFromSource.Store(true)
	}
	h.codeCache.Add(code, e)
	// Re-check after publishing: a concurrent RegisterConfigLoader/
	// RegisterSupplyConsumer may have run between the first check and the Add,
	// and its mark* call would have missed an engine that was not in the cache
	// yet. Without this, activation-time registration would occasionally leave
	// the module on the globals path with no rules at all.
	if hasLoader(code) || h.isSourceDriven(code) {
		e.configFromSource.Store(true)
	}
	return e, nil
}

// engineFor returns the reactor engine for a module, compiling it once and
// caching by content hash. The returned engine has no active pool yet — the
// caller warms it via ensurePool.
func (h *reactorHost) engineFor(ctx context.Context, wasmBytes []byte) (*reactorEngine, error) {
	sum := sha256.Sum256(wasmBytes)
	key := hex.EncodeToString(sum[:])

	h.mu.Lock()
	if e, ok := h.engines[key]; ok {
		h.mu.Unlock()
		return e, nil
	}
	h.mu.Unlock()

	// Compile outside the lock (compilation is slow; §0.1 ~2s cold).
	cm, err := h.runtime(ctx).CompileModule(ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("wasm reactor: compile module: %w", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	// Another goroutine may have won the race; keep the first, drop ours.
	if e, ok := h.engines[key]; ok {
		_ = cm.Close(ctx)
		return e, nil
	}
	e := &reactorEngine{host: h, cm: cm}
	e.lastAppliedVersion.Store("") // seed so Load never panics
	h.engines[key] = e
	return e, nil
}

// ensurePool guarantees the engine has an active pool configured with the given
// config. If the config is unchanged from the current generation, it is a
// no-op; otherwise it builds a new pool and swaps (B-plan). This is the single
// entry point for both first warm-up and config change.
func (e *reactorEngine) ensurePool(ctx context.Context, cfg []byte, size uint64) error {
	if p := e.active.Load(); p != nil && configHash(p.cfg) == configHash(cfg) {
		return nil
	}
	// Legacy globals path: the content has no SupplyResource revision.
	return e.swapConfig(ctx, cfg, size, 0)
}

// markConfigFromSource flips an already-created engine to source-driven config.
// A code with no engine yet is a no-op: engineForCode resolves the flag when it
// creates one (it also consults isSourceDriven, so the intent is not lost).
func (h *reactorHost) markConfigFromSource(code string) {
	if e, ok := h.codeCache.Get(code); ok {
		e.configFromSource.Store(true)
	}
}

// markConfigFromSourceOrSeed flips the engine for code to source-driven config,
// creating nothing: when the engine does not exist yet the intent is recorded so
// engineForCode picks it up at creation. Compiling the module here would pay a
// multi-second cost on the activation path.
func (h *reactorHost) markConfigFromSourceOrSeed(code string) {
	h.mu.Lock()
	h.sourceDriven[code] = struct{}{}
	h.mu.Unlock()

	if e, ok := h.codeCache.Get(code); ok {
		e.configFromSource.Store(true)
	}
}

// isSourceDriven reports whether a code string was marked source-driven.
func (h *reactorHost) isSourceDriven(code string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.sourceDriven[code]
	return ok
}
