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
	//
	// This is the ONE map that stays keyed by the code string, and deliberately:
	// it is a pure memo on the per-message hot path, and a stale or duplicated
	// entry costs at most a recompile that engineFor then dedups. Measured, the
	// alternative is not viable — re-keying it by module sha256 would put ~1.4 ms
	// decode + ~1.8 ms hash on every message and cap throughput near 310 msg/s
	// against the 12508 msg/s this engine sustains. The registries below record
	// FACTS about a module, where a split identity is silent misbehaviour rather
	// than a wasted cycle, so they key on moduleKey instead.
	codeCache *lru.Cache[string, *reactorEngine]

	// prewarm holds modules registered via Prewarm to be compiled and pooled by
	// the warmer at startup. Keyed by moduleKey so re-registering the same module
	// updates its config instead of adding a duplicate — including when the two
	// registrations carry different base64 encodings of the same bytes. The entry
	// retains the original code string because warm-up needs it to reach
	// engineForCode. Guarded by mu.
	prewarm map[string]prewarmEntry

	// sourceDriven records modules whose config is meant to come from a supply or
	// loader (rather than globals["$config"]) even when no engine has been
	// compiled for them yet. Activation-time registration usually precedes the
	// module's first Execute — the compiled-module cache is empty at that point —
	// so this is the only place the intent can be recorded until engineForCode
	// creates the engine and reads it. Keyed by moduleKey: recording this under a
	// base64 string would make the intent invisible to another encoding of the
	// same module, which silently drops it back to the globals path and evaluates
	// every record against no rules at all. Guarded by mu.
	sourceDriven map[string]struct{}
}

// prewarmEntry is one module queued for startup warm-up. It keeps the original
// code string because warm-up resolves the engine through engineForCode, which
// needs the encoding, not the key.
type prewarmEntry struct {
	code string
	cfg  any
}

// registryKeyOrRaw is the key for registries whose entries are later CONSUMED by
// warm-up: prewarm and loaderRegistry. On a decodable module it is the canonical
// moduleKey; on an undecodable one it falls back to the raw string.
//
// The fallback is what keeps warm-up's "an unusable module is reported, not
// silently dropped" contract (see TestPrewarm_BadModuleSurfacesError): warm-up
// calls engineForCode on the retained code string and surfaces the decode error
// there. Dropping the entry at registration instead would turn a reported
// misconfiguration into a module that never warms and nobody is told about.
// Registration on these paths is a void call with no channel to report on.
//
// The fallback cannot collide with a real moduleKey: a moduleKey is 64 hex
// characters, which is itself valid base64, whereas this branch is reached only
// for strings base64 REJECTS.
//
// sourceDriven deliberately does NOT use this. Nothing ever consumes a
// sourceDriven entry in a way that would surface an error — it is a pure lookup —
// so a raw-keyed entry would sit there matching no module forever, which is the
// silent globals-path fallback this whole change exists to remove. Its writer
// (RegisterSupplyConsumer) returns an error instead.
func registryKeyOrRaw(code string) string {
	if k, err := moduleKey(code); err == nil {
		return k
	}
	return code
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
	key := registryKeyOrRaw(code)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prewarm[key] = prewarmEntry{code: code, cfg: cfg}
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
	key := moduleKeyOf(wasmBytes)
	e, err := h.engineForKey(ctx, key, wasmBytes)
	if err != nil {
		return nil, err
	}
	// Resolve the config source and publish the engine as one atomic step,
	// under the same lock the mark* registration path holds.
	//
	// Registration and engine creation are a Dekker-style crossing: each side
	// publishes its own state and then looks for the other's. Registration
	// records the intent (sourceDriven / loaderRegistry) and then flips any
	// already-cached engine; creation reads the intent and then caches the
	// engine. Interleaved, both lookups can miss — registration's codeCache.Get
	// finds nothing because the Add has not happened, and the intent read
	// already ran before the write landed. The engine then stays on the globals
	// path forever, evaluating with no rules at all, and nothing later repairs
	// it.
	//
	// A double-check after the Add only narrows that window; it does not close
	// it, and being nanoseconds wide it is unreachable by any test — untestable
	// code guarding a real defect is worse than no guard. One mutex covering
	// both steps closes it by construction: whichever side takes h.mu second
	// necessarily observes what the first published.
	h.mu.Lock()
	fromSource := h.sourceDrivenLocked(key)
	h.codeCache.Add(code, e)
	h.mu.Unlock()

	// hasLoaderKey takes loaderMu, so it stays outside h.mu — see the lock-order
	// note on markConfigFromSource. It is safe outside because RegisterConfigLoader
	// publishes to loaderRegistry BEFORE it calls markConfigFromSource, so a
	// registration this read misses is one whose flip finds the engine already
	// cached above.
	if fromSource || hasLoaderKey(key) {
		e.configFromSource.Store(true)
	}
	return e, nil
}

// moduleKeyOf is moduleKey for callers that already hold the decoded bytes.
func moduleKeyOf(wasmBytes []byte) string {
	sum := sha256.Sum256(wasmBytes)
	return hex.EncodeToString(sum[:])
}

// engineFor returns the reactor engine for a module, compiling it once and
// caching by content hash. The returned engine has no active pool yet — the
// caller warms it via ensurePool.
func (h *reactorHost) engineFor(ctx context.Context, wasmBytes []byte) (*reactorEngine, error) {
	return h.engineForKey(ctx, moduleKeyOf(wasmBytes), wasmBytes)
}

// engineForKey is engineFor for callers that already computed the module key,
// so a multi-MB module is hashed once per call rather than twice.
func (h *reactorHost) engineForKey(ctx context.Context, key string, wasmBytes []byte) (*reactorEngine, error) {
	h.mu.Lock()
	if e, ok := h.engines[key]; ok {
		h.mu.Unlock()
		obs().OnModuleCompile(ctx, "hit")
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
		obs().OnModuleCompile(ctx, "hit")
		return e, nil
	}
	e := &reactorEngine{host: h, cm: cm}
	e.lastAppliedVersion.Store("") // seed so Load never panics
	h.engines[key] = e
	obs().OnModuleCompile(ctx, "miss")
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
// A module with no engine yet is a no-op: engineForCode resolves the flag when it
// creates one (it also consults sourceDriven, so the intent is not lost).
//
// An undecodable code is also a no-op. There is no engine for it and never will
// be, so there is nothing to flip; its caller (RegisterConfigLoader) still keeps
// the registry entry, and warm-up reports the decode failure from there.
//
// Lock order: callers hold loaderMu-free state here — this takes h.mu, and
// hasLoaderKey takes loaderMu, so the two are never nested in this direction.
func (h *reactorHost) markConfigFromSource(code string) {
	key, err := moduleKey(code)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.flipEngineLocked(key)
}

// seedSourceDrivenByKey records that a module's config comes from a supply or
// loader, and flips its engine if one already exists. It creates nothing: when
// the engine does not exist yet the intent is recorded so engineForCode picks it
// up at creation. Compiling the module here would pay a multi-second cost on the
// activation path.
//
// The seed write and the engine flip happen under ONE h.mu hold, matching
// engineForCode's single-hold read-and-publish. Splitting them is what let an
// engine created concurrently miss both the seed and the flip.
//
// It takes a moduleKey rather than a code string, which is what forces the caller
// to decode — and therefore to handle an undecodable module explicitly. That is
// deliberate: nothing ever consumes a source-driven marking in a way that could
// report a failure later (it is a pure lookup), so an entry seeded under an
// unmatchable key would leave the module permanently on the legacy globals path,
// evaluating against no rules, with no diagnostic anywhere. Its one production
// caller, RegisterSupplyConsumer, returns that error to the activation path.
func (h *reactorHost) seedSourceDrivenByKey(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sourceDriven[key] = struct{}{}
	h.flipEngineLocked(key)
}

// flipEngineLocked marks the engine for key source-driven if one exists.
// Caller must hold h.mu.
//
// It reads h.engines, not codeCache: engines is authoritative and never evicts,
// whereas codeCache is a bounded LRU. Consulting the LRU would silently drop the
// flip for a module evicted since its last execution, leaving a live engine on
// the globals path — evaluating against no rules — with nothing to repair it,
// since engineForCode only resolves the flag when it CREATES an engine and this
// one already exists.
func (h *reactorHost) flipEngineLocked(key string) {
	if e, ok := h.engines[key]; ok {
		e.configFromSource.Store(true)
	}
}

// isSourceDriven reports whether a module, named by its base64 code, was marked
// source-driven. An undecodable code is never source-driven: no engine can exist
// for it.
func (h *reactorHost) isSourceDriven(code string) bool {
	key, err := moduleKey(code)
	if err != nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sourceDrivenLocked(key)
}

// sourceDrivenLocked reports whether a moduleKey was marked source-driven, for
// callers already holding h.mu.
func (h *reactorHost) sourceDrivenLocked(key string) bool {
	_, ok := h.sourceDriven[key]
	return ok
}
