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

	// codeCache fronts engines for callers that supply no digest, keyed by the
	// base64 code string so the hot path skips a multi-MB base64-decode +
	// sha256 on every call. A miss falls through to engineForKey, whose sha256
	// dedup is the correctness backstop. Bounded LRU: module working set is
	// small.
	//
	// It is a memo only, and a poor one for a multi-MB module: a Go map hit must
	// confirm the stored key EQUALS the lookup key, and runtime.memequal walks
	// the full 9 MB unless the two strings share a backing array. Measured on an
	// M3, 198 µs per lookup against 22 ns when the headers coincide (see
	// module_resolution_bench_test.go). Production never shares a header — the
	// activation path encodes its own copy of the module and the artifact cache
	// may produce several more — so this path is the expensive one every time.
	//
	// engineForSource therefore prefers the digest a caller already holds and
	// reaches engines directly, comparing ~64 bytes instead of 9 MB. This cache
	// remains for the digest-less callers (inline `node.Script(b64)`, tests,
	// prewarm), where the alternative is a ~3 ms decode+hash per call.
	//
	// The registries below record FACTS about a module, where a split identity
	// is silent misbehaviour rather than a wasted cycle, so they key on
	// moduleKey instead.
	codeCache *lru.Cache[string, *reactorEngine]

	// prewarm holds modules registered via Prewarm to be compiled and pooled by
	// the warmer at startup. Keyed by moduleKey so re-registering the same module
	// updates its config instead of adding a duplicate — including when the two
	// registrations carry different base64 encodings of the same bytes. The entry
	// retains the original code string because warm-up needs it to reach
	// engineForCode. Guarded by mu.
	prewarm map[string]prewarmEntry

	// sourceDriven records modules whose config is meant to come from a supply
	// (rather than globals["$config"]) even when no engine has been
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

// registryKeyOrRaw is the key for the prewarm registry, whose entries are later
// CONSUMED by warm-up. On a decodable module it is the canonical moduleKey; on an
// undecodable one it falls back to the raw string.
//
// The fallback is what keeps warm-up's "an unusable module is reported, not
// silently dropped" contract (see TestPrewarm_BadModuleSurfacesError): warm-up
// calls engineForCode on the retained code string and surfaces the decode error
// there. Dropping the entry at registration instead would turn a reported
// misconfiguration into a module that never warms and nobody is told about.
// Registration on that path is a void call with no channel to report on.
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

// engineForSource is the hot-path entry: it resolves the reactor engine for one
// script source.
//
// When the caller knows the module's digest, the lookup is a 64-byte map probe
// against h.engines and nothing else — no decode, no hash, and no comparison
// against the multi-MB code string. That is the whole point of carrying a
// digest: see engine.Source and codeCache's comment for what the content-keyed
// lookup costs (198 µs per call, 32% of wasm time in a production profile).
//
// A digest that names no compiled engine yet falls through to the content path,
// which compiles it and publishes it under the same key — so the next call for
// the same digest hits. A malformed digest falls through the same way rather
// than failing: it is a naming defect in the caller, and refusing to run code
// the artifact store already handed us would turn it into an outage.
func (h *reactorHost) engineForSource(ctx context.Context, src engine.Source) (*reactorEngine, error) {
	if src.Digest != "" {
		if key, ok := moduleKeyFromDigest(src.Digest); ok {
			// engines and sourceDriven are read under ONE hold, for the reason
			// engineForCode spells out: the two sides cross, and a lookup split
			// across two holds can miss both. The creating goroutine publishes
			// its engine into h.engines BEFORE it resolves the flag, so an engine
			// found here may be one whose creator has not reached that step yet —
			// and a seed that landed before the engine existed found nothing to
			// flip. Resolving the flag here as well is what keeps a module from
			// serving a message on the legacy globals path, evaluating against no
			// rules and passing every record through untagged.
			h.mu.Lock()
			e, hit := h.engines[key]
			fromSource := hit && h.sourceDrivenLocked(key)
			h.mu.Unlock()
			if hit {
				// Load before Store: this runs on every message across every
				// worker, and an unconditional store to a shared atomic would
				// bounce its cache line between cores for a value that changes
				// once in a module's lifetime.
				if fromSource && !e.configFromSource.Load() {
					e.configFromSource.Store(true)
				}
				obs().OnModuleCompile(ctx, "hit")
				return e, nil
			}
		}
	}
	return h.engineForCode(ctx, src.Code)
}

// engineForCode resolves the reactor engine from the base64 code string alone,
// skipping the multi-MB base64-decode + sha256 on a codeCache hit. On a miss it
// decodes once, delegates to engineForKey (sha256 dedup), and memoizes by code
// string.
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
	h.publishEngine(key, e, code)
	return e, nil
}

// engineForBytes is engineForCode for callers that already hold the raw module
// bytes, so a multi-MB module is never base64-encoded just to be decoded again.
//
// It deliberately does NOT populate codeCache: that cache is keyed by the base64
// string, and this caller has none. Manufacturing one would allocate a ~9 MB
// string whose only use is being compared in full on every subsequent lookup
// (see codeCache's comment: 198 µs per hit). Callers holding bytes hold a digest
// too, and the digest path reaches h.engines directly.
//
// What it must NOT skip is the source-driven resolution -- see publishEngine.
func (h *reactorHost) engineForBytes(ctx context.Context, wasmBytes []byte) (*reactorEngine, error) {
	key := moduleKeyOf(wasmBytes)
	e, err := h.engineForKey(ctx, key, wasmBytes)
	if err != nil {
		return nil, err
	}
	h.publishEngine(key, e, "")
	return e, nil
}

// publishEngine resolves a freshly created engine's config source and memoizes
// it under code, as one atomic step, under the same lock the registration path
// holds. An empty code skips the memo (the bytes path has no base64 string).
//
// Registration and engine creation are a Dekker-style crossing: each side
// publishes its own state and then looks for the other's. Registration
// records the intent (sourceDriven) and then flips any already-cached engine;
// creation reads the intent and then caches the engine. Interleaved, both
// lookups can miss -- registration's engines lookup finds nothing because the
// engine is not published yet, and the intent read already ran before the
// write landed. The engine then stays on the globals path forever, evaluating
// with no rules at all, and nothing later repairs it.
//
// A double-check after the Add only narrows that window; it does not close
// it, and being nanoseconds wide it is unreachable by any test -- untestable
// code guarding a real defect is worse than no guard. One mutex covering
// both steps closes it by construction: whichever side takes h.mu second
// necessarily observes what the first published.
//
// Both entry points route through here rather than each resolving the flag
// themselves. A second copy of this sequence is how the bytes path would
// silently regress to the globals path: the omission compiles, runs, and
// produces correct-looking traffic that matches no rules.
//
// It also runs when engineForKey returned an engine that already existed, not
// one it just created. That is deliberate and idempotent: re-reading the flag
// costs one map probe, and the alternative -- resolving it only on creation --
// would drop the flip for an engine whose registration arrived between its
// creation and this call.
func (h *reactorHost) publishEngine(key string, e *reactorEngine, code string) {
	h.mu.Lock()
	fromSource := h.sourceDrivenLocked(key)
	if code != "" {
		h.codeCache.Add(code, e)
	}
	h.mu.Unlock()

	if fromSource {
		e.configFromSource.Store(true)
	}
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

// seedSourceDrivenByKey records that a module's config comes from a supply, and
// flips its engine if one already exists. It creates nothing: when
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

// reportReadyInstances reports the host's TOTAL resident ready instances,
// summed across every engine.
//
// Why the sum and not this engine's own size: xflow_wasm_instance_total is a
// gauge carrying only a "state" label, so each report REPLACES the series.
// Reporting per-engine made the series read "the last engine to swap" — with
// SAS's two modules resident it read one pool's width while two pools existed
// (a live run read 8 with 14-20 goroutines observed inside wazero on an 8-core
// machine). That is worse than no metric: a missing number makes an operator
// look, and a plausible-but-wrong one makes them stop looking. An entire round
// of queueing arithmetic was built on the hidden half.
//
// Why not a per-module label instead: Observer's contract forbids it in as many
// words — implementations "must never use content, hashes, or execution IDs as
// labels" — and a module's only identity here IS its sha256.
//
// Lock order: callers hold e.mu (swapConfig) and this takes h.mu. That is the
// existing direction — engineForKey and friends take h.mu and never reach for
// an engine's mu — so it introduces no cycle. Keep it that way.
func (h *reactorHost) reportReadyInstances(ctx context.Context) {
	if h == nil {
		// An engine built without a host has no siblings to sum over. Production
		// engines always come from engineForKey, which sets host; the nil case
		// exists only for bare &reactorEngine{} values used as cache filler in
		// tests. Reporting nothing beats panicking on a metrics call.
		return
	}
	h.mu.Lock()
	total := 0
	for _, e := range h.engines {
		if p := e.active.Load(); p != nil {
			total += p.size
		}
	}
	h.mu.Unlock()
	obs().OnInstanceCount(ctx, "ready", total)
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
