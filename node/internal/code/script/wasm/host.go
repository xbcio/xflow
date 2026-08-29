package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// EngineIdleTTLEnv overrides how long a compiled wasm module may sit unused
// before it is reclaimed. Accepts any time.ParseDuration value; "0" disables
// reclamation and restores the insert-only behaviour this change replaced.
const EngineIdleTTLEnv = "XFLOW_WASM_ENGINE_IDLE_TTL"

// defaultEngineIdleTTL is fifteen minutes.
//
// The floor is a human rollback: an operator who publishes a bad module notices
// and rolls back on a scale of minutes, and a rollback inside the window costs
// nothing because the engine is still resident. The ceiling is the leak this
// exists to bound — every resident module holds its compiled form plus
// GOMAXPROCS instances, each of which is recycled at a 12 MiB memory high
// water mark, so a handful of stragglers is hundreds of MB.
//
// Reclaiming too eagerly is not free either: the next message pays a recompile
// (~82 ms warm off the on-disk compile cache) plus a pool rebuild. That cost
// lands on one message of a workflow that has been silent for a quarter of an
// hour, which is the right place for it.
const defaultEngineIdleTTL = 15 * time.Minute

// engineIdleTTLFromEnv resolves the configured idle TTL, falling back to
// defaultEngineIdleTTL on an unset, empty, negative, or unparseable value.
func engineIdleTTLFromEnv() time.Duration {
	raw, ok := os.LookupEnv(EngineIdleTTLEnv)
	if !ok || raw == "" {
		return defaultEngineIdleTTL
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		// Falling back to the default rather than to 0: a typo must not silently
		// restore the unbounded growth. Warned rather than silent — cacheBudget
		// (cache.go) takes the same stance on its own malformed-env-var case, and
		// an operator who typo'd this value needs to find out some way other than
		// noticing 15m of resident-engine growth never bounded.
		slog.Warn("wasm: ignoring malformed engine idle TTL, using the default",
			"env", EngineIdleTTLEnv, "value", raw, "default", defaultEngineIdleTTL, "error", err)
		return defaultEngineIdleTTL
	}
	return d
}

// reactorHost owns the process-wide wazero runtime for reactor-model guests and
// the per-module reactor engines. It mirrors wazeroEngine's runtime setup
// (WithCloseOnContextDone + memory cap + WASI) but instantiates modules as
// resident reactors (_initialize) rather than one-shot commands (_start).
type reactorHost struct {
	rt     wazero.Runtime
	rtOnce sync.Once

	mu      sync.Mutex
	engines map[string]*reactorEngine // keyed by module sha256

	// engineList is a snapshot of engines' values, republished under mu on every
	// insert AND on every reclaim. It exists so the per-message path can walk
	// every engine without taking mu: reportConfigAge runs on every Execute, and
	// observer.go's measurement (69.5 ns for a guarded read against 2.2 ns for an
	// atomic load at 8-way parallelism) is exactly why that path must stay
	// lock-free.
	//
	// engines is no longer insert-only, so the old safety argument ("a reader
	// holding a stale snapshot sees a subset, never a freed engine") no longer
	// carries itself. What replaces it: Go has no free. A reclaimed engine stays
	// a valid *reactorEngine for as long as any snapshot holds it, and every
	// field a snapshot reader touches — ConfigAge via lastSwapAt, Generation and
	// rules via active — reads an atomic that a reclaimed engine leaves in a
	// coherent terminal state (active nil, lastSwapAt frozen). The single
	// non-memory resource, the compiled module, is closed only after the engine
	// is out of both engines and a freshly republished engineList, so no reader
	// can reach a cm after Close.
	//
	// A module missing from a snapshot for the microseconds before republication
	// cannot hide a stale source: on insert it has just been created, so its age
	// is zero either way; on reclaim it is going away and its age stops being a
	// fact about anything serving traffic.
	engineList atomic.Pointer[[]*reactorEngine]

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

	// engineIdleTTL is how long an engine may sit unused before
	// sweepEnginesAsync reclaims it. Set once at construction from
	// engineIdleTTLFromEnv; a test may override it directly since reactorHost is
	// only ever built via newReactorHost in this package.
	engineIdleTTL time.Duration
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
		engines:       map[string]*reactorEngine{},
		codeCache:     c,
		prewarm:       map[string]prewarmEntry{},
		sourceDriven:  map[string]struct{}{},
		engineIdleTTL: engineIdleTTLFromEnv(),
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
			if hit {
				h.touchLocked(e)
			}
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
	// The Get is under h.mu so the hit and its lastUsed stamp are one step. The
	// lock is not a cost worth avoiding here: this lookup compares the full
	// multi-MB key on every hit (198 µs, see codeCache's comment), so a mutex is
	// noise against it. Keeping the Get outside would leave the only resolution
	// path whose hit the sweep cannot see, which is exactly a lookup the reclaim
	// decision could race — the same trap publishEngine documents at the Dekker
	// crossing: narrowing a window is not closing it.
	h.mu.Lock()
	e, ok := h.codeCache.Get(code)
	if ok {
		h.touchLocked(e)
	}
	h.mu.Unlock()
	if ok {
		return e, nil
	}
	wasmBytes, err := decodeCode(code)
	if err != nil {
		return nil, err
	}
	key := moduleKeyOf(wasmBytes)
	e, err = h.engineForKey(ctx, key, wasmBytes)
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
		h.touchLocked(e)
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
		h.touchLocked(e)
		obs().OnModuleCompile(ctx, "hit")
		return e, nil
	}
	e := &reactorEngine{host: h, cm: cm}
	h.engines[key] = e
	h.touchLocked(e)
	h.republishEngineListLocked()
	obs().OnModuleCompile(ctx, "miss")
	// A miss is the only event that adds a file to the on-disk cache, so it is
	// the only one that can push the directory over its budget.
	sweepCacheAsync()
	// A miss is also the only event that can GROW the resident engine set, so it
	// is the only thing (besides the timer below) that can make the set worth
	// sweeping.
	h.sweepEnginesAsync()
	return e, nil
}

// engineSweeping guards the engine sweep the way cache.go's guards the disk
// sweep: at most one in flight, and a sweep that arrives while another is
// running is dropped rather than queued. Dropping is correct — the next
// compile miss, or the timer sweepEnginesAsync re-arms, will run one.
var engineSweeping atomic.Bool

// sweepEnginesAsync runs one reclamation pass off the caller's goroutine and
// re-arms itself while any engine remains resident.
//
// Event-driven from the compile-miss path (a miss is the only thing that ADDS
// an engine, so it is the only thing that can grow the resident set) plus a
// self-rearming timer, because event-driven alone has a hole: uploading N
// distinct modules inside one ttl fires N sweeps that all find nothing old
// enough, and if uploads then stop, nothing ever fires again. The timer closes
// exactly that case and stops re-arming once the map is empty, so an idle
// process carries no timer at all.
func (h *reactorHost) sweepEnginesAsync() {
	if h == nil || h.engineIdleTTL <= 0 {
		return
	}
	if !engineSweeping.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer engineSweeping.Store(false)
		ctx := context.Background()
		h.reclaimIdleEngines(ctx, h.engineIdleTTL)
		h.mu.Lock()
		remaining := len(h.engines)
		h.mu.Unlock()
		obs().OnEngineCount(ctx, remaining)
		if remaining > 0 {
			// A quarter of the ttl, so an engine is reclaimed within ttl*1.25 of
			// going idle rather than up to ttl*2 with a period of ttl.
			time.AfterFunc(h.engineIdleTTL/4, h.sweepEnginesAsync)
		}
	}()
}

// touchLocked records that e was just handed to a caller. Caller must hold h.mu
// — that is the whole point: the sweep reads lastUsed under the same lock, so an
// engine cannot be reclaimed in the window between a lookup finding it and the
// caller using it.
func (h *reactorHost) touchLocked(e *reactorEngine) {
	e.lastUsed.Store(time.Now().UnixNano())
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

// republishEngineListLocked refreshes the lock-free snapshot. Caller must hold
// h.mu. Cheap: engines is a handful of entries and this runs once per module
// compilation, never on a message path.
func (h *reactorHost) republishEngineListLocked() {
	list := make([]*reactorEngine, 0, len(h.engines))
	for _, e := range h.engines {
		list = append(list, e)
	}
	h.engineList.Store(&list)
}

// engineSnapshot returns every engine without taking h.mu. See engineList.
func (h *reactorHost) engineSnapshot() []*reactorEngine {
	if h == nil {
		return nil
	}
	if p := h.engineList.Load(); p != nil {
		return *p
	}
	return nil
}

// maxConfigAge returns the age of the STALEST active content across every
// engine, which is what xflow_supply_age_seconds must carry.
//
// The max, not this engine's own age, for the reason spelled out on
// reportReadyInstances: the metric is a gauge with no module-identity label, so
// each report REPLACES the series. Reporting per-engine made it "whichever
// module executed most recently" — with SAS's two modules resident and traffic
// interleaved, consecutive scrapes alternated between them. A module whose
// source froze hours ago was therefore visible in only about half of the
// scrapes, which is worse than either always or never: an alert on it flaps,
// and a flapping alert gets muted.
//
// Engines that never installed a pool are skipped rather than counted as zero.
// A zero from "nothing loaded yet" and a zero from "just refreshed" mean
// opposite things, and taking a max over the two would let a warming module
// mask a stale sibling.
func (h *reactorHost) maxConfigAge() time.Duration {
	var worst time.Duration
	for _, e := range h.engineSnapshot() {
		if age := e.ConfigAge(); age > worst {
			worst = age
		}
	}
	return worst
}

// activeConfigState returns the host-wide rule count and content revision the
// two config gauges carry. Both are aggregates for the same reason maxConfigAge
// is: the gauges behind them have no module-identity label.
//
// rules is the SUM across engines, so it answers "how many rules are resident in
// this process". It is -1 when ANY engine's config shape was not recognized:
// ruleCount's -1 means "unknown", and folding an unknown into a sum would
// publish a confident number that is quietly short by one module's worth.
//
// revision is the MINIMUM across engines that actually have one, so it answers
// "what is the oldest content still being served" — the conservative reading, and
// the one that makes a rollout look complete only once every module has moved.
// Engines on the legacy globals path carry revision 0, meaning "not from a
// SupplyResource"; they are excluded rather than dragging the minimum to zero.
// Zero is returned when no engine has a revision at all.
func (h *reactorHost) activeConfigState() (rules int, revision uint64) {
	sum, unknown := 0, false
	var minRev uint64
	for _, e := range h.engineSnapshot() {
		p := e.active.Load()
		if p == nil {
			continue
		}
		if p.rules < 0 {
			unknown = true
		} else {
			sum += p.rules
		}
		if p.revision > 0 && (minRev == 0 || p.revision < minRev) {
			minRev = p.revision
		}
	}
	if unknown {
		return -1, minRev
	}
	return sum, minRev
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

// reclaimIdleEngines removes every engine that no resolution path has handed
// out for at least ttl, and returns how many it removed. A ttl of zero disables
// reclamation entirely and returns 0.
//
// Idle duration is the only criterion available. artifact_digest is
// boundary-evaluated per item, so a digest that looks retired can come back on
// the next message via a rollback; "this module will never be asked for again"
// is not a fact this process can learn. Reclamation therefore shrinks the
// resident set, it does not close the rollback window — which is why §4.2's
// registration key stays the digest.
//
// What it drops is the COMPILED ARTIFACT: the engines entry, the engineList
// snapshot slot, the codeCache memo, the instance pool, the wazero
// CompiledModule. What it deliberately keeps is INTENT: the sourceDriven marker
// and any supply consumer registration. See the two rulings in this change's
// plan — keeping them is the fail-closed direction, and the rebuild path
// (script.ensureWasmSupplyConsumers) already restores everything else on the
// next message.
func (h *reactorHost) reclaimIdleEngines(ctx context.Context, ttl time.Duration) int {
	if h == nil || ttl <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-ttl).UnixNano()

	h.mu.Lock()
	var doomed []*reactorEngine
	for key, e := range h.engines {
		if e.lastUsed.Load() > cutoff {
			continue
		}
		// A borrowed instance means a message is mid-eval on this engine. The
		// lastUsed stamp is taken under this same lock at lookup time, so the
		// only gap it leaves is between a caller receiving the engine and
		// reaching borrow — microseconds against a ttl of minutes. This check
		// closes that gap for anything already past borrow rather than leaving
		// it to the retry in reactor.go.
		if p := e.active.Load(); p != nil && p.inFlight() > 0 {
			continue
		}
		delete(h.engines, key)
		e.reclaimed.Store(true)
		doomed = append(doomed, e)
	}
	if len(doomed) > 0 {
		h.republishEngineListLocked()
		// Purge rather than evict per key: codeCache is keyed by the base64 code
		// string, and finding the entries that point at a doomed engine would
		// mean a full-length compare against every resident key (198 µs each, see
		// codeCache's comment) while holding h.mu. The whole cache is a memo —
		// dropping it costs the next inline-code caller one decode+hash, and
		// reclamation is a once-per-ttl event.
		h.codeCache.Purge()
	}
	h.mu.Unlock()

	// Teardown outside the lock: drainPool waits (bounded) and cm.Close is slow,
	// and neither may block a lookup. Safe to do unlocked precisely because the
	// engines are already unreachable — out of engines, out of engineList, out of
	// codeCache — so nothing can hand one to a new caller after this point.
	for _, e := range doomed {
		if old := e.active.Swap(nil); old != nil {
			e.drainPool(ctx, old)
		}
		if e.cm != nil {
			_ = e.cm.Close(ctx)
		}
	}
	return len(doomed)
}
