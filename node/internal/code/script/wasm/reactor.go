package wasm

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// reactorConfigGlobal is the globals key a caller uses to pass the reactor
// config (cleansing/tagging rules) that drives the two-phase init. When present
// it is applied via ensurePool before eval; when absent the engine reuses the
// currently-active pool (or errors if never configured).
//
// Deprecated: this legacy globals path is kept because embedded/SDK callers and
// existing tests still use it, but it is unreachable in production — the
// production path is $supplies + a declared dependency edge + SupplyConsumer
// (supply_consumer.go), which flips a module to configFromSource instead of
// reading $config per call. Task 18 adds the ScriptNode.Execute end-to-end
// regression for the production path.
const reactorConfigGlobal = "$config"

func init() {
	engine.Register("wasm", "wazero-reactor", func() engine.Engine { return sharedReactorEngine })
	// Absorb the wasm cold start at startup instead of on the first request:
	// opening the runtime resolves the on-disk compilation cache and
	// instantiates WASI, and any pre-registered module is compiled and its pool
	// warmed here rather than under a request deadline.
	engine.RegisterWarmer(sharedReactorEngine.warmup)
}

// sharedReactorEngine is the process-wide reactor engine facade. It routes each
// Execute to the per-module reactorEngine owned by sharedReactorHost.
var sharedReactorEngine = &reactorFacade{host: sharedReactorHost}

// reactorFacade adapts the pooled reactor host to the engine.Engine interface.
// engine.Lookup hands callers this stateless facade; the real per-module state
// lives in reactorHost.engines.
type reactorFacade struct {
	host *reactorHost
}

func (f *reactorFacade) Name() string { return "wasm/wazero-reactor" }

// Execute runs one input through the reactor pool for the given module.
//
// When a module is source-driven (configFromSource: a registered supply
// consumer), globals["$config"] is ignored and the active pool (maintained by a
// supply content change) is used directly. The availability ladder decides
// whether to serve: Fresh and Stale both serve from last-good, only Unavailable
// (never successfully configured) fails the call.
//
// When the module is not source-driven, the legacy path applies: rules are
// passed via globals["$config"] and ensurePool is called with a sha256
// equality check.
func (f *reactorFacade) Execute(ctx context.Context, src engine.Source, globals map[string]any, _ engine.Helpers) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wasm/wazero-reactor: %w", err)
	}

	e, err := f.host.engineForSource(ctx, src)
	if err != nil {
		return nil, err
	}

	// Source-driven path: no lock, no per-call sha256 of config bytes.
	if e.configFromSource.Load() {
		switch e.availability() {
		case AvailUnavailable:
			// Reachable only under require_ready:false — the readiness gate keeps
			// traffic away otherwise. Fail rather than eval with no rules: an
			// unconfigured guest would pass everything through untagged, which is
			// a silent data-quality incident.
			return nil, types.NewTransientError("wasm.unconfigured",
				"wasm reactor: no active pool (supply content never applied); retryable")
		case AvailStale, AvailFresh:
			// Both serve. Stale is a last-good state, which is correct behaviour —
			// it is reported via supply_age_seconds, not by refusing traffic.
			//
			// Sampled once per call rather than on a separate ticker: this is the
			// only signal that exposes a source which stopped updating (gen and
			// revision stay put while a source keeps failing), and a source-driven
			// module's Execute calls are frequent enough that per-call sampling
			// costs two atomic loads — ConfigAge and obs() — with no lock and no
			// allocation. obs() must stay lock-free for this to hold; see the
			// measurement in observer.go.
			obs().OnConfigAge(ctx, e.ConfigAge())
		}
		input := stripConfig(globals)
		inputBytes, err := encodeStdin(input)
		if err != nil {
			return nil, err
		}
		return f.evalFromPool(ctx, e, inputBytes)
	}

	// Legacy path: config from globals["$config"].
	cfgRaw, input := splitConfig(globals)
	cfgBytes, err := normalizeConfig(cfgRaw)
	if err != nil {
		return nil, err
	}
	if err := e.ensurePool(ctx, cfgBytes, defaultPoolSize()); err != nil {
		return nil, err
	}

	inputBytes, err := encodeStdin(input)
	if err != nil {
		return nil, err
	}
	return f.evalFromPool(ctx, e, inputBytes)
}

// ConfigGenerationKey is the output key carrying the content version that
// produced a result. It is the SupplyResource revision — server-side and
// monotonic, therefore comparable across runners.
//
// Cross-runner pool swaps are not synchronized (worst case a 10-20s skew,
// which at 12000 msg/s means 120-240k records tagged by a mix of two rule
// versions). The design does not try to close that window: closing it needs a
// global barrier that stops the whole stream, and no comparable system does
// that. It makes the skew TRACEABLE instead — a warehouse can group by
// (key, config_generation), find rows produced by an older version, and
// recompute exactly those.
const ConfigGenerationKey = "config_generation"

// evalFromPool borrows an instance, runs eval, and handles doom/return.
func (f *reactorFacade) evalFromPool(ctx context.Context, e *reactorEngine, inputBytes []byte) (any, error) {
	inst, pool, err := e.borrow(ctx)
	if err != nil {
		return nil, err
	}
	out, doomed, err := inst.evalOnce(ctx, inputBytes)
	if err != nil {
		if doomed {
			e.doom(ctx, pool, inst)
		} else {
			e.giveBack(ctx, pool, inst)
		}
		return nil, err
	}
	e.giveBack(ctx, pool, inst)

	decoded, err := decodeStdout(out)
	if err != nil {
		return nil, err
	}
	// Stamp the content version onto the result. pool — not e.active — is the
	// authority: a swap may have landed while this message was in flight, and
	// the honest answer is the version that actually produced this output.
	return annotateGeneration(decoded, pool.revision), nil
}

// annotateGeneration adds the config generation to a JSON-object result. A
// non-object result (array, scalar, nil) is returned untouched: adding a key
// would change its shape, and the guest's output type is part of its
// contract.
//
// A zero revision is still stamped: it distinguishes "produced by the legacy
// globals path" from "field absent because this host predates the field".
func annotateGeneration(v any, revision uint64) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	m[ConfigGenerationKey] = revision
	return m
}

// warmup is the engine.Warmer. It opens the wazero runtime — which resolves the
// on-disk compilation cache and instantiates WASI — and then compiles and warms
// a pool for every module registered via Prewarm.
//
// Warm-up failures are returned for the caller to log; they are not fatal, since
// Execute warms lazily on the Prewarm path.
func (f *reactorFacade) warmup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f.host.runtime(ctx) // resolve disk cache + WASI once, off the request path

	// Prewarm entries (legacy path — config from registration).
	for _, m := range f.host.prewarmModules() {
		e, err := f.host.engineForCode(ctx, m.code)
		if err != nil {
			return fmt.Errorf("wasm/wazero-reactor: warmup: %w", err)
		}
		cfgBytes, err := normalizeConfig(m.cfg)
		if err != nil {
			return fmt.Errorf("wasm/wazero-reactor: warmup: %w", err)
		}
		if err := e.ensurePool(ctx, cfgBytes, defaultPoolSize()); err != nil {
			return fmt.Errorf("wasm/wazero-reactor: warmup: %w", err)
		}
	}
	return nil
}

// Prewarm registers a module (base64 wasm, same form as ScriptNode code) and its
// config to be compiled and pooled by engine.Warmup at process start, rather
// than lazily on the first request. Call it before Warmup — typically during
// runner/server bootstrap once the deployed workflow's wasm module is known.
//
// Registering the same code twice replaces the config, so a caller can re-issue
// it when the config changes without accumulating entries.
func Prewarm(code string, cfg any) {
	sharedReactorHost.addPrewarm(code, cfg)
}

// CompileModule eagerly compiles a wasm module (base64 code string) so the
// reactor engine cache is primed. It is used by resolveArtifacts to ensure the
// engine exists before supply consumers attempt to configure it. Unlike Prewarm,
// which defers compilation to the next Warmup() call, this compiles immediately.
func CompileModule(ctx context.Context, code string) error {
	_, err := sharedReactorHost.engineForCode(ctx, code)
	return err
}

// splitConfig separates the reactor config from the eval input globals. The
// config lives under reactorConfigGlobal; the remaining keys are the input map
// handed to the guest as the eval environment.
func splitConfig(globals map[string]any) (cfg any, input map[string]any) {
	if globals == nil {
		return nil, map[string]any{}
	}
	cfg = globals[reactorConfigGlobal]
	if cfg == nil {
		return nil, globals
	}
	input = make(map[string]any, len(globals)-1)
	for k, v := range globals {
		if k == reactorConfigGlobal {
			continue
		}
		input[k] = v
	}
	return cfg, input
}

// stripConfig removes the $config key from globals for the source-driven path,
// where config is not sourced from globals.
func stripConfig(globals map[string]any) map[string]any {
	if globals == nil {
		return map[string]any{}
	}
	if _, has := globals[reactorConfigGlobal]; !has {
		return globals
	}
	input := make(map[string]any, len(globals)-1)
	for k, v := range globals {
		if k == reactorConfigGlobal {
			continue
		}
		input[k] = v
	}
	return input
}

// isBatchSkippable classifies an Execute error as either skippable (a single
// record's fault, instance still clean) or batch-fatal (doomed instance, host
// infrastructure failure, or context cancellation — continuing would produce a
// silently short result or evaluate against a suspect instance).
//
// Only a *reactorEvalError with a non-doom code is skippable. Those codes are
// guest-classified per-record failures (errDecode, errUnconfigured, errConfig,
// errOutput) where evalFromPool returned the instance to the pool unharmed.
// Everything else — bare fmt.Errorf from alloc/write/eval traps (doomed=true in
// evalOnce but consumed by evalFromPool before we see it), errEval itself, or
// any error from borrow/config/encode — is batch-fatal.
func isBatchSkippable(err error) bool {
	var evalErr *reactorEvalError
	if !errors.As(err, &evalErr) {
		// Not a guest-classified error at all: host-side trap, borrow failure,
		// config error, encode error, etc. All batch-fatal.
		return false
	}
	// errEval is the doom code (pool.go:87). The instance was torn down.
	return evalErr.code != errEval
}

// ExecuteBatch evaluates each record through the reactor pool in a host-side
// loop, one Execute call per record.
//
// The guest contract is deliberately unchanged: the guest still sees exactly the
// expression environment it always saw, so the rules keep working and
// isEngineRoot's "$"-prefix leak guard keeps holding. The proper abstraction is
// a body sub-graph with per-item downstream fan-out; engine/expand.go's
// ExecuteBatch is still a pass-through stub, so this loop stands in for it.
// TECH DEBT, not the end state.
//
// A record whose eval fails is SKIPPED, not fatal: one malformed record must not
// invalidate the whole batch (the same rule the guest applies internally, see
// compiledRule.matches). A doomed instance IS fatal — that is a host-level
// failure, and letting the batch fail means the offsets stay uncommitted and
// Kafka redelivers, which is clean because nothing downstream has run yet.
func (f *reactorFacade) ExecuteBatch(ctx context.Context, src engine.Source, records []any, globals map[string]any) ([]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wasm/wazero-reactor: batch: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	out := make([]any, 0, len(records))
	for _, rec := range records {
		perRecord := engine.BuildRecordGlobals(globals, rec)
		res, err := f.Execute(ctx, src, perRecord, engine.DefaultHelpers())
		if err != nil {
			if isBatchSkippable(err) {
				continue
			}
			// Batch-fatal: doomed instance (alloc/write/eval trap or errEval),
			// context cancellation, borrow failure, config error, etc.
			return nil, err
		}
		out = append(out, res)
	}
	return out, nil
}
