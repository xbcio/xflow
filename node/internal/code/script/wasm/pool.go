package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// Reactor ABI error codes. Must stay in sync with testdata/reactor/main.go and
// docs/design/WASM-ENGINE-POOLING.md §4.2. configure/eval return >=0 on success
// (rule count / output length) or one of these on failure.
const (
	errDecode       int32 = -1 // input JSON decode failed         → call-level, instance retained
	errUnconfigured int32 = -2 // eval before configure            → call-level, instance retained
	errConfig       int32 = -3 // rule compile failed              → fresh instance discarded (§6.3)
	errEval         int32 = -4 // evaluation panicked              → instance Doomed, rebuilt (§5.6)
	errOutput       int32 = -5 // output exceeds cap               → call-level, instance retained
)

// reactorABIVersion is the ABI contract version the host speaks. A guest whose
// abi_version() disagrees is refused at pool build (docs §4.1 negotiation). It
// is a var, not a const, only so the negotiation test can simulate a guest built
// against a different version; production never reassigns it.
var reactorABIVersion int32 = 1

// maxEvalsPerInstance limits how many successful evals one resident instance
// may serve before it is proactively recycled.
//
// It is a BACKSTOP, not the working bound. recycleMemoryHighWater is what
// actually retires instances on SAS-shaped traffic; this count exists for a
// guest whose memory never climbs at all, so that no instance runs unboundedly
// long without a fresh start.
//
// The history is worth keeping, because two successive calibrations of this
// constant were wrong in the same way. An early measurement reported crash
// points scaling per-BYTE (crash_iter × input_size ≈ 140 MB). Re-measured
// 2026-08-20 with one freshly built runtime per arm, that law did not
// reproduce: two sizes survived 30 000 evals where it predicted a crash, and
// the products of the arms that did trap spanned more than 3x. The probe behind
// the original figures had shared one runtime across its arms, so each arm's
// crash point was a function of the arms before it. The replacement claim --
// "8 000 has never been observed to be too high" -- then held only because
// nothing had measured it against real traffic. TestGuestMemoryCeiling does,
// and 8 000 is unreachable at three of four sizes: see recycleMemoryHighWater.
//
// Raising this value: safe only if WithMemoryLimitPages rises proportionally.
// Lowering it: costs throughput (more rebuilds), never correctness.
const maxEvalsPerInstance = 8_000

// recycleMemoryHighWater is the linear-memory size past which an instance is
// retired: three quarters of the 16 MiB cap, i.e. 12 MiB.
//
// Wasm linear memory never shrinks, so mem.Size() is a monotonic high-water
// mark of everything the guest's Go heap has ever needed at once. That makes it
// the one signal that measures the resource actually being exhausted, rather
// than a proxy for it — and the proxies do not work. TestGuestMemoryCeiling
// swept the real SAS apisix distribution, one freshly built runtime per size
// bucket, and neither eval count nor cumulative bytes separates the survivors
// from the traps:
//
//	bucket  mean rec.  plateau   crossed 12 MiB   outcome
//	p25       1 683 B  10.25 MiB  never           survived 40 000 evals /  64.2 MiB fed
//	p50       2 629 B  11.00 MiB  never           survived 40 000 evals / 100.3 MiB fed
//	p75       5 422 B  11.00 MiB  eval 5 373      TRAPPED at eval 5 464 /  28.2 MiB fed
//	p95      55 776 B  11.00 MiB  eval   465      TRAPPED at eval   477 /  24.6 MiB fed
//
// An eval count cannot be calibrated: the trap points differ 11x. Cumulative
// bytes cannot either: a survivor reached 100.3 MiB while a trap came at 24.6.
// What every arm shares is the SHAPE of its memory curve — a climb to a plateau
// at 10.25-11.00 MiB within the first ~150 evals, then either steady state
// forever, or a runaway that covers the remaining 5 MiB in a dozen evals and
// traps. 12 MiB sits in the gap: above every observed plateau, below every
// observed trap.
//
// So the cost is asymmetric in the right direction. At p25 and p50 — the bulk of
// production traffic — this never fires, and the instance runs to
// maxEvalsPerInstance as before. It fires only on the arms that were going to
// trap anyway, 91 evals early at p75 and 12 early at p95, converting a failed
// message into a planned 62 ms asynchronous rebuild.
//
// Verified by rerunning the same sweep with the bound in place. Every bucket
// completed 40 000 evals with ZERO failed evals, and the two bounds divided the
// work the way the table predicts:
//
//	bucket  recycles / 40 000 evals   by
//	p25     5                         max_evals        (memory never crossed 12 MiB)
//	p50     5                         max_evals        (idem)
//	p75     7                         memory_high_water (one per ~5 700 evals)
//	p95     87                        memory_high_water (one per ~460 evals)
//
// p95 is the case worth reading twice: 87 planned rebuilds against a trap every
// 477 evals before the change. The rebuild RATE is essentially unchanged. What
// changed is that a rebuild no longer costs a message.
//
// Two things this does NOT protect against. A single eval that allocates the
// whole remaining 4 MiB in one step still traps, because the check runs after
// the eval; no high-water threshold can prevent that, and such a record would
// trap a cold instance too. And if some future guest's steady state settles
// ABOVE 12 MiB, every instance recycles shortly after reaching plateau — a
// rebuild every ~250 evals, which is a throughput cost, not a correctness one.
// The failure mode is soft in both directions, which is why the fraction is
// three quarters and not something closer to the cap.
//
// Expressed as a fraction so it tracks WithMemoryLimitPages: a larger cap moves
// the threshold up with it, leaving the same proportional headroom.
const recycleMemoryHighWater = engine.DefaultWasmMemoryPages * wasmPageBytes * 3 / 4

// wasmPageBytes is the wasm spec's fixed page size. It is not configurable.
const wasmPageBytes = 64 << 10

// pooledInstance is one resident reactor instance: an instantiated module with
// _initialize already run and configure already applied. It carries the export
// handles so the hot path avoids per-call lookups.
//
// A pooledInstance belongs to exactly one activePool; its config generation is
// the pool's gen. It is only ever driven by one goroutine at a time — the pool
// guarantees single-borrower ownership (constraint #2: concurrent calls on one
// api.Module are a process-fatal fault).
type pooledInstance struct {
	mod    api.Module
	mem    api.Memory
	alloc  api.Function
	eval   api.Function
	outPtr api.Function
	// outLen and td are resolved once at instantiation rather than per call.
	// outLen is recommended-but-optional and td ("teardown") is optional in the
	// ABI (docs §4.1), so either may be nil — callers must check.
	outLen api.Function
	td     api.Function

	// evalCount tracks successful evals on this instance. When it reaches
	// maxEvalsPerInstance the instance is proactively recycled to prevent the
	// guest Go runtime from exhausting its memory cap (see maxEvalsPerInstance).
	// It is not guarded: the pool guarantees single-borrower ownership.
	evalCount int64

	// recycleCause names the bound that retired this instance, for the recycle
	// metric. It is set by evalOnce at the moment it returns a planned recycle
	// and read by evalFromPool immediately after, on the same goroutine — the
	// alternative was widening evalOnce's already-overloaded return.
	//
	// The distinction is the point of the metric: memory_high_water firing means
	// the guest is on the runaway path this pool exists to intercept, while
	// max_evals firing means it never climbed at all. One number for both would
	// hide which.
	recycleCause string
}

// eval runs one input through the resident instance. It returns the decoded
// output bytes on success, or (nil, doomed, err) on failure where doomed
// signals the instance must be discarded and rebuilt rather than returned to
// the pool.
//
// doomed carries TWO meanings and callers must read err to tell them apart:
//
//	err != nil, doomed=true   the instance FAILED and must be discarded
//	err == nil, doomed=true   the eval succeeded but the instance has reached
//	                          maxEvalsPerInstance and is due for planned recycle
//
// The overload is deliberate — a planned recycle retires the instance by the
// same mechanism a failure does — but it means "doomed" alone never answers
// whether anything went wrong. Any new caller that branches on doomed before
// checking err will report healthy recycles as errors.
//
// Memory ABI (docs §4.3): alloc(len) → ptr; write input; eval(len) → n;
// n<0 → error (out buffer holds structured detail, read for logging only);
// n>=0 → read out_ptr()[:n] and copy out (mem.Read returns a view into linear
// memory that the next borrower would clobber).
func (in *pooledInstance) evalOnce(ctx context.Context, input []byte) (out []byte, doomed bool, err error) {
	ptr, err := in.alloc.Call(ctx, uint64(len(input)))
	if err != nil {
		// A failed alloc means the module was closed (timeout) or trapped.
		return nil, true, classifyHostFault(ctx, fmt.Errorf("wasm reactor: alloc: %w", err))
	}
	if len(input) > 0 && !in.mem.Write(uint32(ptr[0]), input) {
		// Not a context-dependent failure: the input does not fit the pointer
		// the guest just handed out. The same input hits the same wall on every
		// retry, so mark it permanent unconditionally.
		return nil, true, &permanentHostFault{
			err: fmt.Errorf("wasm reactor: write input out of range"),
		}
	}

	r, err := in.eval.Call(ctx, uint64(len(input)))
	if err != nil {
		// Timeout (WithCloseOnContextDone) or trap → instance permanently
		// closed (constraint #4). Doom it.
		return nil, true, classifyHostFault(ctx, fmt.Errorf("wasm reactor: eval: %w", err))
	}

	n := int32(r[0])
	if n < 0 {
		detail := in.readOut(ctx, -1)
		// The guest's own account of why. It goes to the log and NOT into the
		// error: Error() reaches a map batch's _error entry, which is node
		// output and can be persisted, and a guest may quote its input when
		// explaining a rejection. See evaldetail.go.
		logEvalDetail("eval", n, detail)
		// ERR_EVAL is a guest-side panic/eval fault: the instance may hold
		// polluted global state, so doom it (§5.6). Other negatives are
		// call-level (bad input / oversize output); the instance stays clean.
		doom := n == errEval
		return nil, doom, &reactorEvalError{code: n, detail: detail}
	}

	out = in.readOut(ctx, n)
	in.evalCount++
	// Proactive recycle. Returning doomed=true with err=nil is the "planned
	// replacement" path: evalFromPool forwards this result to the caller
	// unchanged and replaces the instance asynchronously in the background.
	//
	// The memory check is the one that fires on real traffic. It is read AFTER
	// the eval rather than before, because what matters is the high-water mark
	// this eval left behind: an instance that has just climbed past the plateau
	// is on the runaway path and has roughly a dozen evals left in it, so the
	// next borrower must not get it. See recycleMemoryHighWater.
	//
	// Checked on every eval rather than sampled: wazero's Memory.Size() is
	// len(buffer) behind an interface call, against an eval measured in
	// milliseconds.
	switch {
	case in.mem.Size() >= recycleMemoryHighWater:
		in.recycleCause = "memory_high_water"
		return out, true, nil
	case in.evalCount >= maxEvalsPerInstance:
		in.recycleCause = "max_evals"
		return out, true, nil
	}
	return out, false, nil
}

// classifyHostFault stamps a doomed-instance error as permanent unless the
// context is what killed it.
//
// Both outcomes look identical at this layer: WithCloseOnContextDone tears the
// module down on a deadline exactly the way a trap does, and both surface as a
// wazero call error on a now-closed module. But they need opposite retry
// answers, and "the instance was doomed" cannot tell them apart.
//
//   - A trap is a property of the input. A malformed message that drives the
//     guest into an out-of-bounds access or an unreachable will do it again on
//     every redelivery. Left unmarked, the Kafka batch path reads the failure
//     as transient (GroupExecResult.Deterministic is false), refuses to admit
//     the batch, and the broker redelivers the same bytes forever -- the
//     partition stops advancing and every message queued behind it stalls with
//     it.
//   - A timeout is a property of the environment: a loaded host, a deadline set
//     too tight. Retrying can well succeed, and marking it permanent would
//     commit the offset and silently discard real messages.
//
// ctx.Err() is the signal, not the error text: it is set precisely when the
// context is the cause, and it does not depend on wazero's wording.
//
// The sentinel is joined rather than wrapped so err's own text stays the head
// of the message: a wasm trap carries the guest stack trace, which is the whole
// diagnostic value of the error, and burying it under "permanent error" would
// cost more than the marker is worth in a log line.
func classifyHostFault(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return err
	}
	return &permanentHostFault{err: err}
}

// permanentHostFault marks a host-side wasm fault as non-retryable while
// keeping the underlying error's text and unwrap chain intact.
type permanentHostFault struct{ err error }

func (e *permanentHostFault) Error() string { return e.err.Error() }

// Unwrap returns both the wrapped error and the sentinel so errors.Is finds
// types.ErrPermanent and errors.As still reaches anything the wasm layer
// wrapped underneath.
func (e *permanentHostFault) Unwrap() []error { return []error{e.err, types.ErrPermanent} }

// readOut copies the guest's output buffer. n<0 asks the guest for the length
// via out_len (used for error detail, where the failing call's return value was
// an error code rather than a length); n>=0 reads exactly n bytes. The returned
// slice is a fresh copy safe to hand upward — mem.Read returns a view into
// linear memory that the next borrower would clobber.
//
// out_len is optional in the ABI, so a guest without it simply yields no error
// detail; the classified error code still reaches the caller.
func (in *pooledInstance) readOut(ctx context.Context, n int32) []byte {
	op, err := in.outPtr.Call(ctx)
	if err != nil || op[0] == 0 {
		return nil
	}
	length := n
	if n < 0 {
		if in.outLen == nil {
			return nil
		}
		lr, err := in.outLen.Call(ctx)
		if err != nil {
			return nil
		}
		length = int32(lr[0])
	}
	if length <= 0 {
		return nil
	}
	view, ok := in.mem.Read(uint32(op[0]), uint32(length))
	if !ok {
		return nil
	}
	cp := make([]byte, len(view))
	copy(cp, view)
	return cp
}

// teardown invokes the optional graceful-stop hook then closes the module.
// Safe to call on an already-doomed instance: the teardown call on a closed
// module errors harmlessly and Close is idempotent.
func (in *pooledInstance) teardown(ctx context.Context) {
	if in.td != nil {
		_, _ = in.td.Call(ctx)
	}
	_ = in.mod.Close(ctx)
}

// reactorEvalError carries a classified guest failure. The detail is for
// host-side logging only and is never echoed to callers (secure-coding policy
// on sensitive-info disclosure).
type reactorEvalError struct {
	code   int32
	detail []byte
}

func (e *reactorEvalError) Error() string {
	switch e.code {
	case errDecode:
		return "wasm reactor: input decode failed"
	case errUnconfigured:
		return "wasm reactor: instance not configured"
	case errConfig:
		return "wasm reactor: config compile failed"
	case errEval:
		return "wasm reactor: evaluation error"
	case errOutput:
		return "wasm reactor: output exceeds limit"
	default:
		return fmt.Sprintf("wasm reactor: error code %d", e.code)
	}
}

// activePool is the set of instances for one config generation. A config change
// builds a whole new activePool and swaps the pointer (B-plan, docs §6.3), so a
// single pool never mixes generations.
type activePool struct {
	gen uint64
	// revision is the SupplyResource revision the cfg came from. It is the
	// GLOBALLY comparable content version — unlike gen, which counts swaps in
	// this process and therefore cannot be compared across runners. It is what a
	// tagged record's config_generation reports, so a warehouse query can tell
	// which rule version produced which row during a rollout skew window.
	// Zero means "config did not come from a SupplyResource" (legacy globals path).
	revision uint64
	// rules is ruleCount(cfg), cached at build time so the host-wide rule gauge
	// can be recomputed from a pool pointer without re-parsing the config. -1
	// carries ruleCount's "shape not recognised" verdict through unchanged; see
	// reactorHost.activeConfigState for why that must not be folded into a sum.
	rules int
	cfg   []byte // config snapshot; replayed when rebuilding a doomed instance
	free  chan *pooledInstance
	size  int

	// drainTimeout overrides drainPoolWait for this pool. Zero means the default.
	// It exists so a test can assert drainPool's bound is honoured without
	// spending the production bound's full minute waiting for it.
	drainTimeout time.Duration
}

// inFlight reports how many of this pool's instances are currently borrowed.
// Derived rather than counted: capacity equals the borrowed set plus the free
// channel by construction (see borrow/giveBack), so a separate counter would be
// a second source of truth that can only disagree with this one.
func (p *activePool) inFlight() int { return p.size - len(p.free) }

// reactorEngine owns the wazero runtime handle, the compiled module, and the
// currently-active instance pool for one wasm module. Config switches replace
// active atomically; borrowers always read the live pool via active.Load().
type reactorEngine struct {
	host *reactorHost
	cm   wazero.CompiledModule

	active atomic.Pointer[activePool]
	gen    atomic.Uint64 // monotonic generation counter

	// lastSwapAt is when the active pool was installed. It drives the Stale
	// determination and xflow_supply_age_seconds: a source that keeps failing
	// leaves gen untouched, so only elapsed time exposes it.
	lastSwapAt atomic.Int64 // unix nanos

	// lastUsed is when this engine was last handed to a caller by one of the
	// three resolution paths. It is the ONLY viable reclamation criterion:
	// artifact_digest is boundary-evaluated per item and a digest can come back
	// via rollback, so "this module is no longer needed" is a moment that never
	// arrives — only "nobody has asked for it in T" does.
	//
	// Distinct from lastSwapAt above: that one tracks CONFIG installs and drives
	// staleness, and a module serving heavy traffic against a frozen rule set
	// has an old lastSwapAt and a fresh lastUsed. Reclaiming on lastSwapAt would
	// tear down exactly the busiest engine.
	//
	// Written under host.mu by touchLocked, read under host.mu by the sweep, so
	// the decision to reclaim is atomic with respect to the lookup that would
	// have used the engine. atomic.Int64 rather than a plain int64 only because
	// closeForTest and the drain path read it outside the lock in assertions.
	lastUsed atomic.Int64

	// reclaimed marks an engine the host has removed from its maps. It exists so
	// borrow can tell "this engine was torn down under me" apart from "this
	// engine was never configured": both leave active nil, but the first is a
	// retryable race (re-resolve and the module recompiles) while the second is
	// a real misconfiguration that retrying would only loop on.
	reclaimed atomic.Bool

	// sourceFailures counts consecutive source failures since the last successful
	// swap. Reset to zero on a successful swap.
	sourceFailures atomic.Int64

	// configFromSource records whether this module's config comes from a supply
	// consumer rather than from globals. It is resolved ONCE when the
	// engine is created or a consumer registers, and read lock-free on every
	// message.
	//
	// The lock had to go: registration used to happen only during warmup (pure
	// reads, no writer), but the registration window moved to activation time, so
	// a writer now contends with ~12000 reads/s and shows up as tail-latency
	// spikes.
	configFromSource atomic.Bool

	mu sync.Mutex // serializes pool (re)builds (swapConfig)
}

// newInstance instantiates a fresh reactor instance, verifies its ABI version,
// and applies the given config. On any failure the partial instance is closed.
// A configure returning errConfig yields a nil instance with an error — the
// caller (pool build) discards the whole new pool and keeps last-good (§6.3).
func (e *reactorEngine) newInstance(ctx context.Context, cfg []byte) (*pooledInstance, error) {
	mcfg := wazero.NewModuleConfig().WithStartFunctions("_initialize").WithName("")
	mod, err := e.host.runtime(ctx).InstantiateModule(ctx, e.cm, mcfg)
	if err != nil {
		return nil, fmt.Errorf("wasm reactor: instantiate: %w", err)
	}

	inst := &pooledInstance{
		mod:    mod,
		mem:    mod.Memory(),
		alloc:  mod.ExportedFunction("alloc"),
		eval:   mod.ExportedFunction("eval"),
		outPtr: mod.ExportedFunction("out_ptr"),
		// Optional per ABI §4.1 — nil is a valid guest, handled at use sites.
		outLen: mod.ExportedFunction("out_len"),
		td:     mod.ExportedFunction("teardown"),
	}
	if inst.alloc == nil || inst.eval == nil || inst.outPtr == nil || mod.ExportedFunction("configure") == nil {
		_ = mod.Close(ctx)
		return nil, fmt.Errorf("wasm reactor: module missing required reactor exports")
	}

	// ABI negotiation (docs §4.1).
	av := mod.ExportedFunction("abi_version")
	if av == nil {
		_ = mod.Close(ctx)
		return nil, fmt.Errorf("wasm reactor: module missing abi_version export")
	}
	avr, err := av.Call(ctx)
	if err != nil {
		_ = mod.Close(ctx)
		return nil, fmt.Errorf("wasm reactor: abi_version call: %w", err)
	}
	if int32(avr[0]) != reactorABIVersion {
		_ = mod.Close(ctx)
		return nil, fmt.Errorf("wasm reactor: ABI mismatch: guest %d, host %d", int32(avr[0]), reactorABIVersion)
	}

	// Configure (Fresh → Ready), exactly once per instance (§6.3 B-plan).
	if err := e.configure(ctx, inst, cfg); err != nil {
		_ = mod.Close(ctx)
		return nil, err
	}
	return inst, nil
}

// configure applies cfg to a fresh instance via the guest configure export.
func (e *reactorEngine) configure(ctx context.Context, in *pooledInstance, cfg []byte) error {
	ptr, err := in.alloc.Call(ctx, uint64(len(cfg)))
	if err != nil {
		return fmt.Errorf("wasm reactor: configure alloc: %w", err)
	}
	if len(cfg) > 0 && !in.mem.Write(uint32(ptr[0]), cfg) {
		return fmt.Errorf("wasm reactor: configure write out of range")
	}
	cfgFn := in.mod.ExportedFunction("configure")
	r, err := cfgFn.Call(ctx, uint64(len(cfg)))
	if err != nil {
		return fmt.Errorf("wasm reactor: configure call: %w", err)
	}
	if n := int32(r[0]); n < 0 {
		detail := in.readOut(ctx, -1)
		logEvalDetail("configure", n, detail)
		return &reactorEvalError{code: n, detail: detail}
	}
	return nil
}

// buildPool instantiates `size` instances all configured with cfg. If any
// instance fails to build/configure, the whole batch is torn down and the error
// returned — the caller keeps the previous active pool (last-good, §6.3).
func (e *reactorEngine) buildPool(ctx context.Context, cfg []byte, size uint64) (*activePool, error) {
	pool := &activePool{
		gen:  e.gen.Add(1),
		cfg:  cfg,
		free: make(chan *pooledInstance, size),
		size: int(size),
	}
	for i := uint64(0); i < size; i++ {
		inst, err := e.newInstance(ctx, cfg)
		if err != nil {
			// Tear down whatever was already built; discard the batch.
			close(pool.free)
			for built := range pool.free {
				built.teardown(ctx)
			}
			return nil, fmt.Errorf("wasm reactor: build instance %d/%d: %w", i+1, size, err)
		}
		pool.free <- inst
	}
	return pool, nil
}

// borrow takes an instance from the live pool, blocking until one is free or
// ctx is done. It returns the instance and the pool it came from (needed so
// return/doom target the correct generation even across a concurrent swap).
func (e *reactorEngine) borrow(ctx context.Context) (*pooledInstance, *activePool, error) {
	p := e.active.Load()
	if p == nil {
		return nil, nil, fmt.Errorf("wasm reactor: no active pool (unconfigured)")
	}
	start := time.Now()
	select {
	case inst := <-p.free:
		obs().OnBorrowWait(ctx, time.Since(start))
		return inst, p, nil
	case <-ctx.Done():
		return nil, nil, fmt.Errorf("wasm reactor: borrow: %w", ctx.Err())
	}
}

// giveBack returns a healthy instance to its pool. If the instance's pool is no
// longer active (swapped out), the instance is torn down instead of parked.
func (e *reactorEngine) giveBack(ctx context.Context, p *activePool, inst *pooledInstance) {
	if e.active.Load() != p {
		inst.teardown(ctx)
		return
	}
	select {
	case p.free <- inst:
	default:
		// Pool full (shouldn't happen: capacity == borrowed set). This is the
		// pool being torn down around a borrower that outlived it — the
		// nearest fit among the documented causes is "shutdown", not a normal
		// swap or eval failure.
		obs().OnInstanceRecycled(ctx, "shutdown")
		inst.teardown(ctx)
	}
}

// doom discards a failed instance and asynchronously rebuilds a replacement in
// the same pool so the pool returns to full strength. If the pool was swapped
// out meanwhile, no replacement is added (the pool is being torn down anyway).
//
// cause classifies the recycle for OnInstanceRecycled. evalOnce does not
// distinguish a ctx timeout from a guest trap/panic in its return value (both
// come back as a Call error with doomed=true), so this uses ctx.Err() as the
// discriminator: non-nil means the deadline actually expired ("timeout");
// nil means the instance failed for some other reason while still in its
// deadline ("eval_error"). This is a real distinction, not a guess: a Call
// error with ctx.Err() == nil cannot be a timeout because
// WithCloseOnContextDone only closes the module when the context is done.
func (e *reactorEngine) doom(ctx context.Context, p *activePool, inst *pooledInstance) {
	cause := "eval_error"
	if ctx.Err() != nil {
		cause = "timeout"
	}
	obs().OnInstanceRecycled(ctx, cause)

	// Close the failed instance on a background context — its own ctx may be
	// the expired deadline that doomed it.
	bg := context.Background()
	inst.teardown(bg)

	if e.active.Load() != p {
		return // pool superseded; don't replenish a dying generation
	}
	go func() {
		repl, err := e.newInstance(bg, p.cfg)
		if err != nil {
			// The pool now runs one instance short, permanently: nothing retries
			// this rebuild. That is a capacity loss, so it must be visible —
			// silent shrinkage looks identical to contention from outside, and
			// the pool's width is what bounds wasm concurrency.
			obs().OnInstanceRecycled(bg, "rebuild_failed")
			return
		}
		if e.active.Load() != p {
			repl.teardown(bg)
			return
		}
		select {
		case p.free <- repl:
		default:
			repl.teardown(bg)
		}
	}()
}

// recyclePlanned proactively replaces a healthy instance that has reached
// maxEvalsPerInstance. Unlike doom it does not indicate a failure: the eval
// result is already in the caller's hands and is valid, so no error is
// returned up the stack. The old instance is torn down and a fresh one is
// built asynchronously, keeping the pool at full strength.
func (e *reactorEngine) recyclePlanned(p *activePool, inst *pooledInstance) {
	cause := inst.recycleCause
	if cause == "" {
		// Only reachable if a future caller reaches recyclePlanned by some path
		// other than evalOnce's planned-recycle return. Name it rather than
		// silently attributing it to one of the real bounds.
		cause = "planned"
	}
	obs().OnInstanceRecycled(context.Background(), cause)
	bg := context.Background()
	inst.teardown(bg)
	if e.active.Load() != p {
		return
	}
	go func() {
		repl, err := e.newInstance(bg, p.cfg)
		if err != nil {
			obs().OnInstanceRecycled(bg, "rebuild_failed")
			return
		}
		if e.active.Load() != p {
			repl.teardown(bg)
			return
		}
		select {
		case p.free <- repl:
		default:
			repl.teardown(bg)
		}
	}()
}

// swapConfig builds a new pool for cfg and atomically installs it (B-plan,
// §6.3). On success the old pool is drained and torn down in the background; on
// failure the active pool is left untouched (last-good preserved, no shrink).
// revision is the SupplyResource revision the content came from (0 for the
// legacy globals path).
func (e *reactorEngine) swapConfig(ctx context.Context, cfg []byte, size uint64, revision uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	start := time.Now()
	newPool, err := e.buildPool(ctx, cfg, size)
	if err != nil {
		e.sourceFailures.Add(1)
		// The host's CURRENT state, deliberately not the rejected config's: a
		// rejected swap leaves active untouched, so the rules actually evaluating
		// traffic are the previous ones. The bundled metrics observer happens to
		// skip both gauges on a rejection, but the Observer interface is public
		// and an embedder that records them must not be handed numbers for a
		// config that is serving nothing.
		rules, revisionNow := e.host.activeConfigState()
		obs().OnPoolSwap(ctx, "rejected", rules, revisionNow, time.Since(start))
		return err // active unchanged
	}
	// Must be set before Swap publishes the pointer: once Swap runs, readers can
	// see newPool immediately, and writing the field afterward would race them.
	newPool.revision = revision
	newPool.rules = ruleCount(cfg)
	old := e.active.Swap(newPool)
	e.lastSwapAt.Store(time.Now().UnixNano())
	e.sourceFailures.Store(0)
	// After the Swap, so the aggregate includes the pool just installed.
	rules, revisionNow := e.host.activeConfigState()
	obs().OnPoolSwap(ctx, "applied", rules, revisionNow, time.Since(start))
	e.host.reportReadyInstances(ctx)
	if old != nil {
		go e.drainPool(context.Background(), old)
	}
	return nil
}

// drainPoolWait bounds how long drainPool waits for a retired pool's instances.
//
// It is generous rather than tight because the straggler it exists to catch is a
// doom replacement built for a pool that died mid-rebuild: doom checks the pool
// is live before parking the replacement, so a swap landing inside that window
// leaves an instance in a channel only drainPool reads.
//
// Carried on activePool rather than read as a package constant so a test can
// assert the bound exists without waiting it out. Zero means the default.
const drainPoolWait = 60 * time.Second

func (p *activePool) drainWait() time.Duration {
	if p.drainTimeout > 0 {
		return p.drainTimeout
	}
	return drainPoolWait
}

// drainPool tears down every instance parked in an old pool.
//
// The wait is bounded, and the bound is load-bearing. An instance that was
// in flight at swap time never arrives here — giveBack sees the pool is no
// longer active and tears that instance down itself, and doom does the same for
// one that failed mid-eval. Waiting unconditionally for `size` receives
// therefore parked this goroutine for the process lifetime, one leaked goroutine
// per swap per in-flight borrow.
//
// Exiting early is not an instance leak, precisely because those two paths
// already reclaim what they hold. What the bound gives up is only the
// mid-rebuild straggler above, and only if it arrives after the deadline.
func (e *reactorEngine) drainPool(ctx context.Context, p *activePool) {
	// One timer for the whole loop, not per receive: the intent is to bound the
	// drain, not each individual wait.
	timer := time.NewTimer(p.drainWait())
	defer timer.Stop()
	for range p.size {
		select {
		case inst, ok := <-p.free:
			if !ok {
				return
			}
			inst.teardown(ctx)
			obs().OnInstanceRecycled(ctx, "pool_swapped")
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// configHash keys a config snapshot for equality checks (has the config
// changed?). It is not exported ABI — purely host bookkeeping.
func configHash(cfg []byte) string {
	sum := sha256.Sum256(cfg)
	return hex.EncodeToString(sum[:])
}

// Availability is the three-tier config availability ladder. It replaces the
// binary "active == nil" check, which conflated Stale with Fresh: functionally
// correct but invisible, and invisible staleness is exactly how a config source
// can silently stop updating for hours.
type Availability int

const (
	// AvailUnavailable: never successfully configured. Reachable only under
	// require_ready:false — with the gate on, the runner never took the
	// activation, so no traffic arrives here.
	AvailUnavailable Availability = iota
	// AvailStale: serving content, but the source has failed repeatedly since.
	// Serving is correct (last-good beats no service); the point is that
	// supply_age_seconds keeps climbing so it can be alerted on.
	AvailStale
	// AvailFresh: the most recent config application succeeded.
	AvailFresh
)

// staleFailureThreshold is how many consecutive source failures mark the active
// content Stale. One transient blip should not flip a healthy module.
const staleFailureThreshold = 3

// availability reports the current tier.
func (e *reactorEngine) availability() Availability {
	if e.active.Load() == nil {
		return AvailUnavailable
	}
	if e.sourceFailures.Load() >= staleFailureThreshold {
		return AvailStale
	}
	return AvailFresh
}

// Generation returns the server-side revision of the currently active content,
// or 0 when there is no active pool or the content came from the legacy globals
// path. This is what an eval result reports as config_generation.
func (e *reactorEngine) Generation() uint64 {
	if p := e.active.Load(); p != nil {
		return p.revision
	}
	return 0
}

// ConfigAge returns how long the active content has been in service. Zero when
// nothing was ever installed. It is the only signal that exposes a source which
// stopped updating: a stuck source leaves gen and revision untouched, so nothing
// but elapsed time changes.
func (e *reactorEngine) ConfigAge() time.Duration {
	ns := e.lastSwapAt.Load()
	if ns == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ns))
}

// normalizeConfig produces the canonical JSON config bytes the guest configure
// expects. It accepts either a pre-marshaled []byte or a map/struct.
func normalizeConfig(cfg any) ([]byte, error) {
	switch v := cfg.(type) {
	case nil:
		return []byte(`{"rules":[]}`), nil
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("wasm reactor: marshal config: %w", err)
		}
		return b, nil
	}
}
