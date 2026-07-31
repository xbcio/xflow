package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

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
}

// eval runs one input through the resident instance. It returns the decoded
// output bytes on success, or (nil, doomed, err) on failure where doomed
// signals the instance must be discarded and rebuilt rather than returned to
// the pool.
//
// Memory ABI (docs §4.3): alloc(len) → ptr; write input; eval(len) → n;
// n<0 → error (out buffer holds structured detail, read for logging only);
// n>=0 → read out_ptr()[:n] and copy out (mem.Read returns a view into linear
// memory that the next borrower would clobber).
func (in *pooledInstance) evalOnce(ctx context.Context, input []byte) (out []byte, doomed bool, err error) {
	ptr, err := in.alloc.Call(ctx, uint64(len(input)))
	if err != nil {
		// A failed alloc means the module was closed (timeout) or trapped.
		return nil, true, fmt.Errorf("wasm reactor: alloc: %w", err)
	}
	if len(input) > 0 && !in.mem.Write(uint32(ptr[0]), input) {
		return nil, true, fmt.Errorf("wasm reactor: write input out of range")
	}

	r, err := in.eval.Call(ctx, uint64(len(input)))
	if err != nil {
		// Timeout (WithCloseOnContextDone) or trap → instance permanently
		// closed (constraint #4). Doom it.
		return nil, true, fmt.Errorf("wasm reactor: eval: %w", err)
	}

	n := int32(r[0])
	if n < 0 {
		detail := in.readOut(ctx, -1) // best-effort structured error for logs
		// ERR_EVAL is a guest-side panic/eval fault: the instance may hold
		// polluted global state, so doom it (§5.6). Other negatives are
		// call-level (bad input / oversize output); the instance stays clean.
		doom := n == errEval
		return nil, doom, &reactorEvalError{code: n, detail: detail}
	}

	return in.readOut(ctx, n), false, nil
}

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
	gen  uint64
	cfg  []byte // config snapshot; replayed when rebuilding a doomed instance
	free chan *pooledInstance
	size int
}

// reactorEngine owns the wazero runtime handle, the compiled module, and the
// currently-active instance pool for one wasm module. Config switches replace
// active atomically; borrowers always read the live pool via active.Load().
type reactorEngine struct {
	host *reactorHost
	cm   wazero.CompiledModule

	active atomic.Pointer[activePool]
	gen    atomic.Uint64 // monotonic generation counter

	// lastAppliedVersion tracks the config version string most recently
	// swapped in successfully by the watcher goroutine. Updated only on
	// successful swap so a failed version is retried on the next poll (§6.5).
	lastAppliedVersion atomic.Value // string

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
	select {
	case inst := <-p.free:
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
		// Pool full (shouldn't happen: capacity == borrowed set). Drop safely.
		inst.teardown(ctx)
	}
}

// doom discards a failed instance and asynchronously rebuilds a replacement in
// the same pool so the pool returns to full strength. If the pool was swapped
// out meanwhile, no replacement is added (the pool is being torn down anyway).
func (e *reactorEngine) doom(p *activePool, inst *pooledInstance) {
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
			// Replenish failed; the pool runs one instance short until the
			// next successful rebuild. A metric would fire here (§6.7).
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
func (e *reactorEngine) swapConfig(ctx context.Context, cfg []byte, size uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	newPool, err := e.buildPool(ctx, cfg, size)
	if err != nil {
		return err // active unchanged
	}
	old := e.active.Swap(newPool)
	if old != nil {
		go e.drainPool(context.Background(), old)
	}
	return nil
}

// drainPool tears down every instance parked in an old pool. In-flight
// borrowers from the old pool return via giveBack, which sees the pool is no
// longer active and tears their instance down too. This waits for the full set
// (size) to be reclaimed so no instance leaks.
func (e *reactorEngine) drainPool(ctx context.Context, p *activePool) {
	for range p.size {
		inst, ok := <-p.free
		if !ok {
			return
		}
		inst.teardown(ctx)
	}
}

// configHash keys a config snapshot for equality checks (has the config
// changed?). It is not exported ABI — purely host bookkeeping.
func configHash(cfg []byte) string {
	sum := sha256.Sum256(cfg)
	return hex.EncodeToString(sum[:])
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
