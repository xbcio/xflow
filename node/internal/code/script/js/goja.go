package js

import (
	"context"
	"fmt"
	"sync"

	"github.com/dop251/goja"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

func init() {
	engine.Register("js", "goja", func() engine.Engine { return sharedGoja })
}

// sharedGoja is process-wide: it holds a sync.Pool of runtimes and an LRU
// program cache, both safe for concurrent use.
var sharedGoja = &gojaEngine{
	programs: newProgramCache(engine.DefaultProgramCacheSize),
}

type gojaEngine struct {
	pool sync.Pool // of *pooledVM
	// programs caches compiled scripts by source code (LRU bounded). The
	// LRU evicts the least-recently-used entry once capacity is reached, so
	// a deployment with high script churn stays memory-bounded.
	programs *programCache
}

type pooledVM struct {
	vm              *goja.Runtime
	baselineGlobals map[string]struct{} // top-level global keys at creation
	// baselineProtoKeys snapshots Object.prototype's own property keys at
	// creation. cleanup() compares against it to detect prototype pollution —
	// a script that writes Object.prototype.x taints the pooled VM, so it is
	// discarded rather than reused across executions.
	baselineProtoKeys map[string]struct{}
}

// prototypeKeys returns the own property key set of <ctor>.prototype (e.g.
// "Object"). Returns nil if the prototype cannot be resolved.
func prototypeKeys(vm *goja.Runtime, ctor string) map[string]struct{} {
	val := vm.Get(ctor)
	if val == nil {
		return nil
	}
	obj := val.ToObject(vm)
	if obj == nil {
		return nil
	}
	proto := obj.Get("prototype")
	if proto == nil {
		return nil
	}
	po := proto.ToObject(vm)
	if po == nil {
		return nil
	}
	out := make(map[string]struct{}, 16)
	for _, k := range po.Keys() {
		out[k] = struct{}{}
	}
	return out
}

func (e *gojaEngine) Name() string { return "js/goja" }

func (e *gojaEngine) compile(code string) (*goja.Program, error) {
	if p, ok := e.programs.get(code); ok {
		return p, nil
	}
	p, err := goja.Compile("script.js", code, false)
	if err != nil {
		return nil, err
	}
	e.programs.add(code, p)
	return p, nil
}

func (e *gojaEngine) get() *pooledVM {
	if v, ok := e.pool.Get().(*pooledVM); ok {
		return v
	}
	vm := goja.New()
	// Resource limit: bound recursion depth so a runaway script raises a
	// runtime error instead of overflowing the host goroutine stack.
	vm.SetMaxCallStackSize(engine.DefaultGojaStackSize)
	base := map[string]struct{}{}
	for _, k := range vm.GlobalObject().Keys() {
		base[k] = struct{}{}
	}
	return &pooledVM{
		vm:                vm,
		baselineGlobals:   base,
		baselineProtoKeys: prototypeKeys(vm, "Object"),
	}
}

// cleanup removes any top-level globals introduced during execution so the
// runtime can be reused. It returns false when the VM is tainted (prototype
// pollution detected) and must not be returned to the pool.
func (p *pooledVM) cleanup() bool {
	g := p.vm.GlobalObject()
	for _, k := range g.Keys() {
		if _, ok := p.baselineGlobals[k]; !ok {
			_ = g.Delete(k)
		}
	}
	// Detect prototype pollution: if Object.prototype gained or lost own
	// properties since baseline, a script mutated shared prototypes. Reusing
	// such a VM would leak that mutation to subsequent executions.
	current := prototypeKeys(p.vm, "Object")
	if p.baselineProtoKeys == nil || current == nil {
		return false
	}
	if len(current) != len(p.baselineProtoKeys) {
		return false
	}
	for k := range current {
		if _, ok := p.baselineProtoKeys[k]; !ok {
			return false
		}
	}
	return true
}

func (e *gojaEngine) Execute(ctx context.Context, src engine.Source, globals map[string]any, h engine.Helpers) (any, error) {
	// TODO(metrics): the precondition this list used to name — "when the
	// project metrics middleware lands" — has been satisfied for a while:
	// script.Observer is wired to observability/metrics.ScriptMetrics through
	// node.SetScriptObserver.
	//
	// Two of the five signals it listed are already emitted by that seam,
	// labelled runtime="goja", so do not add a second timer for them: the
	// execute duration and the ctx-cancelled path arrive as OnScriptExecute —
	// outcome="main" on success, "timeout" when ctx expired, "error" or
	// "permanent" otherwise. Note the duration is measured around the whole
	// script.Execute, so it includes compile and pool get.
	//
	// What is still missing is everything below the engine boundary, and
	// unlike the wasm package this one exposes no observer of its own to
	// report it through. Adding that seam is the actual remaining work:
	//   - script_goja_compile_total{result=hit|miss} (e.programs LRU)
	//   - script_goja_compile_duration_seconds       (goja.Compile only)
	//   - script_goja_pool_get_total{result=hit|miss} (warm vs cold VM)
	//
	// src.Digest is ignored: JS sources are kilobytes, so keying the program
	// cache by content costs a comparison this engine cannot measure. The digest
	// exists for wasm, where the same key is ~9 MB.
	prog, err := e.compile(src.Code)
	if err != nil {
		return nil, fmt.Errorf("js/goja: compile: %w", err)
	}

	pv := e.get()
	vm := pv.vm

	// Inject helpers as $helpers (non-security utilities only).
	_ = vm.Set("$helpers", map[string]any{
		"base64Encode": h.Base64Encode,
		"base64Decode": h.Base64Decode,
	})
	// Inject globals ($input, $credentials, $credential, ...).
	for k, v := range BuildGlobals(globals) {
		_ = vm.Set(k, v)
	}

	// Timeout: watcher interrupts the loop on ctx cancellation. The watcher is
	// unconditional so its exit is observable via `exited`; ctx is always
	// non-nil per the Engine contract (context.Background().Done() is a nil
	// channel that blocks forever, so the watcher simply parks on <-done).
	//
	// LIMITATION: goja's Interrupt is checked only at function-call boundaries
	// in the interpreter, NOT inside tight pure-computation loops. A script
	// like `while(true){}` or `for(let i=0;;i++){}` cannot be interrupted by
	// this watcher — the ctx will expire but RunProgram never returns. CPU-
	// heavy or potentially-unbounded scripts MUST run on js/qjs or
	// wasm/wazero, which support true mid-execution termination.
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			vm.Interrupt("timeout")
		case <-done:
		}
	}()

	val, runErr := vm.RunProgram(prog)
	close(done)
	<-exited            // watcher can no longer touch vm after this
	vm.ClearInterrupt() // clear any interrupt that landed post-completion, before reuse

	if runErr != nil {
		// Interrupted/errored runtime is in an unknown state — discard, don't return to pool.
		return nil, fmt.Errorf("js/goja: %w", runErr)
	}

	if pv.cleanup() {
		e.pool.Put(pv)
	}

	if val == nil || goja.IsUndefined(val) || goja.IsNull(val) {
		return nil, nil
	}
	return val.Export(), nil
}
