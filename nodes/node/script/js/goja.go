package js

import (
	"context"
	"fmt"
	"sync"

	"github.com/dop251/goja"
	"github.com/xbcio/xflow/nodes/node/script"
)

func init() {
	script.Register("js", "goja", func() script.Engine { return sharedGoja })
}

// sharedGoja is process-wide: it holds a sync.Pool of runtimes and an LRU
// program cache, both safe for concurrent use.
var sharedGoja = &gojaEngine{
	programs: newProgramCache(script.DefaultProgramCacheSize),
}

type gojaEngine struct {
	pool sync.Pool // of *pooledVM
	// programs caches compiled scripts by source code (LRU bounded). The
	// LRU evicts the least-recently-used entry once capacity is reached, so
	// a deployment with high script churn stays memory-bounded.
	programs *programCache
}

type pooledVM struct {
	vm       *goja.Runtime
	baseline map[string]struct{} // top-level global keys at creation
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
	vm.SetMaxCallStackSize(script.DefaultGojaStackSize)
	base := map[string]struct{}{}
	for _, k := range vm.GlobalObject().Keys() {
		base[k] = struct{}{}
	}
	return &pooledVM{vm: vm, baseline: base}
}

// cleanup removes any top-level globals introduced during execution so the
// runtime can be safely reused.
func (p *pooledVM) cleanup() {
	g := p.vm.GlobalObject()
	for _, k := range g.Keys() {
		if _, ok := p.baseline[k]; !ok {
			_ = g.Delete(k)
		}
	}
}

func (e *gojaEngine) Execute(ctx context.Context, code string, globals map[string]any, h script.Helpers) (any, error) {
	// TODO(metrics): emit before/after counters and timers when the project
	// metrics middleware lands:
	//   - script_goja_compile_total{result=hit|miss} (e.programs LRU)
	//   - script_goja_compile_duration_seconds       (goja.Compile only)
	//   - script_goja_pool_get_total{result=hit|miss} (warm vs cold VM)
	//   - script_goja_execute_duration_seconds       (RunProgram window)
	//   - script_goja_interrupt_total                (ctx-cancelled path)
	prog, err := e.compile(code)
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

	pv.cleanup()
	e.pool.Put(pv)

	if val == nil || goja.IsUndefined(val) || goja.IsNull(val) {
		return nil, nil
	}
	return val.Export(), nil
}
