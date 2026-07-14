package js

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fastschema/qjs"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

func init() {
	engine.Register("js", "qjs", func() engine.Engine { return qjsEngine{} })
	// QuickJS-on-wasm has a ~330ms first-load cost; let the host warm it up.
	engine.RegisterWarmer(Warmup)
}

// qjsEngine runs scripts on QuickJS-via-wazero (pure Go). The QuickJS wasm
// compiles once process-wide (~330ms, build-cached); each Execute then
// instantiates a fresh module (~3ms) for isolation. Unlike js/goja, there is
// no warm runtime pool — a fresh instance per call is the isolation boundary.
type qjsEngine struct{}

// stripGlobals removes the non-standard QuickJS host globals exposed by
// fastschema/qjs from a fresh runtime. The list was enumerated empirically via
// Object.getOwnPropertyNames(globalThis) against a clean qjs.Runtime; it covers
// every name beyond the standard ECMAScript surface that grants host I/O or
// ambient capabilities (filesystem via std/os, output via print/console, timers,
// gc, and environment introspection). Standard constructors (Array, JSON, Map,
// the TypedArrays, etc.) are intentionally left in place.
//
// KNOWN LIMITATION: this is a denylist (fails open) — a future qjs version that
// adds a new host-capability global would slip through, so re-audit the global
// surface when upgrading the qjs dependency (currently pinned at v0.0.6).
const stripGlobals = `(function(){
  var remove = ['std','os','print','scriptArgs','bjson','console','performance',
    'navigator','gc','queueMicrotask',
    'setTimeout','setInterval','clearTimeout','clearInterval'];
  for (var i = 0; i < remove.length; i++) {
    try { delete globalThis[remove[i]]; } catch (e) {}
  }
})();`

func (qjsEngine) Name() string { return "js/qjs" }

func (qjsEngine) Execute(ctx context.Context, code string, globals map[string]any, h engine.Helpers) (result any, err error) {
	// TODO(metrics): emit before/after counters and timers when the project
	// metrics middleware lands:
	//   - script_qjs_runtime_new_duration_seconds (qjs.New cost; cold-start indicator)
	//   - script_qjs_execute_duration_seconds    (rt.Eval window)
	//   - script_qjs_abort_total{cause=ctx|panic} (recover path)
	if ctx == nil {
		ctx = context.Background()
	}

	// CloseOnContextDone tears down the underlying wazero module when ctx is
	// cancelled. That abort surfaces as a panic from inside Eval/Close rather
	// than an error, so recover here and translate it into the context error.
	defer func() {
		if r := recover(); r != nil && err == nil {
			if cerr := ctx.Err(); cerr != nil {
				err = fmt.Errorf("js/qjs: execution aborted: %w", cerr)
			} else {
				err = fmt.Errorf("js/qjs: execution aborted: %v", r)
			}
		}
	}()

	rt, nerr := qjs.New(qjs.Option{
		Context:            ctx,
		CloseOnContextDone: true,
		// Resource limits: cap the QuickJS heap and C-stack so a runaway
		// script raises an out-of-memory exception instead of consuming
		// unbounded host memory.
		MemoryLimit:  engine.DefaultQJSMemoryLimit,
		MaxStackSize: engine.DefaultQJSMaxStackSize,
	})
	if nerr != nil {
		return nil, fmt.Errorf("js/qjs: new runtime: %w", nerr)
	}
	defer safeCloseQJS(rt)

	c := rt.Context()
	g := c.Global()

	// fastschema/qjs is not bare QuickJS: a fresh runtime exposes QuickJS host
	// globals (std, os, print, scriptArgs, console, timers, gc, ...) that perform
	// real host I/O — os.readdir/std.open reach the actual filesystem. The script
	// node runs untrusted user code, so strip every non-standard host global
	// before any user-visible state is injected. This restores parity with goja's
	// genuinely bare goja.New(). The qjs-library internals (QJS_PROXY_VALUE) and
	// our own $helpers are injected later and are deliberately not in this list.
	if _, serr := rt.Eval("sandbox.js", qjs.Code(stripGlobals)); serr != nil {
		return nil, fmt.Errorf("js/qjs: sandbox setup: %w", serr)
	}

	// Inject globals via JSON round-trip for value parity with goja.
	for k, v := range BuildGlobals(globals) {
		b, merr := json.Marshal(v)
		if merr != nil {
			return nil, fmt.Errorf("js/qjs: marshal global %q: %w", k, merr)
		}
		g.SetProperty(c.NewString(k), c.ParseJSON(string(b)))
	}

	// Inject $helpers (non-security utilities) as native functions.
	helpers := c.NewObject()
	helpers.SetProperty(c.NewString("base64Encode"), c.Function(func(this *qjs.This) (*qjs.Value, error) {
		args := this.Args()
		if len(args) == 0 {
			return c.NewString(""), nil
		}
		return c.NewString(h.Base64Encode(args[0].String())), nil
	}))
	helpers.SetProperty(c.NewString("base64Decode"), c.Function(func(this *qjs.This) (*qjs.Value, error) {
		args := this.Args()
		if len(args) == 0 {
			return c.NewString(""), nil
		}
		dec, derr := h.Base64Decode(args[0].String())
		if derr != nil {
			return nil, derr
		}
		return c.NewString(dec), nil
	}))
	g.SetProperty(c.NewString("$helpers"), helpers)

	val, eerr := rt.Eval("script.js", qjs.Code(code))
	if eerr != nil {
		return nil, fmt.Errorf("js/qjs: %w", eerr)
	}
	if val == nil || val.IsUndefined() || val.IsNull() {
		return nil, nil
	}

	// Serialize the completion value JS-side. The host-side Value.JSONStringify()
	// wasm call corrupts its result ("no NUL terminator") once two or more host
	// functions are registered, so round-trip through JSON.stringify in the JS
	// context and read the resulting string instead.
	g.SetProperty(c.NewString("__xflowResult"), val)
	sv, serr := rt.Eval("result.js", qjs.Code(`JSON.stringify(globalThis.__xflowResult)`))
	if serr != nil {
		return nil, fmt.Errorf("js/qjs: encode result: %w", serr)
	}
	if sv == nil || sv.IsUndefined() || sv.IsNull() {
		return nil, nil
	}
	jsonStr := sv.String()
	if jsonStr == "" || jsonStr == "undefined" {
		return nil, nil
	}

	var decoded any
	if uerr := json.Unmarshal([]byte(jsonStr), &decoded); uerr != nil {
		return nil, fmt.Errorf("js/qjs: decode result: %w", uerr)
	}
	return decoded, nil
}

// safeCloseQJS releases the runtime. When ctx cancellation already closed the
// underlying module, Close panics; swallow it so it cannot mask the real error.
func safeCloseQJS(rt *qjs.Runtime) {
	defer func() { _ = recover() }()
	rt.Close()
}

// Warmup pre-instantiates a qjs runtime to populate fastschema/qjs's internal
// QuickJS-wasm build cache process-wide. The first qjs.New takes ~330ms while
// the wasm is compiled; subsequent calls drop to ~3ms once the cache is hot.
// Call once at server/runner startup to absorb the cold-start latency before
// real traffic.
func Warmup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rt, err := qjs.New(qjs.Option{
		Context:      ctx,
		MemoryLimit:  engine.DefaultQJSMemoryLimit,
		MaxStackSize: engine.DefaultQJSMaxStackSize,
	})
	if err != nil {
		return fmt.Errorf("js/qjs: warmup: %w", err)
	}
	safeCloseQJS(rt)
	return nil
}
