package node_test

// Handler-layer contract tests for "ParamSpec.Default equals the fallback the
// runtime actually uses" (descriptor-contract design §2.4, "Default 不可改").
//
// Nothing at run time reads ParamSpec.Default: an HTTP-registered workflow that
// omits a param gets whatever the handler substitutes. So for each Default the
// handler is run with the param absent, set to the Default as Go wrote it, and
// set to the Default as it comes back from JSON (numbers become float64), and
// the three effective behaviours must be identical. A Default that disagrees
// with the handler is fixed in the handler, never by changing the Default (that
// moves SDK workflow hashes); see TestBuiltinDefaultsGolden.
//
// The Default under test is always read from the live descriptor, never
// restated here, so the golden test and these tests cannot drift apart.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"reflect"
	"strings"
	"testing"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	actionimpl "github.com/xbcio/xflow/node/internal/action"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// descriptorDefault returns the Default declared at path on type@version,
// failing the test if the param or its Default is gone.
func descriptorDefault(t *testing.T, typ string, version int, path string) any {
	t.Helper()
	for _, vd := range builtinDescriptors(t) {
		if vd.desc.Type != typ || vd.version != version {
			continue
		}
		spec, ok := findParam(vd.desc, path)
		if !ok {
			t.Fatalf("%s@%d has no param %q", typ, version, path)
		}
		if spec.Default == nil {
			t.Fatalf("%s@%d/%s has no Default", typ, version, path)
		}
		return spec.Default
	}
	t.Fatalf("no builtin descriptor %s@%d", typ, version)
	return nil
}

// jsonValue returns v as it reads back from a JSON document: every number is
// float64. This is the shape an HTTP-registered definition carries.
func jsonValue(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

type paramVariant struct {
	name   string
	params map[string]any
}

// defaultVariants returns base with key absent, with key set to the Default
// as written in Go, and with key set to the Default after a JSON round trip.
// base is never modified.
func defaultVariants(t *testing.T, base map[string]any, key string, def any) []paramVariant {
	t.Helper()
	absent := maps.Clone(base)
	delete(absent, key)
	goValue := maps.Clone(absent)
	goValue[key] = def
	fromJSON := maps.Clone(absent)
	fromJSON[key] = jsonValue(t, def)
	return []paramVariant{{"absent", absent}, {"default", goValue}, {"default_json", fromJSON}}
}

// assertSameBehaviour runs observe on every variant and requires each result
// to equal the absent variant's.
func assertSameBehaviour(t *testing.T, variants []paramVariant, observe func(t *testing.T, params map[string]any) any) {
	t.Helper()
	want := observe(t, variants[0].params)
	for _, v := range variants[1:] {
		if got := observe(t, v.params); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: effective behaviour %#v, want %#v (param absent)", v.name, got, want)
		}
	}
}

func actionHandler(t *testing.T, typ string) types.ActionHandler {
	t.Helper()
	h, ok := registry.LookupVersion(typ, 1)
	if !ok {
		t.Fatalf("%s@1 is not registered", typ)
	}
	return h
}

func suspendingHandler(t *testing.T, typ string) types.SuspendingHandler {
	t.Helper()
	h, ok := actionHandler(t, typ).(types.SuspendingHandler)
	if !ok {
		t.Fatalf("%s is not a SuspendingHandler", typ)
	}
	return h
}

// outcome is an Execute result reduced to what a caller can observe.
type outcome struct {
	Port string
	Data map[string]any
	Err  string
}

func outcomeOf(out *types.Output, err error) outcome {
	if err != nil {
		return outcome{Err: err.Error()}
	}
	return outcome{Port: out.Port, Data: out.Data}
}

// --- xflow.http -----------------------------------------------------------

func TestDefaultContractHTTPMethod(t *testing.T) {
	def := descriptorDefault(t, "xflow.http", 1, "method")

	var sent string
	orig := actionimpl.DefaultHTTPClient
	actionimpl.DefaultHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sent = r.Method
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
		}, nil
	})}
	t.Cleanup(func() { actionimpl.DefaultHTTPClient = orig })

	h := actionHandler(t, "xflow.http")
	base := map[string]any{"url": "https://example.test/x"}
	assertSameBehaviour(t, defaultVariants(t, base, "method", def), func(t *testing.T, params map[string]any) any {
		sent = ""
		if _, err := h.Execute(context.Background(), &types.Input{Params: params}); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		return sent
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// --- xflow.map (handler half; the engine half is engine/default_contract_test.go)

func TestDefaultContractMapBatchSize(t *testing.T) {
	def := descriptorDefault(t, "xflow.map", 1, "batch_size")
	h := actionHandler(t, "xflow.map")
	base := map[string]any{"items": "items"}
	data := map[string]any{"items": []any{1, 2, 3}}
	assertSameBehaviour(t, defaultVariants(t, base, "batch_size", def), func(t *testing.T, params map[string]any) any {
		return outcomeOf(h.Execute(context.Background(), &types.Input{Params: params, Data: data}))
	})
}

// The expression form is the only place the map HANDLER reads
// continue_on_error; the body form reads it in the engine.
func TestDefaultContractMapContinueOnErrorInline(t *testing.T) {
	def := descriptorDefault(t, "xflow.map", 1, "continue_on_error")
	h := actionHandler(t, "xflow.map")
	// "a" + 1 fails, 1 + 1 succeeds: a partial failure, which is exactly what
	// the setting decides.
	base := map[string]any{"items": "items", "expression": "$item + 1"}
	data := map[string]any{"items": []any{1, "a"}}
	run := func(t *testing.T, params map[string]any) any {
		return outcomeOf(h.Execute(context.Background(), &types.Input{Params: params, Data: data}))
	}
	assertSameBehaviour(t, defaultVariants(t, base, "continue_on_error", def), run)

	// Control: the observation distinguishes the two settings, so the equality
	// above is not vacuous.
	on := maps.Clone(base)
	on["continue_on_error"] = true
	if reflect.DeepEqual(run(t, on), run(t, base)) {
		t.Fatal("continue_on_error=true behaves like absent; the probe does not observe the setting")
	}
}

// --- xflow.merge ----------------------------------------------------------

func TestDefaultContractMergeOnOthers(t *testing.T) {
	def := descriptorDefault(t, "xflow.merge", 1, "on_others")
	h := actionHandler(t, "xflow.merge")
	base := map[string]any{"mode": "wait_any"}
	assertSameBehaviour(t, defaultVariants(t, base, "on_others", def), func(t *testing.T, params map[string]any) any {
		return outcomeOf(h.Execute(context.Background(), &types.Input{
			Params: params,
			Inputs: map[string]any{"a": map[string]any{"v": 1}},
		}))
	})
}

// --- xflow.wait -----------------------------------------------------------

func TestDefaultContractWaitMode(t *testing.T) {
	def := descriptorDefault(t, "xflow.wait", 1, "mode")
	h := suspendingHandler(t, "xflow.wait")
	base := map[string]any{"signal_name": "go", "timeout": "1m"}
	assertSameBehaviour(t, defaultVariants(t, base, "mode", def), func(t *testing.T, params map[string]any) any {
		spec, err := h.PrepareSuspend(context.Background(), &types.Input{NodeName: "w", Params: params})
		if err != nil {
			return err.Error()
		}
		return *spec
	})
}

// --- xflow.approval -------------------------------------------------------
//
// Read-only use of node/internal/group through the registered handler; no
// group code is touched.

func TestDefaultContractApprovalMode(t *testing.T) {
	def := descriptorDefault(t, "xflow.approval", 1, "mode")
	h := suspendingHandler(t, "xflow.approval")
	base := map[string]any{"approvers": []any{"alice", "bob"}}
	prepare := func(t *testing.T, params map[string]any) any {
		spec, err := h.PrepareSuspend(context.Background(), &types.Input{NodeName: "gate", Params: params})
		if err != nil {
			return err.Error()
		}
		return *spec
	}
	assertSameBehaviour(t, defaultVariants(t, base, "mode", def), prepare)

	// Documented NON-equality: an explicit empty mode does not fall back to
	// the Default. parseApprovalParams only substitutes "any" when the key is
	// absent, so mode: "" reaches the mode switch as "" and fails. The editor
	// and validator must treat "" as a value, not as "unset", for this param.
	empty := maps.Clone(base)
	empty["mode"] = ""
	got := prepare(t, empty)
	msg, isErr := got.(string)
	if !isErr || !strings.Contains(msg, "unknown approval mode") {
		t.Fatalf(`mode "" = %#v; want the "unknown approval mode" error (explicit "" does not fall back to %v)`, got, def)
	}
}

func TestDefaultContractApprovalTimeoutAction(t *testing.T) {
	def := descriptorDefault(t, "xflow.approval", 1, "timeout_action")
	h := suspendingHandler(t, "xflow.approval")
	base := map[string]any{"approvers": []any{"alice"}, "timeout": "1m"}
	resume := func(t *testing.T, params map[string]any) any {
		return outcomeOf(h.OnResume(context.Background(), &types.Input{NodeName: "gate", Params: params},
			&types.SignalPayload{Triggered: types.TimeoutFired}))
	}
	assertSameBehaviour(t, defaultVariants(t, base, "timeout_action", def), resume)

	reject := maps.Clone(base)
	reject["timeout_action"] = "reject"
	if reflect.DeepEqual(resume(t, reject), resume(t, base)) {
		t.Fatal("timeout_action=reject behaves like absent; the probe does not observe the setting")
	}
}

// --- xflow.supply.* -------------------------------------------------------

// Supply nodes have no handler; supply.RequireReady is the one reader the
// control plane uses when deriving activations.
func TestDefaultContractSupplyRequireReady(t *testing.T) {
	for _, typ := range []string{"xflow.supply.external", "xflow.supply.static"} {
		t.Run(typ, func(t *testing.T) {
			def := descriptorDefault(t, typ, 0, supply.ParamRequireReady)
			base := map[string]any{supply.ParamResource: "r"}
			assertSameBehaviour(t, defaultVariants(t, base, supply.ParamRequireReady, def), func(_ *testing.T, params map[string]any) any {
				return supply.RequireReady(params)
			})
		})
	}
}

// --- BuiltinFallbacks: params with a handler fallback but no Default --------
//
// Each constant entry is checked the same way a Default is: absent and
// "param = entry.Value" must behave identically. Derived entries check the
// formula in Derived.

func runFallbackChecks(t *testing.T, typ string, version int, checks map[string]func(*testing.T, nodeinternal.Fallback)) {
	t.Helper()
	entries := nodeinternal.BuiltinFallbacksFor(typ, version)
	if len(entries) == 0 {
		t.Fatalf("BuiltinFallbacks has no entry for %s@%d", typ, version)
	}
	seen := map[string]bool{}
	for _, f := range entries {
		seen[f.Param] = true
		check, ok := checks[f.Param]
		if !ok {
			t.Errorf("BuiltinFallbacks entry %s@%d/%s has no handler check", typ, version, f.Param)
			continue
		}
		t.Run(f.Param, func(t *testing.T) { check(t, f) })
	}
	for param := range checks {
		if !seen[param] {
			t.Errorf("check for %s@%d/%s has no BuiltinFallbacks entry", typ, version, param)
		}
	}
}

// sameAsFallback is the check for a constant entry: base with the param
// absent behaves like base with the param set to the entry's Value.
func sameAsFallback(base map[string]any, observe func(t *testing.T, params map[string]any) any) func(*testing.T, nodeinternal.Fallback) {
	return func(t *testing.T, f nodeinternal.Fallback) {
		if f.Value == nil {
			t.Fatalf("%s/%s has no constant Value", f.Type, f.Param)
		}
		absent := maps.Clone(base)
		delete(absent, f.Param)
		set := maps.Clone(absent)
		set[f.Param] = f.Value
		want := observe(t, absent)
		if got := observe(t, set); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s=%#v: effective behaviour %#v, want %#v (param absent)", f.Param, f.Value, got, want)
		}
	}
}

func TestFallbackContractHTTP(t *testing.T) {
	var sentContentType string
	var sentBody string
	orig := actionimpl.DefaultHTTPClient
	actionimpl.DefaultHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sentContentType = r.Header.Get("Content-Type")
		sentBody = ""
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			sentBody = string(b)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
		}, nil
	})}
	t.Cleanup(func() { actionimpl.DefaultHTTPClient = orig })

	h := actionHandler(t, "xflow.http")
	type request struct {
		ContentType, Body string
		Out               outcome
	}
	base := map[string]any{"method": "POST", "url": "https://example.test/x", "body": map[string]any{"a": 1}}
	runFallbackChecks(t, "xflow.http", 1, map[string]func(*testing.T, nodeinternal.Fallback){
		"mode": sameAsFallback(base, func(t *testing.T, params map[string]any) any {
			out := outcomeOf(h.Execute(context.Background(), &types.Input{Params: params}))
			return request{sentContentType, sentBody, out}
		}),
	})
}

func TestFallbackContractMerge(t *testing.T) {
	h := actionHandler(t, "xflow.merge")
	runFallbackChecks(t, "xflow.merge", 1, map[string]func(*testing.T, nodeinternal.Fallback){
		"mode": sameAsFallback(map[string]any{}, func(t *testing.T, params map[string]any) any {
			return outcomeOf(h.Execute(context.Background(), &types.Input{
				Params: params,
				Inputs: map[string]any{"a": map[string]any{"v": 1}, "b": map[string]any{"w": 2}},
			}))
		}),
	})
}

func TestFallbackContractSwitch(t *testing.T) {
	h := actionHandler(t, "xflow.switch")
	run := func(t *testing.T, params map[string]any) any {
		return outcomeOf(h.Execute(context.Background(), &types.Input{Params: params, Data: map[string]any{"n": 5}}))
	}
	runFallbackChecks(t, "xflow.switch", 1, map[string]func(*testing.T, nodeinternal.Fallback){
		// A rule that matches, so "mode" decides between rules and anything else.
		"mode": sameAsFallback(map[string]any{
			"rules": []any{map[string]any{"condition": "n > 1", "output": "big"}},
		}, run),
		// No rule matches, so the default port is what is observed.
		"default_output": sameAsFallback(map[string]any{
			"mode":  "rules",
			"rules": []any{map[string]any{"condition": "n > 10", "output": "big"}},
		}, run),
	})
}

func TestFallbackContractWait(t *testing.T) {
	h := suspendingHandler(t, "xflow.wait")
	runFallbackChecks(t, "xflow.wait", 1, map[string]func(*testing.T, nodeinternal.Fallback){
		// Derived: "<node name>/signal".
		"signal_name": func(t *testing.T, _ nodeinternal.Fallback) {
			prepare := func(params map[string]any) types.SuspendSpec {
				spec, err := h.PrepareSuspend(context.Background(), &types.Input{NodeName: "w", Params: params})
				if err != nil {
					t.Fatal(err)
				}
				return *spec
			}
			absent := prepare(map[string]any{"mode": "signal"})
			explicit := prepare(map[string]any{"mode": "signal", "signal_name": "w/signal"})
			if !reflect.DeepEqual(absent, explicit) || !reflect.DeepEqual(absent.Signals, []string{"w/signal"}) {
				t.Fatalf("absent signal_name = %+v, want %+v", absent, explicit)
			}
		},
	})
}
