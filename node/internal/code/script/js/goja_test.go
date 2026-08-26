package js

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

func newGoja() engine.Engine {
	e, _ := engine.Lookup("js", "goja")
	return e
}

func TestGoja_ObjectCompletion(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		engine.Code(`({status: 'ok', len: $input.name.length})`),
		map[string]any{"$input": map[string]any{"name": "abcd"}},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	if m["status"] != "ok" {
		t.Fatalf("status = %v", m["status"])
	}
}

func TestGoja_ReadsCredential(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		engine.Code(`({t: $credential.token, k: $credentials.aes_key.key})`),
		map[string]any{
			"$credential":  map[string]any{"token": "t-1"},
			"$credentials": map[string]any{"aes_key": map[string]any{"key": "kk"}},
		},
		engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	if m["t"] != "t-1" || m["k"] != "kk" {
		t.Fatalf("credential read wrong: %v", m)
	}
}

func TestGoja_HelpersBase64(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		engine.Code(`({enc: $helpers.base64Encode('hi')})`),
		nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["enc"] != "aGk=" {
		t.Fatalf("base64 helper wrong: %v", out)
	}
}

func TestGoja_SandboxNoIO(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		engine.Code(`({hasRequire: typeof require, hasFetch: typeof fetch, hasProcess: typeof process, hasXHR: typeof XMLHttpRequest})`),
		nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := out.(map[string]any)
	for _, k := range []string{"hasRequire", "hasFetch", "hasProcess", "hasXHR"} {
		if m[k] != "undefined" {
			t.Fatalf("sandbox leak: %s = %v, want undefined", k, m[k])
		}
	}
}

func TestGoja_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := newGoja().Execute(ctx, engine.Code(`while(true){}`), nil, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected timeout interrupt error")
	}
}

func TestGoja_PoolIsolation(t *testing.T) {
	e := newGoja()
	_, err := e.Execute(context.Background(), engine.Code(`leaked = 99; ({})`), nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("first exec error: %v", err)
	}
	out, err := e.Execute(context.Background(), engine.Code(`({seen: typeof leaked})`), nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("second exec error: %v", err)
	}
	if got := out.(map[string]any)["seen"]; got != "undefined" {
		t.Fatalf("pool leak: leaked = %v across executions", got)
	}
}

func TestGoja_RuntimeError(t *testing.T) {
	_, err := newGoja().Execute(context.Background(), engine.Code(`throw new Error('boom')`), nil, engine.DefaultHelpers())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestGoja_TimeoutThenReuse(t *testing.T) {
	e := newGoja()
	// First exec times out under a tight deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := e.Execute(ctx, engine.Code(`while(true){}`), nil, engine.DefaultHelpers()); err == nil {
		t.Fatal("expected timeout error")
	}
	// A subsequent clean exec on the same engine must succeed (no stale interrupt,
	// no poisoned pooled VM).
	for i := 0; i < 20; i++ {
		out, err := e.Execute(context.Background(), engine.Code(`({ok: 1 + 1})`), nil, engine.DefaultHelpers())
		if err != nil {
			t.Fatalf("iteration %d: clean exec failed after a timeout: %v", i, err)
		}
		if out.(map[string]any)["ok"] != int64(2) && out.(map[string]any)["ok"] != 2.0 {
			t.Fatalf("iteration %d: ok = %v", i, out.(map[string]any)["ok"])
		}
	}
}

// TestGoja_StackOverflow verifies SetMaxCallStackSize(DefaultGojaStackSize)
// surfaces unbounded recursion as a runtime error instead of crashing the
// host. goja returns *goja.StackOverflowError unwrapped under our %w wrap.
func TestGoja_StackOverflow(t *testing.T) {
	_, err := newGoja().Execute(context.Background(),
		engine.Code(`(function f(){ return f(); })()`),
		nil, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected stack overflow error")
	}
	var soe *goja.StackOverflowError
	if !errors.As(err, &soe) {
		t.Fatalf("expected *goja.StackOverflowError, got %T: %v", err, err)
	}
}

// TestGoja_PrototypePollutionDiscarded verifies that a script which mutates
// Object.prototype does not leak the mutation to subsequent executions: the
// tainted VM is discarded instead of returned to the pool.
func TestGoja_PrototypePollutionDiscarded(t *testing.T) {
	e := &gojaEngine{programs: newProgramCache(engine.DefaultProgramCacheSize)}
	if _, err := e.Execute(context.Background(), engine.Code(`Object.prototype.__xflow_poll = 42; ({})`), nil, engine.DefaultHelpers()); err != nil {
		t.Fatalf("polluting exec error: %v", err)
	}
	out, err := e.Execute(context.Background(), engine.Code(`({seen: typeof ({}).__xflow_poll})`), nil, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("post-pollution exec error: %v", err)
	}
	if got := out.(map[string]any)["seen"]; got != "undefined" {
		t.Fatalf("prototype pollution leaked across executions: %v", got)
	}
}

// TestGoja_ProgramCacheKeepsTheRecentlyUsed asserts the "LRU" part of the
// program cache, which TestGoja_ProgramCacheEvict above cannot see.
//
// That test compiles three distinct scripts once each, in order, and never
// touches an earlier one again — under which LRU and plain FIFO evict exactly
// the same entry. So programCache.get can be switched from c.Get to c.Peek,
// which returns the value without promoting it, and the whole package stays
// green while the cache degrades to insertion-order eviction.
//
// The cost is paid on the hot path this cache exists for. A deployment runs a
// small number of hot scripts against a long tail of one-off ones; with Peek,
// each new one-off script evicts whichever hot script was compiled longest ago
// regardless of how many times it has been executed since, and that script is
// recompiled from source on its very next message. goja.Compile is the single
// most expensive step in a JS node's execution.
//
// The sequence below is the minimum that distinguishes the two: with LRU the
// re-read of scripts[0] promotes it and scripts[1] is evicted; with Peek (or
// FIFO) scripts[0] is still the eldest and goes instead.
func TestGoja_ProgramCacheKeepsTheRecentlyUsed(t *testing.T) {
	e := &gojaEngine{programs: newProgramCache(2)}
	scripts := []string{`({a: 1})`, `({b: 2})`, `({c: 3})`}

	for i := 0; i < 2; i++ {
		if _, err := e.compile(scripts[i]); err != nil {
			t.Fatalf("compile %d: %v", i, err)
		}
	}
	// Re-execute the eldest entry: a cache hit that must count as a use.
	if _, err := e.compile(scripts[0]); err != nil {
		t.Fatalf("recompile scripts[0]: %v", err)
	}
	if _, err := e.compile(scripts[2]); err != nil {
		t.Fatalf("compile 2: %v", err)
	}

	if !e.programs.contains(scripts[0]) {
		t.Error("scripts[0] was evicted despite being used most recently: the cache " +
			"is evicting by insertion order, so a hot script is dropped whenever " +
			"enough one-off scripts arrive after it and is recompiled from source " +
			"on its next message")
	}
	if e.programs.contains(scripts[1]) {
		t.Error("scripts[1] survived: it is the least recently used entry and is " +
			"what a promoting cache evicts here")
	}
	if !e.programs.contains(scripts[2]) {
		t.Error("scripts[2] missing: the newest entry must always be resident")
	}
}

// TestGoja_ProgramCacheEvict drives a small isolated cache (capacity 2) past
// its limit and asserts the eldest entry is evicted. Uses a fresh engine
// instance to avoid polluting sharedGoja.
func TestGoja_ProgramCacheEvict(t *testing.T) {
	e := &gojaEngine{programs: newProgramCache(2)}
	scripts := []string{
		`({a: 1})`,
		`({b: 2})`,
		`({c: 3})`,
	}
	for i, code := range scripts {
		if _, err := e.compile(code); err != nil {
			t.Fatalf("compile %d: %v", i, err)
		}
	}
	if e.programs.contains(scripts[0]) {
		t.Fatal("expected scripts[0] to be evicted after capacity overflow")
	}
	if !e.programs.contains(scripts[1]) || !e.programs.contains(scripts[2]) {
		t.Fatal("expected scripts[1] and scripts[2] to remain")
	}
}
