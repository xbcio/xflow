package js

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/xbcio/xflow/nodes/node/script"
)

func newGoja() script.Engine {
	e, _ := script.Lookup("js", "goja")
	return e
}

func TestGoja_ObjectCompletion(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		`({status: 'ok', len: $input.name.length})`,
		map[string]any{"$input": map[string]any{"name": "abcd"}},
		script.DefaultHelpers())
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
		`({t: $credential.token, k: $credentials.aes_key.key})`,
		map[string]any{
			"$credential":  map[string]any{"token": "t-1"},
			"$credentials": map[string]any{"aes_key": map[string]any{"key": "kk"}},
		},
		script.DefaultHelpers())
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
		`({enc: $helpers.base64Encode('hi')})`,
		nil, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.(map[string]any)["enc"] != "aGk=" {
		t.Fatalf("base64 helper wrong: %v", out)
	}
}

func TestGoja_SandboxNoIO(t *testing.T) {
	out, err := newGoja().Execute(context.Background(),
		`({hasRequire: typeof require, hasFetch: typeof fetch, hasProcess: typeof process, hasXHR: typeof XMLHttpRequest})`,
		nil, script.DefaultHelpers())
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
	_, err := newGoja().Execute(ctx, `while(true){}`, nil, script.DefaultHelpers())
	if err == nil {
		t.Fatal("expected timeout interrupt error")
	}
}

func TestGoja_PoolIsolation(t *testing.T) {
	e := newGoja()
	_, err := e.Execute(context.Background(), `leaked = 99; ({})`, nil, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("first exec error: %v", err)
	}
	out, err := e.Execute(context.Background(), `({seen: typeof leaked})`, nil, script.DefaultHelpers())
	if err != nil {
		t.Fatalf("second exec error: %v", err)
	}
	if got := out.(map[string]any)["seen"]; got != "undefined" {
		t.Fatalf("pool leak: leaked = %v across executions", got)
	}
}

func TestGoja_RuntimeError(t *testing.T) {
	_, err := newGoja().Execute(context.Background(), `throw new Error('boom')`, nil, script.DefaultHelpers())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestGoja_TimeoutThenReuse(t *testing.T) {
	e := newGoja()
	// First exec times out under a tight deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := e.Execute(ctx, `while(true){}`, nil, script.DefaultHelpers()); err == nil {
		t.Fatal("expected timeout error")
	}
	// A subsequent clean exec on the same engine must succeed (no stale interrupt,
	// no poisoned pooled VM).
	for i := 0; i < 20; i++ {
		out, err := e.Execute(context.Background(), `({ok: 1 + 1})`, nil, script.DefaultHelpers())
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
		`(function f(){ return f(); })()`,
		nil, script.DefaultHelpers())
	if err == nil {
		t.Fatal("expected stack overflow error")
	}
	var soe *goja.StackOverflowError
	if !errors.As(err, &soe) {
		t.Fatalf("expected *goja.StackOverflowError, got %T: %v", err, err)
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
