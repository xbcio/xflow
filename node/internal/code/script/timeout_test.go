package script

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// readScriptTimeout is the only thing standing between a runaway script and a
// permanently occupied worker: Execute (script.go:251-256) applies
// context.WithTimeout only when the returned duration is > 0, so a function
// that returns 0 for any input removes the deadline entirely rather than
// shortening it. Its own doc comment states the contract — "Falls back to
// engine.DefaultScriptTimeout when absent or invalid so a script never runs
// without a deadline" — and nothing asserted it: grep over the repo finds no
// test naming readScriptTimeout, and DefaultScriptTimeout appears only in its
// own definition, in this call, and in an unrelated wasm benchmark that builds
// its own context.
//
// TestScript_CancelledContextIsTransient is the nearest existing test and does
// not cover this: it passes an already-cancelled context in, so the timeout
// never has to be derived at all.

func TestReadScriptTimeoutNeverReturnsZero(t *testing.T) {
	// Zero is the one value that is not a shorter or longer deadline but the
	// absence of one, so it gets its own assertion for every input shape a
	// stored workflow can produce — including the shapes that are *invalid*,
	// which is where a naive parser is most likely to return the zero value.
	for _, tc := range []struct {
		name   string
		params map[string]any
	}{
		{"nil params", nil},
		{"absent", map[string]any{"language": "js"}},
		{"json number", map[string]any{"timeout": float64(5)}},
		{"int", map[string]any{"timeout": 5}},
		{"int64", map[string]any{"timeout": int64(5)}},
		{"duration string", map[string]any{"timeout": "5s"}},
		{"zero", map[string]any{"timeout": float64(0)}},
		{"negative", map[string]any{"timeout": float64(-1)}},
		{"unparseable string", map[string]any{"timeout": "not-a-duration"}},
		{"wrong type", map[string]any{"timeout": true}},
		{"nil value", map[string]any{"timeout": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readScriptTimeout(tc.params)
			if got <= 0 {
				t.Fatalf("readScriptTimeout(%v) = %v: Execute only calls "+
					"context.WithTimeout when this is > 0, so a non-positive result "+
					"means the script runs with no deadline at all and a while(true) "+
					"holds its worker until the process is restarted", tc.params, got)
			}
		})
	}
}

func TestReadScriptTimeoutParsesEachAcceptedShape(t *testing.T) {
	// The four accepted shapes carry the same meaning and must produce the same
	// duration. Stored workflows reach here as JSON, so float64 and string are
	// the shapes production actually uses; int and int64 exist for in-process
	// Go callers. A parser that recognises only some of them silently
	// substitutes the 30s default for whatever the operator configured.
	for _, tc := range []struct {
		name  string
		value any
		want  time.Duration
	}{
		{"json number seconds", float64(5), 5 * time.Second},
		{"int seconds", 5, 5 * time.Second},
		{"int64 seconds", int64(5), 5 * time.Second},
		{"duration string", "5s", 5 * time.Second},
		{"sub-second duration string", "250ms", 250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readScriptTimeout(map[string]any{"timeout": tc.value})
			if got != tc.want {
				t.Fatalf("readScriptTimeout(%#v) = %v, want %v (default is %v — if "+
					"they are equal the shape was not recognised and the configured "+
					"value was silently discarded)", tc.value, got, tc.want,
					engine.DefaultScriptTimeout)
			}
		})
	}
}

func TestReadScriptTimeoutFallsBackToTheSharedDefault(t *testing.T) {
	// Pinned to the constant rather than to 30s: the point is that Execute and
	// engine/limits.go agree on one number, not what that number happens to be.
	// The bound below is what makes this more than a restatement — a fallback
	// that is technically positive but sub-millisecond would satisfy the
	// never-zero test above while failing every script.
	got := readScriptTimeout(map[string]any{"timeout": "garbage"})
	if got != engine.DefaultScriptTimeout {
		t.Fatalf("readScriptTimeout(invalid) = %v, want engine.DefaultScriptTimeout (%v)",
			got, engine.DefaultScriptTimeout)
	}
	if got < time.Second {
		t.Fatalf("the fallback deadline is %v: too short for any real script, so "+
			"every execution that omits an explicit timeout would fail", got)
	}
}

// TestScriptTimeoutActuallyBoundsARunawayScript is the behavioural half. The
// unit tests above prove the number is computed; this proves it reaches
// context.WithTimeout. Without it, deleting the two lines that apply the
// context (script.go:252-255) leaves every assertion above green.
func TestScriptTimeoutActuallyBoundsARunawayScript(t *testing.T) {
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script handler not registered")
	}

	params := map[string]any{
		"code":     `(function(){ while(true){} })()`,
		"language": "js",
		"runtime":  "goja",
		"timeout":  "200ms",
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := h.Execute(context.Background(), &types.Input{
			Params: params,
			Data:   map[string]any{},
		})
		done <- err
	}()

	// The upper bound is a failure bound, not a success gate: the test passes on
	// the handler returning, not on the clock. It is generous relative to the
	// 200ms deadline so a loaded machine cannot fail it, and far below the 30s
	// default so "the configured timeout was ignored and the default applied"
	// still fails here rather than merely running slowly.
	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("Execute() returned nil error for an infinite loop")
		}
		if elapsed > 10*time.Second {
			t.Fatalf("the runaway script ran for %v against a 200ms timeout: the "+
				"configured value did not reach the execution context", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Execute() never returned for `while(true){}` with timeout=200ms: " +
			"the deadline is not applied, so one bad script definition occupies a " +
			"worker slot until the runner is restarted")
	}
}

// TestScriptTimeoutDoesNotAbortAScriptThatFinishes is the negative control for
// the test above. Without it, readScriptTimeout could return one nanosecond and
// still satisfy every assertion here — the runaway would be killed, just along
// with everything else.
func TestScriptTimeoutDoesNotAbortAScriptThatFinishes(t *testing.T) {
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"code":     `({doubled: $input.x * 2})`,
			"language": "js",
			"runtime":  "goja",
		},
		Data: map[string]any{"x": 21.0},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v: a script with no explicit timeout must run "+
			"under the shared default, not under a degenerate one", err)
	}
	if out.Data["doubled"] == nil {
		t.Fatalf("out.Data = %#v, want a doubled field", out.Data)
	}
}
