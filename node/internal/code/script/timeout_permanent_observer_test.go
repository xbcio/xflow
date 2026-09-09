package script_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/node"
	scriptengine "github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// This file closes the gap where script.go's timeout and permanent-error
// branches DID call observeExecute -- so the observer was never silent -- but
// both used the same outcome="error" label a plain, retryable script failure
// also reports (TestScript_RuntimeErrorNotifiesObserver in script_test.go
// asserts exactly that label for `throw new Error(...)`). That collapse made
// xflow_script_execute_total{outcome="error"} conflate three operationally
// different causes: a deadline a retry can clear, a node that will never
// succeed on any retry, and a script's own deterministic reject. Each test
// below drives the REAL branch through Execute (a cancelled context or an
// engine error classified via types.ErrPermanent) rather than constructing an
// observer event directly, so a regression that stops calling observeExecute
// on these branches -- or reverts the label back to "error" -- fails here.

// TestScript_TimeoutNotifiesObserverWithOwnOutcome drives the single-record
// path's `if ctx.Err() != nil` branch (inside eng.Execute's error handler)
// with a genuinely cancelled context and asserts the observer receives
// outcome="timeout".
func TestScript_TimeoutNotifiesObserverWithOwnOutcome(t *testing.T) {
	failWith(t, errors.New("engine error content does not matter: ctx is already done"))

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	_, err := h.Execute(ctx, &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatal("expected a Go error for a cancelled context")
	}
	if rec.executes != 1 {
		t.Fatalf("execute notifications = %d, want 1", rec.executes)
	}
	if rec.lastOutcome != "timeout" {
		t.Fatalf("outcome = %q, want %q -- a deadline was reported under the "+
			"same label as an ordinary script failure, so the two are "+
			"indistinguishable in xflow_script_execute_total", rec.lastOutcome, "timeout")
	}
}

// TestScript_BatchTimeoutNotifiesObserverWithOwnOutcome is the batch-path
// twin: the Kafka batch trigger takes this path (script.go's batchRecords
// branch), which has its own copy of the outcome decision.
func TestScript_BatchTimeoutNotifiesObserverWithOwnOutcome(t *testing.T) {
	failWith(t, errors.New("engine error content does not matter: ctx is already done"))

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"messages": []any{map[string]any{"x": 1}}, "count": 1},
	}
	_, err := h.Execute(ctx, input)

	if err == nil {
		t.Fatal("expected a Go error for a cancelled context")
	}
	if rec.executes != 1 || rec.lastOutcome != "timeout" {
		t.Fatalf("execute notifications = %d, outcome = %q; want 1/timeout", rec.executes, rec.lastOutcome)
	}
}

// TestScript_PermanentErrorNotifiesObserverWithOwnOutcome drives the
// single-record path's types.IsPermanent(err) branch with a real engine
// failure classified via types.ErrPermanent and asserts the observer
// receives outcome="permanent".
func TestScript_PermanentErrorNotifiesObserverWithOwnOutcome(t *testing.T) {
	failWith(t, &permanentFault{err: errors.New("engine state unusable: compile cache corrupt")})

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	_, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatal("expected a Go error for a permanently-classified engine failure")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("returned error lost its permanent classification: %v", err)
	}
	if rec.executes != 1 || rec.lastOutcome != "permanent" {
		t.Fatalf("execute notifications = %d, outcome = %q; want 1/permanent -- "+
			"a node failing outright was reported under the same label as a "+
			"script's own deterministic reject", rec.executes, rec.lastOutcome)
	}
}

// TestScript_BatchPermanentErrorNotifiesObserverWithOwnOutcome is the
// batch-path twin. TestScript_PermanentBatchErrorStaysPermanent (in
// permanent_error_test.go) already pins the error classification for this
// path; this pins the metric.
func TestScript_BatchPermanentErrorNotifiesObserverWithOwnOutcome(t *testing.T) {
	failWith(t, &permanentFault{err: errors.New("engine state unusable: compile cache corrupt")})

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"messages": []any{map[string]any{"x": 1}}, "count": 1},
	}
	_, err := h.Execute(context.Background(), input)

	if err == nil {
		t.Fatal("expected a Go error for a permanently-classified batch failure")
	}
	if rec.executes != 1 || rec.lastOutcome != "permanent" {
		t.Fatalf("execute notifications = %d, outcome = %q; want 1/permanent", rec.executes, rec.lastOutcome)
	}
}

// okRuntime drives the branch the four tests above cannot reach: the engine
// RETURNS SUCCESSFULLY while the context has already expired. failEngine always
// fails, so every path it drives leaves through the error block; the pre-flight
// check at the top of the success path is only reachable with an engine that
// hands back a result. Registered under its own (language, runtime) pair rather
// than by installing a nil error in failWith's slot -- a nil slot would silently
// defeat that helper's "fail slot already held" mutual-exclusion guard.
const okRuntime = "ok-test"

type okEngine struct{}

func (okEngine) Name() string { return "js/" + okRuntime }

func (okEngine) Execute(_ context.Context, _ scriptengine.Source, _ map[string]any, _ scriptengine.Helpers) (any, error) {
	return map[string]any{"ok": true}, nil
}

func (okEngine) ExecuteBatch(_ context.Context, _ scriptengine.Source, records []any, _ map[string]any) ([]any, error) {
	out := make([]any, len(records))
	for i := range records {
		out[i] = map[string]any{"ok": true}
	}
	return out, nil
}

var okOnce sync.Once

func registerOKRuntime() {
	okOnce.Do(func() {
		scriptengine.Register("js", okRuntime, func() scriptengine.Engine { return okEngine{} })
	})
}

// TestScript_ExpiredContextOnSuccessNotifiesTimeout covers the pre-flight
// `ctx.Err() != nil` check that sits AFTER a successful engine run. It is the
// one label site the other four tests leave uncovered -- verified by mutation:
// flipping this site's "timeout" back to "error" reddened nothing before this
// test existed. Reaching it requires a race the node deliberately refuses to
// resolve in the engine's favour: the deadline passed while the script ran, so
// the result is discarded rather than accepted.
func TestScript_ExpiredContextOnSuccessNotifiesTimeout(t *testing.T) {
	registerOKRuntime()

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(okRuntime)
	_, err := h.Execute(ctx, &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatal("expected the raced result to be rejected, got a nil error " +
			"-- an expired deadline must not be resolved in the engine's favour")
	}
	// A deadline is environmental: the same input may well succeed next attempt,
	// so this must stay retryable. Were it permanent, the engine would skip it.
	if types.IsPermanent(err) {
		t.Fatalf("expired-context result classified permanent: %v", err)
	}
	if rec.executes != 1 {
		t.Fatalf("execute notifications = %d, want 1", rec.executes)
	}
	if rec.lastOutcome != "timeout" {
		t.Fatalf("outcome = %q, want %q -- a deadline that passed during a "+
			"successful run is still a deadline", rec.lastOutcome, "timeout")
	}
}
