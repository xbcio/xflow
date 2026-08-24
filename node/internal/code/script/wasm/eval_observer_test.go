package wasm

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// TestOnEvalReportsRealStdinSize drives a real eval through the registered
// engine and checks that OnEval reports the size of the payload the guest
// actually received.
//
// It asserts against encodeStdin's output rather than a fixed number, because
// the point of the metric is to track what CROSSES the boundary. encodeStdin
// strips keys ($items, $supplies) that the caller's env still holds, so a test
// that asserted the env's own size would pass while the metric reported
// something else entirely — and a future strip would move the real number
// without moving the assertion.
//
// Driving e.Execute rather than calling obs().OnEval directly is the whole
// value of this test: the observation sits inside evalFromPool, which only the
// real path reaches. A test that invoked the observer itself would prove the
// recorder works, not that anything calls it.
func TestOnEvalReportsRealStdinSize(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	in := &types.Input{Data: map[string]any{"$item": realisticRecord(2594), "x": float64(10)}}
	env := exprx.BuildExprEnv(in, nil)
	env["$config"] = ruleConfig([2]string{"r1", "x > 5"})

	want, err := encodeStdin(stripConfigKey(env))
	if err != nil {
		t.Fatalf("encodeStdin: %v", err)
	}

	if _, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), env, engine.DefaultHelpers()); err != nil {
		t.Fatalf("execute: %v", err)
	}

	calls := rec.evalCalls()
	if len(calls) != 1 {
		t.Fatalf("OnEval called %d times, want 1", len(calls))
	}
	if calls[0].stdinBytes != len(want) {
		t.Errorf("OnEval reported %d stdin bytes, want %d (encodeStdin output)",
			calls[0].stdinBytes, len(want))
	}
	// Duration must be a real measurement, not a zero-valued placeholder. A
	// wasm eval crossing the ABI cannot complete in zero wall-clock time, so
	// zero here means the timer was never started.
	if calls[0].d <= 0 {
		t.Errorf("OnEval reported duration %v, want a positive measurement", calls[0].d)
	}
}

// stripConfigKey removes $config the way splitConfig does before encodeStdin
// sees the globals, so the expected size matches what the guest receives.
func stripConfigKey(env map[string]any) map[string]any {
	out := make(map[string]any, len(env))
	for k, v := range env {
		if k == reactorConfigGlobal {
			continue
		}
		out[k] = v
	}
	return out
}

// TestOnEvalStdinSizeTracksPayload pins the property the metric exists for: a
// larger record must report proportionally more stdin bytes.
//
// This is what makes the histogram diagnostic rather than decorative. Eval cost
// is dominated by the guest re-parsing stdin, so the size series is how a
// payload regression — a redundant copy of the record leaking in — is told apart
// from the engine getting slower. If the reported size did not move with the
// record, both failures would look identical on the dashboard.
func TestOnEvalStdinSizeTracksPayload(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	cfg := ruleConfig([2]string{"r1", "x > 5"})
	for _, size := range []int{2594, 20000} {
		in := &types.Input{Data: map[string]any{"$item": realisticRecord(size), "x": float64(10)}}
		env := exprx.BuildExprEnv(in, nil)
		env["$config"] = cfg
		if _, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), env, engine.DefaultHelpers()); err != nil {
			t.Fatalf("execute size=%d: %v", size, err)
		}
	}

	calls := rec.evalCalls()
	if len(calls) != 2 {
		t.Fatalf("OnEval called %d times, want 2", len(calls))
	}
	small, large := calls[0].stdinBytes, calls[1].stdinBytes
	// The record reaches the payload more than once (flattened AND as $input),
	// so the growth is a multiple of the record delta rather than equal to it.
	// Asserting "strictly larger by at least the record delta" pins the
	// direction and the floor without hardcoding that multiplier, which a
	// legitimate change to the env shape is allowed to move.
	if large-small < 20000-2594 {
		t.Errorf("stdin grew by %d bytes for a %d-byte record increase; want at least the record delta",
			large-small, 20000-2594)
	}
}

// TestOnEvalCarriesNodeIdentity pins that eval cost is attributable.
//
// Duration and size answer "is this slow because it is big"; neither answers
// "which node". A runner hosting several script nodes — SAS runs a decode node
// and a clean node in the same process — merges them into one series, so the
// node that regressed is the one you cannot name.
//
// The identity comes from the context because that is the only thing crossing
// this boundary that the node layer controls. Driving e.Execute rather than
// calling obs().OnEval directly is what makes the test meaningful: the read sits
// inside evalFromPool, and a test that invoked the observer itself would prove
// only that the recorder records.
func TestOnEvalCarriesNodeIdentity(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	in := &types.Input{Data: map[string]any{"$item": realisticRecord(2594), "x": float64(10)}}
	env := exprx.BuildExprEnv(in, nil)
	env["$config"] = ruleConfig([2]string{"r1", "x > 5"})

	ctx := engine.WithNodeIdentity(context.Background(), engine.NodeIdentity{
		Workflow: "sas-collect", Node: "decode",
	})
	if _, err := e.Execute(ctx, engine.Code(b64(reactorWasm)), env, engine.DefaultHelpers()); err != nil {
		t.Fatalf("execute: %v", err)
	}

	calls := rec.evalCalls()
	if len(calls) != 1 {
		t.Fatalf("OnEval called %d times, want 1", len(calls))
	}
	if calls[0].workflow != "sas-collect" || calls[0].node != "decode" {
		t.Errorf("OnEval reported workflow=%q node=%q, want sas-collect/decode",
			calls[0].workflow, calls[0].node)
	}
}

// TestOnEvalReportsNoIdentityWhenTheContextCarriesNone guards against inventing
// a name.
//
// An engine reached without going through the node layer — a benchmark, a test,
// a future caller — has no node to name. Substituting a placeholder ("unknown",
// the engine name) would put a value on the series that reads as authoritative
// and is not, and once it is there nothing distinguishes it from a real node
// called "unknown". Empty says "unattributed" and says it honestly.
func TestOnEvalReportsNoIdentityWhenTheContextCarriesNone(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	in := &types.Input{Data: map[string]any{"$item": realisticRecord(2594), "x": float64(10)}}
	env := exprx.BuildExprEnv(in, nil)
	env["$config"] = ruleConfig([2]string{"r1", "x > 5"})

	if _, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), env, engine.DefaultHelpers()); err != nil {
		t.Fatalf("execute: %v", err)
	}

	calls := rec.evalCalls()
	if len(calls) != 1 {
		t.Fatalf("OnEval called %d times, want 1", len(calls))
	}
	if calls[0].workflow != "" || calls[0].node != "" {
		t.Errorf("OnEval reported workflow=%q node=%q for a context naming no node; want both empty",
			calls[0].workflow, calls[0].node)
	}
}
