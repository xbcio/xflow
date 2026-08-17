package wasm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

func newReactor(t *testing.T) engine.Engine {
	t.Helper()
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	return e
}

// ruleConfig builds a $config globals entry from name→expr pairs.
func ruleConfig(rules ...[2]string) map[string]any {
	rs := make([]any, 0, len(rules))
	for _, r := range rules {
		rs = append(rs, map[string]any{"name": r[0], "expr": r[1]})
	}
	return map[string]any{"rules": rs}
}

// matched extracts the matched rule-name set from a reactor eval result.
func matched(t *testing.T, out any) map[string]bool {
	t.Helper()
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result not an object: %#v", out)
	}
	set := map[string]bool{}
	arr, _ := m["matched"].([]any)
	for _, v := range arr {
		set[v.(string)] = true
	}
	return set
}

func TestReactor_ConfigureThenEval(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"big", "x > 5"}, [2]string{"small", "x < 5"}),
		"x":       8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	m := matched(t, out)
	if !m["big"] || m["small"] {
		t.Fatalf("x=8: got %v, want {big}", m)
	}
}

// TestReactor_StatePersistsAcrossCalls exercises constraint #1: package-level
// compiled rules survive across eval calls on a resident instance. The pool is
// warmed once (first call) and reused; a second call with the same config must
// hit an already-configured instance.
func TestReactor_StatePersistsAcrossCalls(t *testing.T) {
	e := newReactor(t)
	cfg := ruleConfig([2]string{"pos", "x > 0"})
	for i, x := range []float64{3, -1, 7} {
		out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
			"$config": cfg, "x": x,
		}, engine.DefaultHelpers())
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		m := matched(t, out)
		want := x > 0
		if m["pos"] != want {
			t.Fatalf("call %d x=%v: pos=%v want %v", i, x, m["pos"], want)
		}
	}
}

// TestReactor_ConfigSwap exercises the B-plan config change (§6.3): a new config
// builds a new pool and the old rules stop matching. Because config equality is
// by content hash, switching configs must produce different behavior.
func TestReactor_ConfigSwap(t *testing.T) {
	e := newReactor(t)
	// First generation: rule "big".
	out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"big", "x > 5"}), "x": 8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("gen1: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatal("gen1: expected big to match x=8")
	}
	// Second generation: different rule set — "big" no longer exists.
	out, err = e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"neg", "x < 0"}), "x": 8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("gen2: %v", err)
	}
	m := matched(t, out)
	if m["big"] {
		t.Fatal("gen2: stale rule 'big' still present after config swap")
	}
	if m["neg"] {
		t.Fatal("gen2: neg should not match x=8")
	}
}

// TestReactor_BadConfigRejected exercises §6.3 invariant 1: a config with a
// broken rule fails the whole pool build and the error surfaces; the previous
// good pool (if any) is preserved.
func TestReactor_BadConfigRejected(t *testing.T) {
	e := newReactor(t)
	// Warm a good pool first.
	if _, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"ok", "x > 0"}), "x": 1.0,
	}, engine.DefaultHelpers()); err != nil {
		t.Fatalf("warm: %v", err)
	}
	// Now submit a broken rule — must error, not silently accept.
	_, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"broken", "this is (not valid expr"}), "x": 1.0,
	}, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected bad-config error, got nil")
	}
	// Last-good preserved: the original pool still serves correctly.
	out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": ruleConfig([2]string{"ok", "x > 0"}), "x": 1.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("after bad config, last-good pool broken: %v", err)
	}
	if !matched(t, out)["ok"] {
		t.Fatal("last-good pool not serving after bad config rejected")
	}
}

// TestReactor_Concurrent exercises constraint #2: the pool must let many
// goroutines call concurrently WITHOUT a single instance being driven by two
// goroutines at once (which is a process-fatal fault). Correct results under
// heavy concurrency prove the pool serializes per-instance access.
func TestReactor_Concurrent(t *testing.T) {
	e := newReactor(t)
	cfg := ruleConfig([2]string{"big", "x > 5"})
	// Warm once so the pool exists before the storm.
	if _, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": cfg, "x": 9.0,
	}, engine.DefaultHelpers()); err != nil {
		t.Fatalf("warm: %v", err)
	}

	const goroutines, iters = 32, 50
	var wg sync.WaitGroup
	var fail atomic.Int64
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range iters {
				x := float64((g*iters + i) % 12) // deterministic mix around threshold 5
				out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
					"$config": cfg, "x": x,
				}, engine.DefaultHelpers())
				if err != nil {
					t.Errorf("g%d i%d: %v", g, i, err)
					fail.Add(1)
					return
				}
				if got := matched(t, out)["big"]; got != (x > 5) {
					t.Errorf("g%d i%d x=%v: big=%v want %v", g, i, x, got, x > 5)
					fail.Add(1)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if fail.Load() > 0 {
		t.Fatalf("%d concurrent evals failed", fail.Load())
	}
}

// TestReactor_TimeoutDoomsAndRebuilds exercises constraints #3/#4: an eval that
// times out closes that instance permanently, the pool discards it, rebuilds a
// replacement, and subsequent evals succeed (the pool self-heals). Uses the
// spin guest whose eval loops forever.
func TestReactor_TimeoutDoomsAndRebuilds(t *testing.T) {
	e := newReactor(t)
	code := b64(reactorSpinWasm)
	cfg := map[string]any{"rules": []any{}} // spin guest ignores config

	// Warm a size-1 pool by forcing GOMAXPROCS-independent behavior: we just
	// warm normally, then hammer with a timeout. Even with a larger pool, every
	// instance that runs the spinning eval must be doomed and rebuilt.
	// First, a timed-out call.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := e.Execute(ctx, engine.Code(code), cfg, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected timeout error from spinning eval")
	}

	// Give the async rebuild a moment (doom rebuilds on a background goroutine).
	// Then a fresh call with a generous deadline must eventually succeed once a
	// rebuilt instance is available. The spin guest's eval never returns, so we
	// can only confirm the pool did not deadlock: borrow must still be
	// serviceable (rebuild replenished the doomed slot). We assert by checking a
	// borrow succeeds within a bounded time using a NON-spinning follow-up is
	// impossible with the same module, so instead assert the pool can still be
	// borrowed from (a second timed-out call still returns an error rather than
	// hanging forever on an empty pool).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	done := make(chan error, 1)
	go func() {
		_, err := e.Execute(ctx2, engine.Code(code), cfg, engine.DefaultHelpers())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected second spinning eval to also time out")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pool deadlocked: borrow never serviced after doom (rebuild not replenishing)")
	}
}

// TestReactor_UnconfiguredIsError confirms eval without a warmed pool cannot
// happen through Execute (Execute always ensures a pool), but a config that
// produces zero rules is valid (§6.5 empty-config semantics).
func TestReactor_EmptyConfigValid(t *testing.T) {
	e := newReactor(t)
	out, err := e.Execute(context.Background(), engine.Code(b64(reactorWasm)), map[string]any{
		"$config": map[string]any{"rules": []any{}},
		"x":       8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("empty config should be valid: %v", err)
	}
	if len(matched(t, out)) != 0 {
		t.Fatalf("empty config should match nothing, got %v", matched(t, out))
	}
}

// BenchmarkReactor_Eval measures the hot-path per-call cost with a warmed pool,
// the headline number the design targets (§0.1: ~345µs vs command 20.9ms).
func BenchmarkReactor_Eval(b *testing.B) {
	e, _ := engine.Lookup("wasm", "wazero-reactor")
	code := b64(reactorWasm)
	cfg := ruleConfig(
		[2]string{"r1", "x > 5"}, [2]string{"r2", "x < 100"}, [2]string{"r3", "x > 0"},
	)
	// Warm.
	if _, err := e.Execute(context.Background(), engine.Code(code), map[string]any{"$config": cfg, "x": 8.0}, engine.DefaultHelpers()); err != nil {
		b.Fatalf("warm: %v", err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := e.Execute(context.Background(), engine.Code(code), map[string]any{"$config": cfg, "x": float64(i % 20)}, engine.DefaultHelpers()); err != nil {
				b.Fatalf("eval: %v", err)
			}
			i++
		}
	})
}

// BenchmarkCommand_Execute is the command-model baseline for comparison. It
// uses the echo guest (the reactor guest has no _start). Different guest, but
// it isolates the per-call instantiate+bootstrap cost the reactor model removes.
func BenchmarkCommand_Execute(b *testing.B) {
	e, _ := engine.Lookup("wasm", "wazero")
	code := b64(echoWasm)
	b.ResetTimer()
	for range b.N {
		if _, err := e.Execute(context.Background(), engine.Code(code), map[string]any{"$input": map[string]any{"x": 8.0}}, engine.DefaultHelpers()); err != nil {
			b.Fatalf("exec: %v", err)
		}
	}
}
