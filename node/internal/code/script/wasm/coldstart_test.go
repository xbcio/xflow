package wasm

import (
	"context"
	"testing"
	"time"
)

// TestP2_ColdStartBudget is the P2 acceptance measurement from design §7:
// "process restart cold start < 100 ms". It drives the real reactorHost path
// (the same code the runner's warm-up calls), first with an empty cache
// directory to stand in for a fresh deploy, then with a brand-new host over the
// warm directory to stand in for a restart of that same deploy.
//
// Both phases include instantiating and configuring a full pool, so the number
// is what a runner actually pays before serving its first request — not just the
// CompileModule call in isolation.
func TestP2_ColdStartBudget(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	// The package-level cache is a sync.Once singleton, so this test measures
	// through a private cache to stay independent of test ordering.
	resetCacheForTest(t)

	code := b64(reactorWasm)
	cfg := ruleConfig([2]string{"big", "x > 5"})

	start := func() time.Duration {
		t.Helper()
		h := newReactorHost()
		f := &reactorFacade{host: h}
		h.addPrewarm(code, cfg)
		t0 := time.Now()
		if err := f.warmup(context.Background()); err != nil {
			t.Fatalf("warmup: %v", err)
		}
		return time.Since(t0)
	}

	cold := start()
	restart := start()

	t.Logf("first deploy (empty cache dir) = %v; after restart (warm cache dir) = %v", cold, restart)
	if restart >= cold {
		t.Fatalf("restart (%v) was not faster than first deploy (%v); disk cache is not helping", restart, cold)
	}
	if restart > 100*time.Millisecond {
		t.Fatalf("restart cold start %v exceeds the design's <100ms budget", restart)
	}
}
