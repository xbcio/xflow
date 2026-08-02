package wasm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// --- Test helpers ---

// mockLoader is a controllable ConfigLoader for testing watcher behavior.
type mockLoader struct {
	cfg     atomic.Value // []byte
	version atomic.Value // string
	err     atomic.Value // error (wrapped in errHolder to store nil)
	calls   atomic.Int64
}

type errHolder struct{ err error }

func newMockLoader(cfg []byte, version string) *mockLoader {
	m := &mockLoader{}
	m.cfg.Store(cfg)
	m.version.Store(version)
	m.err.Store(errHolder{nil})
	return m
}

func (m *mockLoader) Load(_ context.Context) ([]byte, string, error) {
	m.calls.Add(1)
	if h, ok := m.err.Load().(errHolder); ok && h.err != nil {
		return nil, "", h.err
	}
	cfg := m.cfg.Load().([]byte)
	ver := m.version.Load().(string)
	return cfg, ver, nil
}

func (m *mockLoader) setConfig(cfg []byte, version string) {
	m.cfg.Store(cfg)
	m.version.Store(version)
}

func (m *mockLoader) setError(err error) {
	m.err.Store(errHolder{err})
}

func (m *mockLoader) loadCount() int64 {
	return m.calls.Load()
}

// registerLoaderForTest registers a loader and removes it on cleanup, so tests
// do not leak registrations into the process-wide registry.
//
// It writes the registry directly rather than calling RegisterConfigLoader
// because it must also DELETE the entry on cleanup, and there is no public
// unregister — the production lifetime of a loader registration is the process.
// The key must be derived exactly as RegisterConfigLoader derives it: keying a
// test entry by the raw code string while production keys by module identity
// would make warm-up skip every entry these tests install, and every assertion
// about swapping would then pass vacuously against a module that never warmed.
func registerLoaderForTest(t *testing.T, code string, loader ConfigLoader, ttl time.Duration) {
	t.Helper()
	key := registryKeyOrRaw(code)
	loaderMu.Lock()
	loaderRegistry[key] = loaderEntry{code: code, loader: loader, ttl: ttl}
	loaderMu.Unlock()
	t.Cleanup(func() {
		loaderMu.Lock()
		delete(loaderRegistry, key)
		loaderMu.Unlock()
	})
}

// waitForGen blocks until the engine's active pool advances past gen, failing
// the test on timeout.
func waitForGen(t *testing.T, e *reactorEngine, gen uint64, why string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s (gen still %d)", why, gen)
		case <-time.After(20 * time.Millisecond):
			if p := e.active.Load(); p != nil && p.gen > gen {
				return
			}
		}
	}
}

// goodConfig returns a valid reactor config JSON with one rule.
func goodConfig(ruleName, expr string) []byte {
	return []byte(`{"rules":[{"name":"` + ruleName + `","expr":"` + expr + `"}]}`)
}

// emptyConfig returns a valid reactor config JSON with zero rules.
func emptyConfig() []byte {
	return []byte(`{"rules":[]}`)
}

// badConfig returns a config with an unparseable expression that causes
// configure to return errConfig.
func badConfig() []byte {
	return []byte(`{"rules":[{"name":"broken","expr":"this is (not valid expr"}]}`)
}

// --- Tests ---

// TestConfigLoader_VersionUnchangedNoSwap asserts that when the loader returns
// the same version, the pool generation does not change (zero swap cost).
func TestConfigLoader_VersionUnchangedNoSwap(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	// Register and warmup.
	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	gen := e.active.Load().gen

	// Wait for a couple of ticks — version is unchanged so gen must not move.
	time.Sleep(200 * time.Millisecond)
	if newGen := e.active.Load().gen; newGen != gen {
		t.Fatalf("version unchanged but pool swapped: gen %d → %d", gen, newGen)
	}
}

// TestConfigLoader_VersionChangeTriggersSwap asserts that when the loader
// returns a new version, the watcher builds a new pool and the new rules take
// effect.
func TestConfigLoader_VersionChangeTriggersSwap(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	e, _ := h.engineForCode(ctx, code)
	gen1 := e.active.Load().gen

	// Verify the initial rule works.
	out, err := f.Execute(ctx, code, map[string]any{"x": 8.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatal("v1: expected big to match x=8")
	}

	// Update to a new version with a different rule.
	loader.setConfig(goodConfig("small", "x < 3"), "v2")

	// Wait for watcher to pick up the change.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for version change to trigger swap")
		case <-time.After(20 * time.Millisecond):
			if e.active.Load().gen > gen1 {
				goto swapped
			}
		}
	}
swapped:

	// Verify new rule is in effect.
	out, err = f.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if !matched(t, out)["small"] {
		t.Fatalf("v2: expected small to match x=1, got %v", matched(t, out))
	}
	// Old rule must be gone.
	if matched(t, out)["big"] {
		t.Fatal("v2: stale rule 'big' still active after swap")
	}
}

// TestConfigLoader_LoadErrorPreservesLastGood asserts that when Load returns an
// error, the current pool remains active (§6.5: never clear rules on source
// flap).
func TestConfigLoader_LoadErrorPreservesLastGood(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	e, _ := h.engineForCode(ctx, code)
	gen := e.active.Load().gen

	// Make loader fail.
	loader.setError(errors.New("network timeout"))
	time.Sleep(200 * time.Millisecond)

	// Pool must be preserved.
	if e.active.Load().gen != gen {
		t.Fatal("load error caused pool swap — must preserve last-good")
	}
	// Eval must still work.
	out, err := f.Execute(ctx, code, map[string]any{"x": 8.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute after load error: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatal("last-good pool not serving after load error")
	}
}

// TestConfigLoader_BadConfigRejectedVersionNotRecorded asserts that when a new
// config fails to build a pool (bad rules), the bad version is NOT recorded as
// applied. Once the config source is fixed, the next poll retries and succeeds.
func TestConfigLoader_BadConfigRejectedVersionNotRecorded(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	e, _ := h.engineForCode(ctx, code)
	gen := e.active.Load().gen

	// Push a bad config with a new version.
	loader.setConfig(badConfig(), "v2-bad")
	time.Sleep(200 * time.Millisecond)

	// Pool must be unchanged (bad config rejected, last-good preserved).
	if e.active.Load().gen != gen {
		t.Fatal("bad config caused a pool swap — must keep last-good")
	}

	// Fix the config with the SAME version "v2-bad" — the version was NOT
	// recorded, so the watcher retries it.
	loader.setConfig(goodConfig("small", "x < 3"), "v2-bad")

	// Wait for the retry to succeed.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout: bad version was recorded, retry never happened")
		case <-time.After(20 * time.Millisecond):
			if e.active.Load().gen > gen {
				goto recovered
			}
		}
	}
recovered:

	// Verify new rule is in effect.
	out, err := f.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute after recovery: %v", err)
	}
	if !matched(t, out)["small"] {
		t.Fatal("recovered config not in effect")
	}
}

// TestConfigLoader_FirstFailureTransientError asserts that when the initial
// warmup Load fails, active remains nil and Execute returns a transient error.
func TestConfigLoader_FirstFailureTransientError(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	loader := newMockLoader(nil, "")
	loader.cfg.Store([]byte(`{}`)) // won't be used
	loader.setError(errors.New("dns lookup failed"))

	registerLoaderForTest(t, code, loader, 0)

	ctx := context.Background()
	err := f.warmup(ctx)
	if err == nil {
		t.Fatal("warmup should return error when first Load fails")
	}

	// Execute must return a transient (retryable) error.
	_, execErr := f.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if execErr == nil {
		t.Fatal("expected transient error from Execute with nil pool")
	}
	var ce *types.ClassifiedError
	if !errors.As(execErr, &ce) {
		t.Fatalf("expected ClassifiedError, got %T: %v", execErr, execErr)
	}
	if !ce.Retryable {
		t.Fatalf("error must be retryable, got: %+v", ce)
	}
	if ce.Kind != types.ErrorKindTransient {
		t.Fatalf("error kind must be transient, got: %v", ce.Kind)
	}
}

// TestConfigLoader_FirstBadConfigTransientError asserts that when the initial
// warmup gets config that fails buildPool, Execute returns transient.
func TestConfigLoader_FirstBadConfigTransientError(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	loader := newMockLoader(badConfig(), "v1-bad")

	registerLoaderForTest(t, code, loader, 0)

	ctx := context.Background()
	err := f.warmup(ctx)
	if err == nil {
		t.Fatal("warmup should return error when first config is bad")
	}

	_, execErr := f.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if execErr == nil {
		t.Fatal("expected transient error from Execute with nil pool")
	}
	var ce *types.ClassifiedError
	if !errors.As(execErr, &ce) {
		t.Fatalf("expected ClassifiedError, got %T: %v", execErr, execErr)
	}
	if !ce.Retryable {
		t.Fatalf("error must be retryable, got: %+v", ce)
	}
}

// TestConfigLoader_EmptyRulesetValid asserts that an empty ruleset ({"rules":[]})
// is a valid config that warms successfully (§6.5).
func TestConfigLoader_EmptyRulesetValid(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	loader := newMockLoader(emptyConfig(), "v-empty")

	registerLoaderForTest(t, code, loader, 0)

	ctx := context.Background()
	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup with empty config should succeed: %v", err)
	}

	out, err := f.Execute(ctx, code, map[string]any{"x": 8.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute with empty config: %v", err)
	}
	if len(matched(t, out)) != 0 {
		t.Fatalf("empty config should match nothing, got %v", matched(t, out))
	}
}

// TestConfigLoader_TTLZeroNoGoroutine asserts that ttl <= 0 does not start a
// background watcher: Load is called exactly once during warmup and never again.
func TestConfigLoader_TTLZeroNoGoroutine(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	registerLoaderForTest(t, code, loader, 0)

	ctx := context.Background()
	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	countAfterWarmup := loader.loadCount()
	if countAfterWarmup != 1 {
		t.Fatalf("expected exactly 1 Load call during warmup, got %d", countAfterWarmup)
	}

	// Wait; no further calls should happen.
	time.Sleep(200 * time.Millisecond)
	if loader.loadCount() != countAfterWarmup {
		t.Fatalf("ttl=0 but loader was called again: %d calls", loader.loadCount())
	}
}

// TestConfigLoader_CtxCancelStopsGoroutine asserts that cancelling the warmup
// context stops the background watcher goroutine (Load stops being called).
func TestConfigLoader_CtxCancelStopsGoroutine(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	cfg := goodConfig("big", "x > 5")
	loader := newMockLoader(cfg, "v1")

	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	if err := f.warmup(ctx); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// Let the watcher tick a few times.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Wait for goroutine to see the cancellation.
	time.Sleep(100 * time.Millisecond)
	countAtCancel := loader.loadCount()

	// Wait more — no further calls should happen.
	time.Sleep(200 * time.Millisecond)
	if loader.loadCount() != countAtCancel {
		t.Fatalf("ctx cancelled but loader still called: %d → %d",
			countAtCancel, loader.loadCount())
	}
}

// TestConfigLoader_LegacyPathUnchanged asserts that modules WITHOUT a registered
// loader still follow the legacy path: config from globals["$config"], sha256
// comparison, ensurePool on every call.
func TestConfigLoader_LegacyPathUnchanged(t *testing.T) {
	// Use a fresh host to avoid contamination from other tests' registrations.
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)

	// No loader registered for this code.
	ctx := context.Background()
	out, err := f.Execute(ctx, code, map[string]any{
		"$config": ruleConfig([2]string{"big", "x > 5"}),
		"x":       8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("legacy path: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatal("legacy path: expected big to match x=8")
	}

	// Swap config via globals (legacy).
	out, err = f.Execute(ctx, code, map[string]any{
		"$config": ruleConfig([2]string{"neg", "x < 0"}),
		"x":       8.0,
	}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("legacy path swap: %v", err)
	}
	if matched(t, out)["big"] {
		t.Fatal("legacy path: stale rule 'big' after config swap")
	}
}

// TestConfigLoader_RecoversAfterBootTimeSourceFailure is the counterpart to
// TestConfigLoader_FirstFailureTransientError: warmup failing must not be
// terminal. Execute does no lazy load on the loader path, so if warmup skipped
// startWatcher when the first Load failed, the module would serve transient
// errors forever even after the source came back.
func TestConfigLoader_RecoversAfterBootTimeSourceFailure(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}
	code := b64(reactorWasm)
	loader := newMockLoader(goodConfig("big", "x > 5"), "v1")
	loader.setError(errors.New("config service down at boot"))
	registerLoaderForTest(t, code, loader, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := f.warmup(ctx); err == nil {
		t.Fatal("warmup should report the failed config source")
	}

	e, _ := h.engineForCode(ctx, code)
	if e.active.Load() != nil {
		t.Fatal("active pool must stay nil while the source is down")
	}

	// Source comes back. The watcher — which must have started despite the
	// warmup failure — is the only thing that can notice.
	loader.setError(nil)
	waitForGen(t, e, 0, "watcher to recover after boot-time source failure")

	out, err := f.Execute(ctx, code, map[string]any{"x": 8.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("execute after recovery: %v", err)
	}
	if !matched(t, out)["big"] {
		t.Fatalf("recovered config not in effect, got %v", matched(t, out))
	}
}

// TestConfigLoader_OneBadSourceDoesNotBlockOthers asserts warmup keeps going
// after a module's config source fails: an unrelated module must still get its
// warm pool, rather than being denied one by an early return.
func TestConfigLoader_OneBadSourceDoesNotBlockOthers(t *testing.T) {
	h := newReactorHost()
	f := &reactorFacade{host: h}

	// Two distinct modules: reactorWasm and reactorMinWasm compile separately,
	// so each gets its own reactorEngine and pool.
	badCode := b64(reactorWasm)
	goodCode := b64(reactorMinWasm)

	badLoader := newMockLoader(nil, "")
	badLoader.setError(errors.New("unreachable"))
	registerLoaderForTest(t, badCode, badLoader, 0)
	registerLoaderForTest(t, goodCode, StaticLoader(emptyConfig()), 0)

	ctx := context.Background()
	if err := f.warmup(ctx); err == nil {
		t.Fatal("warmup should report the failing source")
	}

	badEngine, _ := h.engineForCode(ctx, badCode)
	if badEngine.active.Load() != nil {
		t.Fatal("failing module must have no active pool")
	}

	goodEngine, err := h.engineForCode(ctx, goodCode)
	if err != nil {
		t.Fatalf("engineForCode(good): %v", err)
	}
	if goodEngine.active.Load() == nil {
		t.Fatal("healthy module was denied its pool by the other module's failure")
	}
}

// TestStaticLoader asserts StaticLoader returns constant config and version.
func TestStaticLoader(t *testing.T) {
	cfg := []byte(`{"rules":[{"name":"x","expr":"1>0"}]}`)
	sl := StaticLoader(cfg)

	cfg1, v1, err := sl.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cfg2, v2, err := sl.Load(context.Background())
	if err != nil {
		t.Fatalf("load2: %v", err)
	}
	if string(cfg1) != string(cfg2) {
		t.Fatal("StaticLoader returned different config bytes")
	}
	if v1 != v2 {
		t.Fatal("StaticLoader returned different versions")
	}
	if v1 == "" {
		t.Fatal("StaticLoader version is empty")
	}
	if string(cfg1) != string(cfg) {
		t.Fatal("StaticLoader returned wrong config")
	}
}
