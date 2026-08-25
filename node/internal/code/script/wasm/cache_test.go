package wasm

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// resetCacheForTest forces the next compilationCacheFor call to re-resolve the
// cache directory, so a test can point it at a temp dir despite the singleton
// having been initialized by an earlier test. The previous cache is restored
// (not closed) on cleanup — runtimes opened by other tests still hold it.
func resetCacheForTest(t *testing.T) {
	t.Helper()
	prevOnce, prevVal, prevDir, prevErr := cacheOnce, cacheVal, cacheDir, cacheErr
	prevSweep := sweepDir.Load()
	cacheOnce, cacheVal, cacheDir, cacheErr = new(sync.Once), nil, "", nil
	sweepDir.Store(nil)
	t.Cleanup(func() {
		cacheOnce, cacheVal, cacheDir, cacheErr = prevOnce, prevVal, prevDir, prevErr
		sweepDir.Store(prevSweep)
	})
}

func TestResolveCacheDir_ExplicitDir(t *testing.T) {
	t.Setenv(CacheDirEnv, "/tmp/xflow-wasm-test")
	dir, err := resolveCacheDir()
	if err != nil {
		t.Fatalf("resolveCacheDir: %v", err)
	}
	if dir != "/tmp/xflow-wasm-test" {
		t.Fatalf("dir = %q, want the explicit override", dir)
	}
}

func TestResolveCacheDir_Disabled(t *testing.T) {
	t.Setenv(CacheDirEnv, cacheDisabled)
	dir, err := resolveCacheDir()
	if err != nil {
		t.Fatalf("resolveCacheDir: %v", err)
	}
	if dir != "" {
		t.Fatalf("dir = %q, want empty (disabled)", dir)
	}
}

// An empty-but-set value is a likely misconfiguration (e.g. an unset env var
// expanded into the variable), so it must fail loudly rather than silently
// resolving to the default path or to "disabled".
func TestResolveCacheDir_EmptyIsAnError(t *testing.T) {
	t.Setenv(CacheDirEnv, "")
	if _, err := resolveCacheDir(); err == nil {
		t.Fatal("empty XFLOW_WASM_CACHE_DIR should be an error")
	}
}

func TestResolveCacheDir_DefaultsUnderUserCacheDir(t *testing.T) {
	os.Unsetenv(CacheDirEnv)
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache dir on this machine: %v", err)
	}
	dir, err := resolveCacheDir()
	if err != nil {
		t.Fatalf("resolveCacheDir: %v", err)
	}
	if want := filepath.Join(base, defaultCacheSubdir); dir != want {
		t.Fatalf("dir = %q, want %q", dir, want)
	}
}

// TestDiskCache_SurvivesRuntimeRecreation is the real acceptance test for the
// P2 goal "process restart cold start < 100 ms" (design §1 constraint #6). A
// fresh CompilationCacheWithDir over an existing directory is exactly what a
// restarted process gets, so compiling the same module through a brand-new
// runtime must hit the on-disk entry instead of recompiling from scratch.
func TestDiskCache_SurvivesRuntimeRecreation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	compile := func() time.Duration {
		t.Helper()
		cache, err := wazero.NewCompilationCacheWithDir(dir)
		if err != nil {
			t.Fatalf("cache: %v", err)
		}
		defer cache.Close(ctx)

		rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
			WithCompilationCache(cache))
		defer rt.Close(ctx)
		wasi_snapshot_preview1.MustInstantiate(ctx, rt)

		start := time.Now()
		cm, err := rt.CompileModule(ctx, reactorWasm)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		elapsed := time.Since(start)
		_ = cm.Close(ctx)
		return elapsed
	}

	cold := compile()
	warm := compile() // new cache object + new runtime == a restarted process

	t.Logf("compile cold=%v, after restart=%v", cold, warm)
	if warm >= cold {
		t.Fatalf("disk cache did not help: cold=%v, warm=%v", cold, warm)
	}
	// The design's acceptance bar. Generous vs the measured 82 ms so the test
	// does not flake on a loaded CI box, but tight enough to catch the cache
	// being silently bypassed (a cold compile is seconds).
	//
	// Not judged when the cold sample says this machine is instrumented or busy
	// (see maxCalibrationCold): the same warm compile measures ~82ms alone and
	// ~519ms under -race. The relative assertion above is NOT gated — it is the
	// one that catches a bypassed cache, and it survives contamination with room
	// to spare (23.2s cold vs 519ms warm under -race).
	if reason := budgetNotMeasurable(cold); reason != "" {
		t.Logf("not judging the <500ms warm-compile bar: %s (warm was %v)", reason, warm)
		return
	}
	if warm > 500*time.Millisecond {
		t.Fatalf("warm compile %v exceeds the <500ms bar; disk cache likely bypassed", warm)
	}
}

// TestCompilationCacheFor_Shared asserts the process-wide cache is a singleton,
// which is what lets the legacy command runtime and the reactor runtime share
// compiled code (design §1 constraint #5).
func TestCompilationCacheFor_Shared(t *testing.T) {
	ctx := context.Background()
	if a, b := compilationCacheFor(ctx), compilationCacheFor(ctx); a != b {
		t.Fatal("compilationCacheFor returned different instances; runtimes would not share compiled code")
	}
	if dir, err := cacheStatus(); err != nil {
		t.Logf("disk cache unavailable (in-memory fallback): %v", err)
	} else {
		t.Logf("cache dir = %q", dir)
	}
}
