package wasm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tetratelabs/wazero"
)

// CacheDirEnv overrides where the wasm compilation cache is persisted. Set it
// to "off" to disable the on-disk cache entirely (in-memory only).
const CacheDirEnv = "XFLOW_WASM_CACHE_DIR"

// cacheDisabled is the CacheDirEnv value that turns persistence off.
const cacheDisabled = "off"

// defaultCacheSubdir is appended to os.UserCacheDir when CacheDirEnv is unset.
const defaultCacheSubdir = "xflow/wasm"

// compilationCache is the process-wide wazero CompilationCache shared by every
// wasm runtime in this package (the legacy command engine and the reactor host).
//
// Sharing is both safe and the point: a CompilationCache is explicitly reusable
// across runtimes, and doing so cuts a second runtime's compile of the same
// module from seconds to tens of milliseconds (design §1 constraint #5). Backing
// it with a directory extends that across process restarts (constraint #6:
// 3.18 s cold → 82 ms after restart), which is what makes a runner redeploy
// cheap instead of paying a multi-second compile on the first request.
//
// wazero namespaces the directory by its own version and GOOS/GOARCH, so a
// wazero upgrade invalidates stale entries automatically rather than loading
// incompatible machine code.
//
// KNOWN LIMITATION: wazero's file cache takes no inter-process lock. Two
// processes sharing one directory may both write the same entry; entries are
// written atomically per file, so the outcome is a redundant write rather than
// corruption. Point separate deployments at separate directories via
// CacheDirEnv if you want to avoid the churn entirely.
var (
	// cacheOnce is a pointer so tests can substitute a fresh Once (and restore
	// the original) without copying a lock value.
	cacheOnce = new(sync.Once)
	cacheVal  wazero.CompilationCache
	cacheDir  string // resolved directory; empty when in-memory only
	cacheErr  error  // why the disk cache was not used, for diagnostics
)

// compilationCacheFor returns the shared cache, creating it on first use. It
// fails open: if the directory cannot be resolved or created, an in-memory
// cache is returned instead so wasm execution still works (it just pays the
// full compile again after a restart). The reason is retained in cacheErr and
// surfaced by cacheStatus for logging.
func compilationCacheFor(ctx context.Context) wazero.CompilationCache {
	cacheOnce.Do(func() {
		dir, err := resolveCacheDir()
		if err != nil {
			cacheErr = err
			cacheVal = wazero.NewCompilationCache()
			return
		}
		if dir == "" {
			// Explicitly disabled.
			cacheVal = wazero.NewCompilationCache()
			return
		}
		c, err := wazero.NewCompilationCacheWithDir(dir)
		if err != nil {
			cacheErr = fmt.Errorf("wasm: compilation cache dir %q unusable: %w", dir, err)
			cacheVal = wazero.NewCompilationCache()
			return
		}
		cacheVal, cacheDir = c, dir
	})
	_ = ctx // reserved: wazero's cache constructor takes no context today
	return cacheVal
}

// resolveCacheDir picks the on-disk cache location. An empty return with a nil
// error means the caller asked for the cache to be disabled.
func resolveCacheDir() (string, error) {
	if v, ok := os.LookupEnv(CacheDirEnv); ok {
		if v == cacheDisabled {
			return "", nil
		}
		if v == "" {
			return "", fmt.Errorf("wasm: %s is set but empty; use %q to disable", CacheDirEnv, cacheDisabled)
		}
		return v, nil
	}
	base, err := os.UserCacheDir()
	if err != nil {
		// No HOME/XDG_CACHE_HOME (common in minimal containers). Fall back to
		// in-memory rather than guessing a writable path.
		return "", fmt.Errorf("wasm: no user cache dir (set %s): %w", CacheDirEnv, err)
	}
	return filepath.Join(base, defaultCacheSubdir), nil
}

// cacheStatus reports the effective cache configuration for startup logging:
// the resolved directory ("" when in-memory only) and why, if the disk cache
// was requested but unavailable.
func cacheStatus() (dir string, err error) {
	return cacheDir, cacheErr
}
