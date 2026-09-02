package wasm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
)

// CacheDirEnv overrides where the wasm compilation cache is persisted. Set it
// to "off" to disable the on-disk cache entirely (in-memory only).
const CacheDirEnv = "XFLOW_WASM_CACHE_DIR"

// cacheDisabled is the CacheDirEnv value that turns persistence off.
const cacheDisabled = "off"

// defaultCacheSubdir is appended to os.UserCacheDir when CacheDirEnv is unset.
const defaultCacheSubdir = "xflow/wasm"

// CacheMaxBytesEnv caps the on-disk compilation cache. Set it to 0 to leave the
// directory unbounded (the behaviour before this cap existed).
const CacheMaxBytesEnv = "XFLOW_WASM_CACHE_MAX_BYTES"

// defaultCacheMaxBytes bounds the cache directory.
//
// Nothing in wazero evicts: filecache.Add writes one file per compiled module
// and the only Delete is the one wazero itself never calls, so a directory that
// sees repeated guest rebuilds grows without limit. Measured on a development
// machine, ~30 MB per rebuild of a production decode guest took this directory to
// 20 GB. That is not merely wasted space — filling the host disk puts the podman
// VM into read-only mode, at which point Kafka and MySQL become unreachable and
// integration tests SKIP SILENTLY while still exiting 0.
//
// 1 GiB holds roughly 30 generations of a two-guest deployment, which is far
// more history than any rollback needs. An evicted entry costs one recompile
// (~2-3 s cold), never a failure.
const defaultCacheMaxBytes int64 = 1 << 30

// cacheVersionDirPrefix is how wazero names the per-version subdirectory it
// creates under the configured cache dir ("wazero-<version>-<arch>-<os>"). It is
// not exported by wazero, so a rename upstream would make the sweep silently
// find nothing; TestSweepCache_MatchesWazeroLayout builds a real on-disk cache
// and fails if this prefix stops matching.
const cacheVersionDirPrefix = "wazero-"

// staleTempAge is how long a wazero temp file (filecache.Add's "<key>.*.tmp")
// must sit untouched before the sweep treats it as debris from a crashed
// process rather than an in-flight write by a live one. Deleting a live one
// would only fail that process's cache write, but there is no reason to.
const staleTempAge = time.Hour

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
//
// wazero also never evicts — see defaultCacheMaxBytes for what that cost in
// practice and for the bound this package applies on top.
var (
	// cacheOnce is a pointer so tests can substitute a fresh Once (and restore
	// the original) without copying a lock value.
	cacheOnce = new(sync.Once)
	cacheVal  wazero.CompilationCache
	cacheDir  string // resolved directory; empty when in-memory only
	cacheErr  error  // why the disk cache was not used, for diagnostics

	// sweeping guards against piling up concurrent sweeps: a burst of compile
	// misses would otherwise each walk the same directory and race to delete the
	// same entries. A dropped sweep is harmless — the next miss triggers another.
	sweeping atomic.Bool

	// sweepDir mirrors cacheDir for sweepCacheAsync.
	//
	// cacheDir is written inside cacheOnce and is only ordered against a reader
	// that went through compilationCacheFor itself. The compile path does not:
	// it reaches the cache through reactorHost.runtime's OWN sync.Once, so a
	// goroutine that loses that race returns without ever touching cacheOnce.
	// The ordering does hold today, transitively, through two Onces in two
	// files — which is exactly the kind of guarantee that stops holding when
	// someone adds a third call site. An atomic costs nothing here and does not
	// depend on the argument.
	sweepDir atomic.Pointer[string]
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
		sweepDir.Store(&dir)
		// Sweep at startup as well as after each compile miss. A process that
		// restarts often enough to never reach a miss still inherits whatever
		// the previous ones left, and that inheritance is exactly how this
		// directory reached 20 GB.
		sweepCacheAsync()
	})
	_ = ctx // reserved: wazero's cache constructor takes no context today
	return cacheVal
}

// cacheBudget reads the configured cap. A zero or negative value, and any
// unparseable one, means unbounded: refusing to start over a malformed cache
// size would take the runner down for a tuning knob.
func cacheBudget() int64 {
	v, ok := os.LookupEnv(CacheMaxBytesEnv)
	if !ok {
		return defaultCacheMaxBytes
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		slog.Warn("wasm: ignoring malformed cache size, leaving the cache unbounded",
			"env", CacheMaxBytesEnv, "value", v, "error", err)
		return 0
	}
	return n
}

// sweepCacheAsync bounds the cache directory off the hot path. It is called
// after a compile miss — the one event that adds an entry — and at startup.
//
// Off the caller's goroutine because the sweep walks the whole directory, and
// the caller is either mid-compile or mid-startup; neither should wait on a
// stat loop to reclaim space that is not urgent by the millisecond.
func sweepCacheAsync() {
	p := sweepDir.Load()
	if p == nil || *p == "" {
		return
	}
	dir, budget := *p, cacheBudget()
	if budget <= 0 {
		return
	}
	if !sweeping.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer sweeping.Store(false)
		freed, remaining, err := sweepCache(dir, budget)
		switch {
		case err != nil:
			slog.Warn("wasm: compilation cache sweep failed; the directory may grow unbounded",
				"dir", dir, "error", err)
		case freed > 0:
			slog.Info("wasm: evicted oldest compilation cache entries",
				"dir", dir, "freed_bytes", freed, "remaining_bytes", remaining, "budget_bytes", budget)
		}
	}()
}

// sweepCache deletes the oldest entries under dir until the total fits budget.
// It deliberately preserves empty wazero version directories: a CompilationCache
// creates its version directory once, then assumes it remains available for
// later cache writes. Removing that directory concurrently with a compile makes
// wazero's CreateTemp fail instead of merely turning the cache write into a miss.
//
// Oldest by MODIFICATION time, which for this cache is insertion time: wazero's
// Get only opens the file, so nothing records a last use. This is FIFO, not
// LRU, and it can evict a module that is compiled on every startup while
// keeping one compiled once and never used again. Reading access time instead
// would need a per-OS build tag for the syscall.Stat_t field name and would
// still be worthless where it matters: Linux mounts default to relatime, giving
// a 24-hour resolution, and container images are commonly mounted noatime,
// where it never moves at all. The cost of the wrong choice is one recompile.
func sweepCache(dir string, budget int64) (freed, remaining int64, err error) {
	entries, total, err := cacheEntries(dir)
	if err != nil {
		return 0, 0, err
	}
	if total <= budget {
		return 0, total, nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].modTime.Before(entries[j].modTime) })

	for _, e := range entries {
		if total <= budget {
			break
		}
		switch err := os.Remove(e.path); {
		case err == nil:
			freed += e.size
		case errors.Is(err, os.ErrNotExist):
			// A concurrent sweep in another process got there first. The space
			// is reclaimed either way, so count it against the total, but do not
			// claim it as freed by this sweep.
		default:
			// Permission denied, or a filesystem that refuses the unlink. Skip
			// it and keep going: aborting the sweep over one stuck entry would
			// leave the directory unbounded, which is the failure being fixed.
			continue
		}
		total -= e.size
	}
	return freed, total, nil
}

// cacheEntry is one compiled-module file considered for eviction.
type cacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

// cacheEntries lists every evictable file across ALL of wazero's version
// directories, and their total size.
//
// All of them deliberately, against one shared budget. A wazero upgrade starts
// a new version directory and abandons the old one permanently — nothing will
// ever read it again. Sweeping the versions together means those abandoned
// entries, being the oldest, are evicted first without this package having to
// know which version is current.
func cacheEntries(dir string) (out []cacheEntry, total int64, err error) {
	versions, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	for _, v := range versions {
		if !v.IsDir() || !strings.HasPrefix(v.Name(), cacheVersionDirPrefix) {
			continue
		}
		vdir := filepath.Join(dir, v.Name())
		files, err := os.ReadDir(vdir)
		if err != nil {
			continue // vanished or unreadable; the next sweep can retry
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			info, err := f.Info()
			if err != nil {
				continue
			}
			if strings.HasSuffix(f.Name(), ".tmp") && time.Since(info.ModTime()) < staleTempAge {
				continue // another process is probably writing it right now
			}
			out = append(out, cacheEntry{
				path:    filepath.Join(vdir, f.Name()),
				size:    info.Size(),
				modTime: info.ModTime(),
			})
			total += info.Size()
		}
	}
	return out, total, nil
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
