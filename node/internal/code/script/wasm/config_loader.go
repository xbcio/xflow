package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"
)

// ConfigLoader is the external configuration source for a wasm reactor module.
// A background watcher polls Load at a configured TTL; when the version changes
// the host builds a new instance pool and atomically swaps it in (B-plan §6.3).
//
// Implementations must be safe for concurrent use: the watcher goroutine calls
// Load periodically while Execute may read the active pool concurrently.
type ConfigLoader interface {
	// Load returns the current config bytes and a version identifier (etag,
	// updated_at, content hash, etc.). The watcher compares version strings to
	// detect change — only when version differs from the last successfully
	// applied version does it attempt a pool rebuild.
	Load(ctx context.Context) (cfg []byte, version string, err error)
}

// StaticLoader returns a ConfigLoader that always yields the same config.
// Its version is the sha256 of cfg, so it never triggers a pool swap after the
// initial warm-up.
func StaticLoader(cfg []byte) ConfigLoader {
	sum := sha256.Sum256(cfg)
	return &staticLoader{cfg: cfg, version: hex.EncodeToString(sum[:])}
}

type staticLoader struct {
	cfg     []byte
	version string
}

func (s *staticLoader) Load(_ context.Context) ([]byte, string, error) {
	return s.cfg, s.version, nil
}

// loaderEntry is the registration record for one module's config loader. It
// retains the code string because warm-up resolves the engine through
// engineForCode, which needs the encoding rather than the key.
type loaderEntry struct {
	code   string
	loader ConfigLoader
	ttl    time.Duration
}

// loaderRegistry holds all registered config loaders, keyed by module identity
// (moduleKey) rather than by the base64 code string — see moduleKey for why a
// code string is not an identity. It is read on every Execute to pick the config
// path, so the lock is an RWMutex: registration happens at bootstrap, reads
// happen per request.
var (
	loaderMu       sync.RWMutex
	loaderRegistry = map[string]loaderEntry{}
)

// RegisterConfigLoader associates a ConfigLoader with a wasm module.
// During warmup any already-registered loader is invoked once to build the
// initial pool; if ttl > 0 a background goroutine polls for version changes.
//
// Re-registering the same module replaces the previous loader (aligns with
// addPrewarm semantics). It may be called at any time, including after warmup —
// registration flips the module's engine to source-driven config so Execute
// needs no lock; when the engine does not exist yet, engineForCode resolves the
// flag from the registry at creation time.
//
// An undecodable code is still registered (under the raw string, see
// registryKeyOrRaw): this function cannot report an error, so warm-up surfaces
// the decode failure when it tries to build the module.
func RegisterConfigLoader(code string, loader ConfigLoader, ttl time.Duration) {
	loaderMu.Lock()
	loaderRegistry[registryKeyOrRaw(code)] = loaderEntry{code: code, loader: loader, ttl: ttl}
	loaderMu.Unlock()

	// Flip the already-created engine, if any. A miss is fine: engineForCode
	// resolves the flag when it creates the engine.
	sharedReactorHost.markConfigFromSource(code)
}

// hasLoaderKey reports whether a module's config comes from a loader rather than
// from globals. It is a locked read, so it must run only at registration and
// warmup time — NOT on the Execute hot path. reactorEngine.configFromSource is
// the lock-free flag Execute actually reads; hasLoaderKey exists only to seed it.
//
// It takes a moduleKey, not a code string, so that all four registries agree on
// what identifies a module.
func hasLoaderKey(key string) bool {
	loaderMu.RLock()
	defer loaderMu.RUnlock()
	_, ok := loaderRegistry[key]
	return ok
}

// registeredLoaders snapshots all registered loaders for warmup iteration.
func registeredLoaders() map[string]loaderEntry {
	loaderMu.RLock()
	defer loaderMu.RUnlock()
	out := make(map[string]loaderEntry, len(loaderRegistry))
	for k, v := range loaderRegistry {
		out[k] = v
	}
	return out
}

// startWatcher launches the background polling goroutine for a module with a
// registered loader. It polls Load every ttl; when version changes it triggers
// a pool swap via the existing swapConfig path. The goroutine exits when ctx is
// cancelled (runner shutdown).
//
// version tracking: lastAppliedVersion is updated ONLY on successful swap. If
// Load returns a new version but buildPool fails (bad config), the version is
// NOT recorded — so the next poll retries the same version once the source is
// fixed.
func startWatcher(ctx context.Context, code string, e *reactorEngine, loader ConfigLoader, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lastAppliedVersion, _ := e.lastAppliedVersion.Load().(string)

				cfg, version, err := loader.Load(ctx)
				if err != nil {
					slog.Warn("wasm config loader: source unreachable",
						"module", code[:min(len(code), 32)],
						"error", err)
					continue // preserve last-good pool (§6.5)
				}
				if version == lastAppliedVersion {
					continue // zero-cost: version unchanged, don't touch pool
				}
				// Version changed — attempt pool swap. The loader path has no
				// SupplyResource revision, so it passes 0 (legacy globals path).
				if err := e.swapConfig(ctx, cfg, defaultPoolSize(), 0); err != nil {
					slog.Warn("wasm config loader: new config rejected",
						"module", code[:min(len(code), 32)],
						"version", version,
						"error", err)
					// Do NOT update lastAppliedVersion — next poll retries this
					// version once the bad config is fixed.
					continue
				}
				e.lastAppliedVersion.Store(version)
			}
		}
	}()
}
