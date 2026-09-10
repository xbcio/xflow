package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// DefaultFSStoreMaxBytes is the cap newRunnerArtifactResolver
// (sdk/xflow/runner.go) applies when RunnerConfig.ArtifactCacheMaxBytes is
// left at zero.
//
// A cached artifact is content-addressed and at most ~16 MiB (matching
// store.MaxArtifactBytes; not imported here to avoid the store->objectstore
// import cycle — see ErrNotFound's doc comment). A digest is never rewritten
// in place, and a script redeploy always mints a fresh digest, so this
// directory grows exactly the way the sibling wasm compilation cache did
// before it gained a cap (node/internal/code/script/wasm/cache.go,
// defaultCacheMaxBytes): unbounded, until repeated rebuilds on one
// development machine reached 20 GB and filled the host disk, putting the
// podman VM into read-only mode (Kafka/MySQL then unreachable, integration
// tests skipping silently while still exiting 0).
//
// 1 GiB mirrors that cache's budget: room for dozens of full-size artifacts,
// or many more of the smaller scripts actually deployed. An evicted entry
// costs one extra origin fetch, never a failure — GetObject falls through to
// origin on any cache miss (see ReadThrough.GetObject).
const DefaultFSStoreMaxBytes int64 = 1 << 30

// fsStoreStaleTempAge mirrors wasm/cache.go's staleTempAge: how long a
// PutObject temp file (".tmp-<random>", created via os.CreateTemp in the same
// directory as the final path) must sit untouched before the sweep treats it
// as debris from a crashed writer rather than an in-flight one. Deleting a
// fresh one would fail a live PutObject's rename for no gain; an hour-old one
// is crash debris that would otherwise sit outside the budget forever, since
// os.CreateTemp names are never reused.
const fsStoreStaleTempAge = time.Hour

// FSStore implements Store by mapping keys directly to filesystem paths under a
// root directory. Keys produced by store.ObjectKeyForDigest are already two-level
// sharded (artifacts/sha256/ab/cd/<hex>), so no additional hashing is applied.
//
// Metadata is not persisted: the filesystem has no natural place for it without
// sidecar files, and the upstream refFromObject tolerates nil Metadata (Filename
// is purely descriptive). This trade-off means a round-trip through FSStore
// loses the Metadata map, which is acceptable for a cache that only needs to
// serve bytes back by digest.
type FSStore struct {
	root string

	// PartitionByNamespace, when true, prefixes every resolved path with
	// "ns/<namespace>/" where namespace comes from namespace.FromContext(ctx).
	// Default false: FSStore's fundamental contract is content-addressed
	// global dedup (design §4.2) — the SAME digest resolves to the SAME file
	// regardless of which namespace is asking, which is exactly what lets a
	// server-side backing store (and the many tests that use FSStore as a
	// stand-in for one, e.g. service/apiserver/artifact_endpoint_test.go)
	// Put once under one ambient namespace and Get/Stat under another and
	// still find the bytes.
	//
	// The ONE place that invariant is actively harmful is the runner-side
	// local read-through cache (sdk/xflow/runner.go's
	// newRunnerArtifactResolver): there, a cache HIT bypasses the server
	// entirely, so if namespace A's task fetches and caches digest D, a later
	// task in namespace B on the SAME runner would be served D from disk with
	// zero requests to origin — silently defeating the server's per-tenant
	// HasReference check (module_artifact.go) no matter how correct that
	// check is. Only that call site sets this field; every other FSStore
	// construction in this repository leaves it false.
	PartitionByNamespace bool

	// MaxBytes bounds the total size of files this store keeps under root.
	// Zero (the default) leaves it unbounded, which is the correct behaviour
	// for every FSStore construction in this repository except one: the
	// runner's local artifact read-through cache
	// (sdk/xflow/runner.go's newRunnerArtifactResolver), which sets it to
	// DefaultFSStoreMaxBytes (or RunnerConfig.ArtifactCacheMaxBytes). Every
	// other construction — including the server-side backing store the many
	// tests in this repository use FSStore as a stand-in for — is meant to
	// retain everything ever written, so it must keep this at zero. A
	// negative value is ALSO treated as unbounded (enforceBudget's guard is
	// "<= 0", not "== 0"): this gives an operator an explicit way to disable
	// the cap without touching code, distinct from zero, which
	// RunnerConfig.ArtifactCacheMaxBytes already uses to mean "unset, use the
	// package default" — overloading that same zero to also mean "disabled"
	// would make the two unreachable from each other.
	//
	// The budget is enforced by a sweep that runs synchronously at the end of
	// a PutObject call that leaves the store over budget: PutObject is only
	// reached on a cache MISS (ReadThrough writes through after an origin
	// fetch), which already paid a network round trip, so an additional local
	// directory walk adds negligible latency. This differs from the wasm
	// compilation cache's sweep (node/internal/code/script/wasm/cache.go),
	// which runs off-goroutine because that cache is a single process-wide
	// singleton hit by every compile; an FSStore instance here is a
	// per-runner cache hit only on artifact fetch misses, an event orders of
	// magnitude rarer, so the extra determinism of a synchronous sweep is
	// worth the (small, self-bounded — see below) walk cost.
	//
	// Eviction is FIFO by mtime, exactly like the wasm cache: the oldest
	// entries are removed first until the total fits, and there is no
	// distinction between "compiled once and never reused" and "compiled on
	// every startup" because nothing records last access for a plain
	// filesystem read. The cost of evicting the wrong one is a single extra
	// origin fetch, never a failure.
	//
	// The sweep also prunes any shard directory (and, under
	// PartitionByNamespace, any per-namespace directory) an eviction leaves
	// empty — see pruneEmptyDirs. This is not cosmetic: keys are two-level
	// sharded (artifacts/sha256/xx/yy/<hex>), so the last evicted digest in a
	// shard leaves up to four empty directory levels behind, and a workload
	// that cycles through many distinct digests turns that into an inode
	// count with the same unbounded-growth shape MaxBytes exists to close
	// for bytes (bounded in practice by ~65536 possible shard directories,
	// but the per-namespace directories under PartitionByNamespace are not
	// similarly bounded — see the global-budget rationale below). An earlier
	// version of this comment claimed the walk's cost is "naturally bounded
	// by the very budget it enforces" and stopped there, which is true for
	// file BYTES but was never true for directory COUNT: 2000 PutObject
	// calls against a 50-file budget measured 2228 leftover directories,
	// 1921 of them (86%) empty, because the sweep deleted files but never
	// their now-empty parents. Recorded here rather than quietly rewritten,
	// since the mistake was asserting an unmeasured fact, not just missing a
	// case.
	//
	// If MaxBytes is smaller than a single object (up to ~16 MiB, see
	// store.MaxArtifactBytes), PutObject still writes and returns success —
	// but the sweep that write triggers must not evict the file that write
	// just created (sweepFSStore's exempt parameter enforces this).
	// Otherwise PutObject would report success for a key that GetObject
	// immediately fails to find: a capacity limit masquerading as a lie
	// about the store's own contract. The accepted cost is that the
	// directory can exceed MaxBytes by up to one object's worth at any
	// instant — the exemption only protects the object THIS PutObject just
	// wrote; the next PutObject's sweep is free to evict it like anything
	// else once it is no longer the newest thing on disk.
	//
	// No in-memory ledger is kept across Puts or process restarts: each sweep
	// recomputes the total by walking root fresh. That walk's cost is bounded
	// by the very budget it enforces (the tree's file bytes never exceed
	// MaxBytes by more than one object for long — see above), so there is
	// nothing to recover after a restart and no ledger that can go stale.
	//
	// When PartitionByNamespace is also set, the budget applies GLOBALLY
	// across every namespace's partition, not per namespace: the number of
	// namespaces sharing one runner is itself unbounded, so a per-namespace
	// budget would only move the unbounded-growth problem this field exists
	// to close down one level, to "budget × namespace count". The accepted
	// trade-off is that a namespace with heavy digest churn can evict another
	// namespace's warm entries, costing that namespace an extra origin fetch
	// on its next access — a performance cost, never a correctness or
	// isolation break, because resolve() still confines every read and write
	// to its own namespace's subtree regardless of what the sweep evicted
	// elsewhere.
	MaxBytes int64

	// sweeping debounces concurrent sweep attempts on this instance: two
	// PutObject calls racing past the budget check would otherwise both walk
	// the whole tree and race to delete the same entries. A skipped sweep is
	// harmless — os.Remove tolerates a file already gone, and the next
	// PutObject over budget retries.
	sweeping atomic.Bool
}

var _ Store = (*FSStore)(nil)

// NewFSStore creates a store rooted at dir. The directory must already exist;
// subdirectories are created on demand during PutObject.
func NewFSStore(dir string) *FSStore {
	return &FSStore{root: filepath.Clean(dir)}
}

// PutObject writes body to disk atomically using a temp file + rename pattern.
// Concurrent puts to the same key are safe: both rename, last writer wins, both
// produce identical content (content-addressed). If opts.IfNoneMatch is set and
// the file already exists, ErrPreconditionFailed is returned without rewriting.
func (s *FSStore) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (*Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, err
	}

	if opts.IfNoneMatch {
		if _, statErr := os.Stat(path); statErr == nil {
			return nil, ErrPreconditionFailed
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("objectstore/fs: mkdir %s: %w", dir, err)
	}

	// Write to a temp file in the same directory so rename is atomic (same
	// filesystem). Cleanup on any failure path.
	//
	// The ".tmp-" prefix is also what enforceBudget's sweep uses to recognise
	// an in-flight write of ITS OWN and leave it alone (see fsStoreEntries).
	tmp, err := createTempInDir(dir)
	if err != nil {
		return nil, fmt.Errorf("objectstore/fs: create temp: %w", err)
	}
	tmpName := tmp.Name()
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	n, err := io.Copy(tmp, body)
	if err != nil {
		return nil, fmt.Errorf("objectstore/fs: write: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("objectstore/fs: close temp: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("objectstore/fs: size mismatch: wrote %d, expected %d", n, size)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return nil, fmt.Errorf("objectstore/fs: rename: %w", err)
	}
	success = true

	// Enforce the cap, if any, after the write is durable. Synchronous and on
	// this goroutine — see MaxBytes's doc comment for why that is the right
	// call here (unlike the wasm compilation cache's async sweep). path is
	// passed through so the sweep can exempt the file this call just wrote
	// from its own eviction pass — see sweepFSStore's exempt parameter.
	s.enforceBudget(path)

	return &Object{
		Key:          key,
		Size:         n,
		ETag:         digestFromKey(key),
		LastModified: time.Now(),
	}, nil
}

// GetObject opens the file for streaming reads. The caller must close the
// returned ReadCloser. Returns ErrNotFound when the file does not exist.
func (s *FSStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("objectstore/fs: get %s: %w", key, ErrNotFound)
		}
		return nil, nil, fmt.Errorf("objectstore/fs: open: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("objectstore/fs: stat after open: %w", err)
	}

	obj := &Object{
		Key:          key,
		Size:         info.Size(),
		ETag:         digestFromKey(key),
		LastModified: info.ModTime(),
	}
	return f, obj, nil
}

// HeadObject returns metadata (size, mtime) without reading content. Uses
// os.Stat only. Returns ErrNotFound when the file does not exist.
func (s *FSStore) HeadObject(ctx context.Context, key string) (*Object, error) {
	path, err := s.resolve(ctx, key)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("objectstore/fs: head %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("objectstore/fs: stat: %w", err)
	}

	return &Object{
		Key:          key,
		Size:         info.Size(),
		ETag:         digestFromKey(key),
		LastModified: info.ModTime(),
	}, nil
}

// resolve maps key to an absolute path and validates that it stays within the
// permitted subtree. This is the path-traversal defence: even though keys
// from ObjectKeyForDigest are structurally safe, this layer must not assume
// the caller validated.
//
// When s.PartitionByNamespace is set, every path is additionally partitioned
// under root/ns/<namespace>/ so two namespaces sharing a runner never resolve
// to the same file for the same digest — see the field's doc comment for why
// that matters and why it defaults off.
func (s *FSStore) resolve(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("objectstore/fs: empty key")
	}
	base := s.root
	if s.PartitionByNamespace {
		ns := namespace.FromContext(ctx)
		if err := namespace.Validate(ns); err != nil {
			return "", fmt.Errorf("objectstore/fs: invalid namespace %q: %w", ns, err)
		}
		base = filepath.Join(s.root, "ns", string(ns))
	}
	joined := filepath.Join(base, filepath.FromSlash(key))
	cleaned := filepath.Clean(joined)
	// Ensure the resolved path is strictly within the permitted subtree — when
	// partitioned, this is stricter than "within root", so a crafted key
	// cannot traverse into a sibling namespace's cache directory either.
	if !strings.HasPrefix(cleaned, base+string(filepath.Separator)) {
		return "", fmt.Errorf("objectstore/fs: key %q resolves outside root", key)
	}
	return cleaned, nil
}

// digestFromKey recovers the "sha256:<hex>" digest from a key shaped like
// "artifacts/sha256/ab/cd/<hex>". If the key does not match, returns empty.
// Shared with HTTPStore (same package).
func digestFromKey(key string) string {
	// Expected: artifacts/<alg>/<xx>/<yy>/<hex>
	parts := strings.Split(key, "/")
	if len(parts) != 5 {
		return ""
	}
	alg := parts[1]
	hex := parts[4]
	return alg + ":" + hex
}

// enforceBudget is a no-op unless MaxBytes is set, in which case it sweeps
// root down to that budget. justWritten is the absolute path PutObject just
// finished writing (as returned by resolve); it is passed through to
// sweepFSStore so this sweep never evicts the very file that triggered it —
// see MaxBytes's doc comment for why that matters and what it costs.
func (s *FSStore) enforceBudget(justWritten string) {
	if s.MaxBytes <= 0 {
		return
	}
	// Debounce concurrent callers: two PutObject calls racing past the budget
	// would otherwise both walk the whole tree and race to delete the same
	// entries. Skipping is harmless here — the walk this call would have done
	// is redundant with the one already running, and if that in-flight sweep
	// does not happen to account for this call's own new bytes, the next
	// PutObject that lands over budget retries.
	if !s.sweeping.CompareAndSwap(false, true) {
		return
	}
	defer s.sweeping.Store(false)

	freed, remaining, err := sweepFSStore(s.root, s.MaxBytes, justWritten)
	switch {
	case err != nil:
		slog.Warn("objectstore/fs: cache sweep failed; the directory may grow unbounded",
			"root", s.root, "error", err)
	case freed > 0:
		slog.Info("objectstore/fs: evicted oldest cache entries",
			"root", s.root, "freed_bytes", freed, "remaining_bytes", remaining, "budget_bytes", s.MaxBytes)
	}
}

// fsStoreEntry is one on-disk file considered for eviction.
type fsStoreEntry struct {
	path    string
	size    int64
	modTime time.Time
}

// fsStoreEntries walks the ENTIRE tree under root — every namespace
// partition together, when PartitionByNamespace is in use — and returns every
// evictable file plus their total size. Walking everything under one budget,
// rather than per-namespace, is what makes the budget global; see MaxBytes's
// doc comment.
func fsStoreEntries(root string) (out []fsStoreEntry, total int64, err error) {
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				// Vanished mid-walk (a concurrent Put's own temp file, or a
				// concurrent sweep already removed it) — the next sweep can
				// retry, this one just sees less than the true total.
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil // vanished between ReadDir and Stat; same reasoning as above
		}
		if strings.HasPrefix(name, ".tmp-") && time.Since(info.ModTime()) < fsStoreStaleTempAge {
			return nil // another PutObject is probably writing this right now
		}
		out = append(out, fsStoreEntry{path: path, size: info.Size(), modTime: info.ModTime()})
		total += info.Size()
		return nil
	})
	if walkErr != nil {
		if errors.Is(walkErr, os.ErrNotExist) {
			// root itself does not exist yet (nothing has been Put), or was
			// removed out from under us. Either way there is nothing to sweep.
			return nil, 0, nil
		}
		return nil, 0, walkErr
	}
	return out, total, nil
}

// sweepFSStore deletes the oldest files under root, by mtime, until the total
// fits budget. FIFO rather than LRU for the same reason as the wasm
// compilation cache: a plain filesystem read does not update anything this
// package can distinguish from write time without a per-OS syscall for the
// access-time field, and that field is frequently unusable anyway (relatime
// gives it 24h resolution; many container images mount noatime, where it
// never moves). The cost of evicting the wrong entry is one extra origin
// fetch on its next use, never a failure.
//
// exempt, when non-empty, is a path that must never be removed by this call
// even if it sorts as the oldest entry (e.g. tied mtimes) or the ONLY entry
// over budget. PutObject passes the file it just wrote: without this, a
// MaxBytes smaller than a single object would make PutObject's own trigger
// sweep immediately delete the file PutObject just reported as successfully
// written, so the very next GetObject would 404 on a key PutObject swore
// existed. See MaxBytes's doc comment for the cost this accepts.
func sweepFSStore(root string, budget int64, exempt string) (freed, remaining int64, err error) {
	entries, total, err := fsStoreEntries(root)
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
		if e.path == exempt {
			// Never evict the object this sweep's own PutObject just wrote —
			// see the exempt parameter's doc comment above. It remains on
			// disk (and counted in total) even though the store stays over
			// budget as a result; a LATER PutObject's sweep is free to
			// evict it once something newer exists.
			continue
		}
		switch err := os.Remove(e.path); {
		case err == nil:
			freed += e.size
		case errors.Is(err, os.ErrNotExist):
			// A concurrent sweep (this process or, per the wasm cache's
			// precedent, potentially another one sharing the directory) got
			// there first. The space is reclaimed either way, so count it
			// against the total, but do not claim it as freed by this sweep.
		default:
			// Permission denied, or a filesystem that refuses the unlink.
			// Skip it and keep going: aborting the whole sweep over one stuck
			// entry would leave the directory unbounded, which is the
			// failure this exists to prevent.
			continue
		}
		total -= e.size
		// The file is gone (removed just now, or already gone before this
		// call); its parent shard directory (and, under
		// PartitionByNamespace, the per-namespace directory above that) may
		// now be empty. Prune it so the sweep bounds directory count the
		// same way it bounds bytes — see pruneEmptyDirs.
		pruneEmptyDirs(root, filepath.Dir(e.path))
	}
	return freed, total, nil
}

// pruneEmptyDirs removes dir, then each ancestor of dir in turn, for as long
// as each is empty and strictly inside root. Called after evicting a file, to
// reclaim the shard directories (artifacts/sha256/xx/yy, and under
// PartitionByNamespace, ns/<namespace>) that a plain os.Remove of the file
// itself leaves behind. root itself is never removed, and the walk never
// climbs above it: dir is checked against root before every attempt, so a
// caller cannot accidentally prune outside the store's own tree even if the
// path shape ever changes.
//
// Concurrency, the case this function exists to be safe under: os.Remove
// only succeeds on an EMPTY directory. A shard directory another goroutine's
// PutObject is actively using — its MkdirAll has run and its own file
// already lives there, or a sibling ".tmp-*" write is still in progress in
// it — is therefore never removed here; ENOTEMPTY simply stops the climb
// (which is also the correct stopping point: if this level is non-empty,
// every ancestor above it contains that same non-empty content, so there is
// nothing higher up worth trying either).
//
// The one race this reasoning does NOT cover on its own: a concurrent
// PutObject's MkdirAll can succeed (directory now exists and is briefly
// EMPTY, because that PutObject has not yet created its own temp file in
// it) in the instant right before this function's os.Remove runs on that
// same directory. Removing a directory that momentarily looks empty but is
// about to be written into is exactly what os.Remove is FOR (it has no way
// to know the caller intends to use it a moment later), so this is a real
// TOCTOU window, not a hypothetical one. It is closed on the writer's side
// instead: createTempInDir (used by PutObject) recreates a vanished
// directory and retries its CreateTemp exactly once, so a PutObject that
// loses this race recovers rather than failing.
func pruneEmptyDirs(root, dir string) {
	root = filepath.Clean(root)
	for {
		dir = filepath.Clean(dir)
		if dir == root || !strings.HasPrefix(dir, root+string(filepath.Separator)) {
			return
		}
		if err := os.Remove(dir); err != nil {
			return // non-empty, already gone, or permission denied — stop climbing
		}
		dir = filepath.Dir(dir)
	}
}

// createTempInDir creates a temp file in dir (PutObject's write target
// directory), retrying MkdirAll+CreateTemp exactly once if dir has vanished.
//
// The only way dir can vanish between PutObject's own MkdirAll and this call
// is a concurrent eviction sweep's pruneEmptyDirs racing this write and
// winning — see pruneEmptyDirs's doc comment for why that race is real.
// Recreating the directory and retrying once closes that window on this
// side rather than leaving PutObject to fail a write over a directory this
// function is entitled to remake; a second disappearance (which would mean
// something more persistent than a losing race, e.g. the whole root being
// removed out from under the store) is returned as the original error
// rather than retried again.
func createTempInDir(dir string) (*os.File, error) {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return tmp, err
	}
	if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
		return nil, err
	}
	return os.CreateTemp(dir, ".tmp-*")
}
