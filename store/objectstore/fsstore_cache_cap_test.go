package objectstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/objectstore"
)

// shardedKey builds a valid two-level-sharded key (matching
// store.ObjectKeyForDigest's output shape) for a fake digest identified by n,
// so each call produces a distinct key without needing a real sha256.
func shardedKey(n int) string {
	hex := fmt.Sprintf("%064x", n)
	return "artifacts/sha256/" + hex[:2] + "/" + hex[2:4] + "/" + hex
}

// TestFSStore_MaxBytesEvictsOldestFirst is the positive control: once the
// cache holds more than MaxBytes, the OLDEST entries -- and only enough of
// them to fit the budget -- are gone.
//
// Ages are forced with os.Chtimes while MaxBytes is still zero (unbounded),
// so no sweep runs during setup and the final trigger Put is the only write
// that can evict anything. That is what makes the survivor set exact and
// deterministic rather than a race against whichever sweep happened to run
// when.
func TestFSStore_MaxBytesEvictsOldestFirst(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()
	const entrySize = 1 << 20 // 1 MiB

	const n = 5
	keys := make([]string, n)
	paths := make([]string, n)
	for i := range n - 1 {
		key := shardedKey(i)
		content := bytes.Repeat([]byte{byte(i)}, entrySize)
		if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
			t.Fatalf("seed PutObject %d: %v", i, err)
		}
		keys[i] = key
		paths[i] = filepath.Join(dir, filepath.FromSlash(key))
		// Strictly increasing ages, oldest (index 0) first. Absolute value
		// does not matter, only relative order.
		when := time.Now().Add(time.Duration(i-n) * time.Hour)
		if err := os.Chtimes(paths[i], when, when); err != nil {
			t.Fatalf("chtimes %d: %v", i, err)
		}
	}

	// Enable the cap only now, sized for exactly two of the five entries
	// that will exist once the trigger Put below lands.
	fs.MaxBytes = 2 * entrySize

	last := n - 1
	key := shardedKey(last)
	content := bytes.Repeat([]byte{byte(last)}, entrySize)
	if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("trigger PutObject: %v", err)
	}
	keys[last] = key
	paths[last] = filepath.Join(dir, filepath.FromSlash(key))

	// Total after the trigger write is 5 * 1 MiB against a 2 MiB budget:
	// the three oldest (indices 0, 1, 2) must be evicted, leaving exactly
	// the two newest (indices 3 and 4, the just-written trigger entry).
	for _, i := range []int{0, 1, 2} {
		if _, err := os.Stat(paths[i]); !os.IsNotExist(err) {
			t.Errorf("entry %d should have been evicted (it is oldest), stat err = %v", i, err)
		}
	}
	for _, i := range []int{3, 4} {
		if _, err := os.Stat(paths[i]); err != nil {
			t.Errorf("entry %d should have survived (it is newest): %v", i, err)
		}
	}

	// Exact count, not ">= 1": a sweep that wiped everything would also
	// satisfy "the oldest are gone".
	survivors := 0
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			survivors++
		}
	}
	if survivors != 2 {
		t.Fatalf("%d entries survived, want exactly 2", survivors)
	}
}

// TestFSStore_MaxBytesUnderBudgetKeepsEverything is the negative control this
// task explicitly requires: with room to spare, PutObject must not delete
// anything. Without this test, an implementation that evicts unconditionally
// (or evicts everything on every Put) would pass the positive test above too.
func TestFSStore_MaxBytesUnderBudgetKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	fs.MaxBytes = 1 << 30 // 1 GiB: far larger than what this test writes
	ctx := context.Background()
	const entrySize = 1 << 20 // 1 MiB

	const n = 4
	paths := make([]string, n)
	for i := range n {
		key := shardedKey(i)
		content := bytes.Repeat([]byte{byte(i)}, entrySize)
		if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
		paths[i] = filepath.Join(dir, filepath.FromSlash(key))
	}

	for i, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("entry %d was deleted despite the store being far under budget: %v", i, err)
		}
	}
}

// TestFSStore_MaxBytesZeroIsUnbounded pins the default: MaxBytes left at its
// zero value (every FSStore construction in this repository except the
// runner's artifact cache) must never evict, no matter how much is written.
// This is the regression guard for every existing FSStore use -- including
// the server-side backing store the many tests in this package use FSStore
// as a stand-in for -- which relies on FSStore retaining everything.
func TestFSStore_MaxBytesZeroIsUnbounded(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir) // MaxBytes left at its zero value
	ctx := context.Background()
	const entrySize = 1 << 20 // 1 MiB

	const n = 8
	paths := make([]string, n)
	for i := range n {
		key := shardedKey(i)
		content := bytes.Repeat([]byte{byte(i)}, entrySize)
		if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
		paths[i] = filepath.Join(dir, filepath.FromSlash(key))
	}

	for i, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("entry %d was deleted with MaxBytes unset (0); it must be unbounded: %v", i, err)
		}
	}
}

// shardedKeyVaried is like shardedKey but spreads n across the shard prefix
// (the first two path segments under "artifacts/sha256/") instead of
// colliding on "00/00" the way shardedKey's zero-padded "%064x" does for
// every n small enough to matter in these tests. Needed by tests that must
// exercise MULTIPLE distinct shard directories rather than many files
// piling into one.
func shardedKeyVaried(n int) string {
	hex := fmt.Sprintf("%04x%060x", n, n)
	return "artifacts/sha256/" + hex[:2] + "/" + hex[2:4] + "/" + hex
}

// countEmptyDirs walks root and counts directories (excluding root itself)
// that contain zero entries.
func countEmptyDirs(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() || path == root {
			return nil
		}
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return readErr
		}
		if len(entries) == 0 {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return count
}

// TestFSStore_MaxBytesPrunesNowEmptyDirectories is the regression test for a
// directory leak this feature had until this commit: sweepFSStore deleted
// evicted FILES but never the shard directories (artifacts/sha256/xx/yy)
// those files leave behind once empty. A workload that cycles through many
// distinct digests -- a different xx/yy shard per digest, since the shard
// prefix is the digest's own leading bytes -- turned that into an inode
// count growing exactly as unboundedly as the byte count MaxBytes exists to
// cap: measured directly, 2000 PutObject calls against a 50-file budget left
// 2228 directories behind, 1921 of them (86%) empty.
//
// 32 entries land in 32 DISTINCT shard directories (shardedKeyVaried, not
// shardedKey), with a 2-entry budget: the trigger write forces 30 of those
// shard directories to go fully empty. The assertion is exact -- zero empty
// directories anywhere under root -- not an upper bound with slack, so an
// implementation that prunes some but not all levels (e.g. only the
// leaf "yy" directory, leaving "artifacts/sha256/xx" behind once its last
// "yy" child is gone) still fails this test.
func TestFSStore_MaxBytesPrunesNowEmptyDirectories(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	ctx := context.Background()
	const entrySize = 1 << 10 // 1 KiB: only directory count matters here, not bytes

	const n = 32
	paths := make([]string, n)
	for i := range n - 1 {
		key := shardedKeyVaried(i)
		content := bytes.Repeat([]byte{byte(i)}, entrySize)
		if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
			t.Fatalf("seed PutObject %d: %v", i, err)
		}
		paths[i] = filepath.Join(dir, filepath.FromSlash(key))
		when := time.Now().Add(time.Duration(i-n) * time.Hour)
		if err := os.Chtimes(paths[i], when, when); err != nil {
			t.Fatalf("chtimes %d: %v", i, err)
		}
	}

	// Enable the cap only now, sized for exactly two of the 32 entries that
	// will exist once the trigger Put below lands -- mirrors
	// TestFSStore_MaxBytesEvictsOldestFirst's technique for a deterministic
	// survivor set.
	fs.MaxBytes = 2 * entrySize

	last := n - 1
	key := shardedKeyVaried(last)
	content := bytes.Repeat([]byte{byte(last)}, entrySize)
	if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("trigger PutObject: %v", err)
	}
	paths[last] = filepath.Join(dir, filepath.FromSlash(key))

	survivors := 0
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			survivors++
		}
	}
	if survivors != 2 {
		t.Fatalf("%d entries survived, want exactly 2 (sanity check before asserting on directories)", survivors)
	}

	if got := countEmptyDirs(t, dir); got != 0 {
		t.Fatalf("%d empty directories left under %s after eviction, want exactly 0 -- "+
			"sweepFSStore deleted files but did not prune their now-empty shard directories", got, dir)
	}
}

// TestFSStore_MaxBytesSmallerThanOneObjectStillServesIt is the regression
// test for PutObject lying about its own contract: measured directly, an
// 8 MiB object against a 1 MiB MaxBytes made PutObject return success and
// then made the very next GetObject fail with ErrNotFound, because the
// sweep PutObject triggers on its own write evicted the file that write had
// just reported as durably written (it was the only entry, and the only
// entry over budget is also the oldest entry).
//
// The budget here (half the object's size) can never be satisfied by
// keeping the object, so this pins the accepted trade-off documented on
// MaxBytes: the object is exempt from the sweep ITS OWN write triggers, so
// it stays retrievable, and the store is allowed to sit over budget by that
// one object's worth rather than silently fail to cache it.
func TestFSStore_MaxBytesSmallerThanOneObjectStillServesIt(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	const objectSize = 8 << 20 // 8 MiB
	fs.MaxBytes = objectSize / 2
	ctx := context.Background()

	key := shardedKey(0)
	content := bytes.Repeat([]byte{0xAB}, objectSize)
	if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), objectSize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	rc, obj, err := fs.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject immediately after PutObject: %v -- MaxBytes (%d) being smaller than the "+
			"object (%d) must not make PutObject's own sweep evict the object it just wrote",
			err, fs.MaxBytes, objectSize)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(content))
	}
	if obj.Size != objectSize {
		t.Fatalf("Object.Size = %d, want %d", obj.Size, objectSize)
	}
}

// TestFSStore_MaxBytesNegativeIsUnbounded pins the other half of
// enforceBudget's "<= 0" guard: a negative MaxBytes (RunnerConfig's explicit
// "disabled" value, distinct from zero's "use the package default" -- see
// artifactCacheMaxBytes in sdk/xflow/runner.go) must behave exactly like the
// zero default and never evict. This is a DIFFERENT mutation target than
// TestFSStore_MaxBytesZeroIsUnbounded: a guard narrowed from "<= 0" to
// "== 0" would still pass the zero test but would treat a negative MaxBytes
// as a (nonsensical, and in Go, always-true "total <= budget" is false for
// any positive total) budget of that magnitude, evicting everything on
// every write.
func TestFSStore_MaxBytesNegativeIsUnbounded(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	fs.MaxBytes = -1
	ctx := context.Background()
	const entrySize = 1 << 20 // 1 MiB

	const n = 8
	paths := make([]string, n)
	for i := range n {
		key := shardedKey(i)
		content := bytes.Repeat([]byte{byte(i)}, entrySize)
		if _, err := fs.PutObject(ctx, key, bytes.NewReader(content), entrySize, objectstore.PutOptions{}); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
		paths[i] = filepath.Join(dir, filepath.FromSlash(key))
	}

	for i, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("entry %d was deleted with MaxBytes = -1; a negative value must be unbounded, "+
				"exactly like the zero default: %v", i, err)
		}
	}
}

// TestFSStore_MaxBytesIsGlobalAcrossNamespacePartitions pins the design
// decision recorded on MaxBytes's doc comment: when PartitionByNamespace is
// also set, the budget is enforced across ALL namespace partitions together,
// not once per namespace. A per-namespace budget would let the number of
// namespaces sharing one runner multiply the disk usage this cap exists to
// bound, recreating the exact unbounded-growth failure by another route.
func TestFSStore_MaxBytesIsGlobalAcrossNamespacePartitions(t *testing.T) {
	dir := t.TempDir()
	fs := objectstore.NewFSStore(dir)
	fs.PartitionByNamespace = true
	const entrySize = 1 << 20 // 1 MiB

	// tenant A alone fills exactly the whole budget.
	fs.MaxBytes = 2 * entrySize
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("tenant-a"))
	ctxB := namespace.WithNamespace(context.Background(), namespace.Namespace("tenant-b"))

	keyA0, keyA1 := shardedKey(0), shardedKey(1)
	if _, err := fs.PutObject(ctxA, keyA0, bytes.NewReader(bytes.Repeat([]byte{0}, entrySize)), entrySize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject tenant A entry 0: %v", err)
	}
	if _, err := fs.PutObject(ctxA, keyA1, bytes.NewReader(bytes.Repeat([]byte{1}, entrySize)), entrySize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject tenant A entry 1: %v", err)
	}
	pathA0 := filepath.Join(dir, "ns", "tenant-a", filepath.FromSlash(keyA0))

	// tenant B's own write pushes the GLOBAL total (tenant A's 2 MiB + this
	// 1 MiB) over the 2 MiB budget. If the budget were per-namespace this
	// would be a no-op (tenant B's own partition is still under budget); a
	// global budget must instead reach into tenant A's partition and evict
	// its oldest entry.
	keyB0 := shardedKey(0)
	if _, err := fs.PutObject(ctxB, keyB0, bytes.NewReader(bytes.Repeat([]byte{2}, entrySize)), entrySize, objectstore.PutOptions{}); err != nil {
		t.Fatalf("PutObject tenant B entry: %v", err)
	}

	if _, err := os.Stat(pathA0); !os.IsNotExist(err) {
		t.Fatalf("tenant A's oldest entry should have been evicted by tenant B's write under a GLOBAL "+
			"budget, stat err = %v", err)
	}
}
