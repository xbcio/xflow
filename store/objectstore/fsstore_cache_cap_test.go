package objectstore_test

import (
	"bytes"
	"context"
	"fmt"
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
	for i := 0; i < n-1; i++ {
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
	for i := 0; i < n; i++ {
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
	for i := 0; i < n; i++ {
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
