package wasm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
)

// writeEntry creates one fake cache entry aged by age.
func writeEntry(t *testing.T, dir, name string, size int, age time.Duration) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
	return path
}

// TestSweepCache_MatchesWazeroLayout is the load-bearing test in this file, and
// the only one that talks to wazero at all.
//
// Every other test here builds the directory layout itself, so all of them would
// keep passing if wazero renamed its version directory or stopped using one:
// cacheEntries would find zero files, sweepCache would report nothing to do, and
// the cache would go back to growing without limit — reported as a clean, quiet
// success. That is the failure this test exists to make loud. It compiles a real
// module through a real on-disk CompilationCache and then asserts that the sweep
// can SEE what wazero wrote.
func TestSweepCache_MatchesWazeroLayout(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	cache, err := wazero.NewCompilationCacheWithDir(dir)
	if err != nil {
		t.Fatalf("cache: %v", err)
	}
	defer cache.Close(ctx)

	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCompilationCache(cache))
	defer rt.Close(ctx)
	cm, err := rt.CompileModule(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_ = cm.Close(ctx)

	entries, total, err := cacheEntries(dir)
	if err != nil {
		t.Fatalf("cacheEntries: %v", err)
	}
	if len(entries) == 0 || total == 0 {
		// Show what is actually on disk: if wazero changed its layout, the
		// listing names the new one.
		var found []string
		_ = filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error {
			rel, _ := filepath.Rel(dir, p)
			found = append(found, rel)
			return nil
		})
		t.Fatalf("the sweep sees nothing after wazero wrote a real cache entry "+
			"(entries=%d, total=%d B). cacheVersionDirPrefix=%q no longer matches "+
			"wazero's layout, so eviction has silently stopped working.\non disk: %v",
			len(entries), total, cacheVersionDirPrefix, found)
	}
	t.Logf("wazero wrote %d entr(ies), %.1f MiB, under %s",
		len(entries), float64(total)/(1<<20), filepath.Dir(entries[0].path))
}

// TestSweepCache_EvictsOldestFirstDownToBudget checks both halves of the
// contract: enough is deleted, and no more than enough.
func TestSweepCache_EvictsOldestFirstDownToBudget(t *testing.T) {
	dir := t.TempDir()
	vdir := filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin")

	// 4 x 1 MiB, oldest first. A 2 MiB budget must remove exactly the two
	// oldest.
	oldest := writeEntry(t, vdir, "aaaa", 1<<20, 4*time.Hour)
	older := writeEntry(t, vdir, "bbbb", 1<<20, 3*time.Hour)
	newer := writeEntry(t, vdir, "cccc", 1<<20, 2*time.Hour)
	newest := writeEntry(t, vdir, "dddd", 1<<20, 1*time.Hour)

	freed, remaining, err := sweepCache(dir, 2<<20)
	if err != nil {
		t.Fatalf("sweepCache: %v", err)
	}
	if freed != 2<<20 || remaining != 2<<20 {
		t.Fatalf("freed=%d remaining=%d, want freed=%d remaining=%d",
			freed, remaining, 2<<20, 2<<20)
	}
	for _, gone := range []string{oldest, older} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Errorf("%s should have been evicted (it is older)", filepath.Base(gone))
		}
	}
	// The other half of the assertion. A sweep that deleted everything would
	// satisfy "under budget" too, and would throw away a warm cache on every
	// miss.
	for _, kept := range []string{newer, newest} {
		if _, err := os.Stat(kept); err != nil {
			t.Errorf("%s should have been kept (it is newer): %v", filepath.Base(kept), err)
		}
	}
}

// TestSweepCache_UnderBudgetDeletesNothing guards the common case: almost every
// call happens with room to spare, and must not touch the disk.
func TestSweepCache_UnderBudgetDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	vdir := filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin")
	kept := writeEntry(t, vdir, "aaaa", 1<<20, time.Hour)

	freed, remaining, err := sweepCache(dir, 1<<30)
	if err != nil {
		t.Fatalf("sweepCache: %v", err)
	}
	if freed != 0 {
		t.Fatalf("freed = %d, want 0 when already under budget", freed)
	}
	if remaining != 1<<20 {
		t.Fatalf("remaining = %d, want %d", remaining, 1<<20)
	}
	if _, err := os.Stat(kept); err != nil {
		t.Fatalf("entry was deleted despite being under budget: %v", err)
	}
}

// TestSweepCache_AbandonedVersionDirGoesFirst is why cacheEntries pools every
// version directory into one budget rather than sweeping the current one alone.
//
// After a wazero upgrade the old directory is dead weight that nothing will
// ever read again. Its entries are also, by construction, the oldest ones — so
// FIFO eviction clears them before touching anything live, without this package
// needing to know which version is current.
func TestSweepCache_AbandonedVersionDirGoesFirst(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, cacheVersionDirPrefix+"1.8.0-arm64-darwin")
	cur := filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin")

	writeEntry(t, old, "aaaa", 1<<20, 30*24*time.Hour)
	writeEntry(t, old, "bbbb", 1<<20, 30*24*time.Hour)
	live := writeEntry(t, cur, "cccc", 1<<20, time.Minute)

	if _, _, err := sweepCache(dir, 1<<20); err != nil {
		t.Fatalf("sweepCache: %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("the live entry was evicted before the abandoned version's: %v", err)
	}
	// The emptied version directory must go too, or an upgraded deployment
	// accumulates one dead directory per wazero release forever.
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("emptied version dir %s was not pruned", filepath.Base(old))
	}
	if _, err := os.Stat(cur); err != nil {
		t.Fatalf("the current version dir was pruned while still holding an entry: %v", err)
	}
}

// TestSweepCache_SkipsInFlightTempFiles: wazero writes "<key>.*.tmp" and renames
// it into place. Deleting a fresh one would fail a live process's cache write
// for no gain, while an hour-old one is debris from a crash and must be
// reclaimable — otherwise a crash loop leaks past the budget with nothing able
// to clean it up.
func TestSweepCache_SkipsInFlightTempFiles(t *testing.T) {
	dir := t.TempDir()
	vdir := filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin")
	inFlight := writeEntry(t, vdir, "aaaa.12345.tmp", 4<<20, time.Second)
	stale := writeEntry(t, vdir, "bbbb.67890.tmp", 4<<20, 2*staleTempAge)

	if _, _, err := sweepCache(dir, 0+1); err != nil { // budget of 1 byte: evict all it may
		t.Fatalf("sweepCache: %v", err)
	}
	if _, err := os.Stat(inFlight); err != nil {
		t.Errorf("an in-flight temp file was deleted out from under a live writer: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("a %v-old temp file was not reclaimed; crash debris would accumulate forever", 2*staleTempAge)
	}
}

// TestSweepCache_IgnoresForeignDirs: the configured directory may be shared with
// something else (CacheDirEnv points wherever an operator says). Only wazero's
// own version directories are ours to delete from.
func TestSweepCache_IgnoresForeignDirs(t *testing.T) {
	dir := t.TempDir()
	foreign := writeEntry(t, filepath.Join(dir, "someone-elses-data"), "important", 8<<20, 48*time.Hour)
	writeEntry(t, filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin"), "aaaa", 1<<20, time.Hour)

	if _, remaining, err := sweepCache(dir, 1); err != nil {
		t.Fatalf("sweepCache: %v", err)
	} else if remaining != 0 {
		t.Fatalf("remaining = %d, want 0 (only wazero's own entries are counted)", remaining)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("the sweep deleted a file it did not put there: %v", err)
	}
}

func TestCacheBudget(t *testing.T) {
	tests := []struct {
		name  string
		set   bool
		value string
		want  int64
	}{
		{name: "unset uses the default", set: false, want: defaultCacheMaxBytes},
		{name: "explicit value", set: true, value: "1048576", want: 1 << 20},
		{name: "surrounding space tolerated", set: true, value: "  1048576 ", want: 1 << 20},
		// Unbounded rather than a startup failure: a malformed tuning knob must
		// not take a runner down.
		{name: "zero disables the bound", set: true, value: "0", want: 0},
		{name: "garbage disables the bound", set: true, value: "1GB", want: 0},
		{name: "negative disables the bound", set: true, value: "-1", want: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(CacheMaxBytesEnv, tc.value)
			} else {
				os.Unsetenv(CacheMaxBytesEnv)
			}
			if got := cacheBudget(); got != tc.want {
				t.Fatalf("cacheBudget() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestSweepCacheAsync_NoDirIsANoop: with the cache in-memory only (CacheDirEnv
// "off", or no writable HOME) there is no directory to sweep, and the sweep must
// not walk the process's working directory by accident.
func TestSweepCacheAsync_NoDirIsANoop(t *testing.T) {
	prev := sweepDir.Load()
	sweepDir.Store(nil)
	t.Cleanup(func() { sweepDir.Store(prev) })

	sweepCacheAsync()
	if sweeping.Load() {
		t.Fatal("sweepCacheAsync started a sweep with no cache directory configured")
	}
}

// TestSweepCache_MissingDirIsNotAnError: the directory is created lazily by
// wazero, so a sweep can legitimately race ahead of it.
func TestSweepCache_MissingDirIsNotAnError(t *testing.T) {
	freed, remaining, err := sweepCache(filepath.Join(t.TempDir(), "not-created-yet"), 1<<20)
	if err != nil {
		t.Fatalf("sweepCache on a missing dir: %v", err)
	}
	if freed != 0 || remaining != 0 {
		t.Fatalf("freed=%d remaining=%d, want 0/0", freed, remaining)
	}
}

// TestCompileMiss_TriggersSweep is the WIRING test, and the reason none of the
// tests above can stand in for it.
//
// Every other test in this file calls sweepCache directly. All of them would
// keep passing if the call in engineForKey were deleted, or had never been
// added — a correct evictor that nothing invokes, and a cache that grows to
// 20 GB again while the suite reports green. So this one goes through the real
// compile path: a genuine reactorHost, a genuine module, the process-wide cache
// singleton pointed at a temp dir, and no direct call to any sweep function.
func TestCompileMiss_TriggersSweep(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(CacheDirEnv, dir)
	// One entry of the guest is ~31 MiB. A 40 MiB budget leaves room for the
	// real one this test is about to compile while forcing the 64 MiB of
	// pre-existing debris out.
	t.Setenv(CacheMaxBytesEnv, "41943040")
	resetCacheForTest(t)

	vdir := filepath.Join(dir, cacheVersionDirPrefix+"v1.9.0-arm64-darwin")
	var debris []string
	for i := range 8 {
		debris = append(debris, writeEntry(t, vdir, fmt.Sprintf("dead%060x", i), 8<<20,
			time.Duration(30-i)*24*time.Hour))
	}

	ctx := context.Background()
	h := newReactorHost()
	if _, err := h.engineFor(ctx, reactorWasm); err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	// The sweep runs off the compile goroutine on purpose, so poll rather than
	// assert immediately. A fixed sleep would either flake or slow the suite.
	deadline := time.Now().Add(30 * time.Second)
	var remaining int64
	for time.Now().Before(deadline) {
		_, remaining, _ = cacheEntries(dir)
		if remaining <= 41943040 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if remaining > 41943040 {
		t.Fatalf("cache still holds %.1f MiB after a compile miss, over the 40 MiB budget. "+
			"engineForKey is not calling sweepCacheAsync — the evictor exists but nothing runs it.",
			float64(remaining)/(1<<20))
	}

	evicted := 0
	for _, d := range debris {
		if _, err := os.Stat(d); os.IsNotExist(err) {
			evicted++
		}
	}
	if evicted == 0 {
		t.Fatal("no debris was evicted; the sweep ran but found nothing to remove")
	}
	t.Logf("compile miss evicted %d/%d stale entries, leaving %.1f MiB",
		evicted, len(debris), float64(remaining)/(1<<20))
}

// exists to stop, at 1/1000 scale: the SAS decode guest is ~7.7 MB of wasm whose
// compiled form took ~30 MB per rebuild, and a development machine reached 20 GB
// of them. Rebuilds are what a supply-driven guest update does, so the count is
// unbounded over a runner's lifetime.
func TestSweepCache_ReclaimsRepeatedRebuilds(t *testing.T) {
	dir := t.TempDir()
	vdir := filepath.Join(dir, cacheVersionDirPrefix+"1.9.0-arm64-darwin")

	const rebuilds = 64
	const entrySize = 1 << 20
	const budget = 8 << 20
	for i := range rebuilds {
		// Each rebuild produces a distinct content hash, so nothing is
		// overwritten -- that is precisely why the directory grows.
		writeEntry(t, vdir, fmt.Sprintf("%064x", i), entrySize,
			time.Duration(rebuilds-i)*time.Minute)
	}

	if _, remaining, err := sweepCache(dir, budget); err != nil {
		t.Fatalf("sweepCache: %v", err)
	} else if remaining > budget {
		t.Fatalf("remaining = %d B, still over the %d B budget", remaining, budget)
	}

	// Assert the exact survivor count, not just "under budget": a sweep that
	// deleted all 64 would also be under budget, and would mean every restart
	// recompiles from scratch.
	files, err := os.ReadDir(vdir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if want := budget / entrySize; len(files) != want {
		t.Fatalf("%d entries survived, want exactly %d", len(files), want)
	}
}
