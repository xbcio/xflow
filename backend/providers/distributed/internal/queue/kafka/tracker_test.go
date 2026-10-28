package kafka

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// step is one action in a single-partition scenario. begin registers the
// offset; otherwise the offset's token is completed and the result checked.
type step struct {
	begin       bool
	offset      int64
	wantCommit  int64
	wantAdvance bool
	wantPending int
}

func begin(offset int64, pending int) step {
	return step{begin: true, offset: offset, wantPending: pending}
}

func done(offset, commit int64, advance bool, pending int) step {
	return step{offset: offset, wantCommit: commit, wantAdvance: advance, wantPending: pending}
}

func runSteps(t *testing.T, steps []step) {
	t.Helper()
	tr := newTracker()
	tokens := make(map[int64]token)
	for i, s := range steps {
		if s.begin {
			tokens[s.offset] = tr.Begin(0, s.offset)
		} else {
			commit, advanced := tr.Done(tokens[s.offset])
			if commit != s.wantCommit || advanced != s.wantAdvance {
				t.Fatalf("step %d: Done(%d) = (%d, %v), want (%d, %v)",
					i, s.offset, commit, advanced, s.wantCommit, s.wantAdvance)
			}
		}
		if got := tr.Pending(); got != s.wantPending {
			t.Fatalf("step %d: Pending() = %d, want %d", i, got, s.wantPending)
		}
	}
}

func TestTrackerSinglePartition(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "in order completion advances one by one",
			steps: []step{
				begin(0, 1), begin(1, 2), begin(2, 3),
				done(0, 0, true, 2),
				done(1, 1, true, 1),
				done(2, 2, true, 0),
			},
		},
		{
			name: "out of order completion advances once the head is filled",
			steps: []step{
				begin(0, 1), begin(1, 2), begin(2, 3), begin(3, 4),
				done(3, 0, false, 4),
				done(1, 0, false, 4),
				done(2, 0, false, 4),
				done(0, 3, true, 0),
			},
		},
		{
			name: "partial run stops at the next gap",
			steps: []step{
				begin(0, 1), begin(1, 2), begin(2, 3), begin(3, 4),
				done(2, 0, false, 4),
				done(0, 0, true, 3),
				done(3, 0, false, 3),
				done(1, 3, true, 0),
			},
		},
		{
			name: "offset holes still advance",
			steps: []step{
				begin(0, 1), begin(1, 2), begin(5, 3), begin(6, 4),
				done(5, 0, false, 4),
				done(6, 0, false, 4),
				done(0, 0, true, 3),
				done(1, 6, true, 0),
			},
		},
		{
			name: "duplicate done is ignored",
			steps: []step{
				begin(0, 1), begin(1, 2),
				done(1, 0, false, 2),
				done(1, 0, false, 2),
				done(0, 1, true, 0),
				done(0, 0, false, 0),
				done(1, 0, false, 0),
			},
		},
		{
			name: "done for an unregistered offset is ignored",
			steps: []step{
				begin(3, 1),
				done(4, 0, false, 1),
				done(3, 3, true, 0),
			},
		},
		{
			name: "pending counts done messages above a gap",
			steps: []step{
				begin(10, 1), begin(11, 2), begin(12, 3),
				done(12, 0, false, 3),
				done(11, 0, false, 3),
				done(10, 12, true, 0),
			},
		},
		{
			name: "registration continues after the queue drains",
			steps: []step{
				begin(0, 1), done(0, 0, true, 0),
				begin(1, 1), begin(2, 2),
				done(2, 0, false, 2),
				done(1, 2, true, 0),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runSteps(t, tc.steps) })
	}
}

func TestTrackerPartitionsAreIndependent(t *testing.T) {
	tr := newTracker()
	a0, a1 := tr.Begin(0, 100), tr.Begin(0, 101)
	b0, b1 := tr.Begin(1, 7), tr.Begin(1, 8)
	if got := tr.Pending(); got != 4 {
		t.Fatalf("Pending() = %d, want 4", got)
	}

	if c, ok := tr.Done(b1); ok {
		t.Fatalf("Done(b1) = (%d, true), want no advance", c)
	}
	if c, ok := tr.Done(a0); !ok || c != 100 {
		t.Fatalf("Done(a0) = (%d, %v), want (100, true)", c, ok)
	}
	if c, ok := tr.Done(b0); !ok || c != 8 {
		t.Fatalf("Done(b0) = (%d, %v), want (8, true)", c, ok)
	}
	if got := tr.Pending(); got != 1 {
		t.Fatalf("Pending() = %d, want 1", got)
	}
	if c, ok := tr.Done(a1); !ok || c != 101 {
		t.Fatalf("Done(a1) = (%d, %v), want (101, true)", c, ok)
	}
	if got := tr.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0", got)
	}
}

func TestTrackerRewindStartsNewEpoch(t *testing.T) {
	tests := []struct {
		name       string
		rewindTo   int64
		drainFirst bool
	}{
		{name: "lower offset while in flight", rewindTo: 5},
		{name: "same offset as the highest registered", rewindTo: 7},
		{name: "lower offset after the queue drained", rewindTo: 3, drainFirst: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTracker()
			old5, old6, old7 := tr.Begin(0, 5), tr.Begin(0, 6), tr.Begin(0, 7)
			if tc.drainFirst {
				tr.Done(old5)
				tr.Done(old6)
				tr.Done(old7)
			}

			fresh := tr.Begin(0, tc.rewindTo)
			if fresh.epoch == old5.epoch {
				t.Fatalf("rewind kept epoch %d, want a bumped epoch", fresh.epoch)
			}
			if got := tr.Pending(); got != 1 {
				t.Fatalf("Pending() after rewind = %d, want 1", got)
			}

			// Stale tokens must not touch the new epoch, even when an old
			// token shares the offset with the new one.
			for _, tk := range []token{old5, old6, old7} {
				if c, ok := tr.Done(tk); ok {
					t.Fatalf("stale Done(%+v) = (%d, true), want ignored", tk, c)
				}
			}
			if got := tr.Pending(); got != 1 {
				t.Fatalf("Pending() after stale Done = %d, want 1", got)
			}

			next := tr.Begin(0, tc.rewindTo+1)
			if c, ok := tr.Done(next); ok {
				t.Fatalf("Done(next) = (%d, true), want no advance behind the head", c)
			}
			if c, ok := tr.Done(fresh); !ok || c != tc.rewindTo+1 {
				t.Fatalf("Done(fresh) = (%d, %v), want (%d, true)", c, ok, tc.rewindTo+1)
			}
			if got := tr.Pending(); got != 0 {
				t.Fatalf("final Pending() = %d, want 0", got)
			}
		})
	}
}

func TestTrackerRewindOnlyAffectsItsPartition(t *testing.T) {
	tr := newTracker()
	a := tr.Begin(0, 10)
	b0, b1 := tr.Begin(1, 20), tr.Begin(1, 21)

	tr.Begin(0, 10) // redelivery on partition 0 only
	if c, ok := tr.Done(b1); ok {
		t.Fatalf("Done(b1) = (%d, true), want no advance", c)
	}
	if c, ok := tr.Done(b0); !ok || c != 21 {
		t.Fatalf("Done(b0) = (%d, %v), want (21, true)", c, ok)
	}
	if c, ok := tr.Done(a); ok {
		t.Fatalf("stale Done(a) = (%d, true), want ignored", c)
	}
	if got := tr.Pending(); got != 1 {
		t.Fatalf("Pending() = %d, want 1", got)
	}
}

func TestTrackerWaitBelowImmediate(t *testing.T) {
	tr := newTracker()
	tr.Begin(0, 0)
	tr.Begin(0, 1)

	tests := []struct {
		name  string
		limit int
	}{
		{name: "zero limit means no limit", limit: 0},
		{name: "negative limit means no limit", limit: -3},
		{name: "pending already below limit", limit: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A cancelled context must not matter when no wait is needed.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := tr.WaitBelow(ctx, tc.limit); err != nil {
				t.Fatalf("WaitBelow(limit=%d) = %v, want nil", tc.limit, err)
			}
		})
	}
}

func TestTrackerWaitBelowWokenByDone(t *testing.T) {
	tr := newTracker()
	t0, t1 := tr.Begin(0, 0), tr.Begin(0, 1)

	result := make(chan error, 1)
	go func() { result <- tr.WaitBelow(context.Background(), 2) }()

	select {
	case err := <-result:
		t.Fatalf("WaitBelow returned %v while Pending() == limit", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Done on the second message does not move the watermark: still blocked.
	tr.Done(t1)
	select {
	case err := <-result:
		t.Fatalf("WaitBelow returned %v without a watermark advance", err)
	case <-time.After(50 * time.Millisecond):
	}

	tr.Done(t0)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("WaitBelow = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitBelow was not woken by the advancing Done")
	}
}

func TestTrackerWaitBelowWokenByRewind(t *testing.T) {
	tr := newTracker()
	tr.Begin(0, 0)
	tr.Begin(0, 1)

	result := make(chan error, 1)
	go func() { result <- tr.WaitBelow(context.Background(), 2) }()
	time.Sleep(50 * time.Millisecond)

	// A rewind drops both in-flight messages and registers one: Pending 2 -> 1.
	tr.Begin(0, 0)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("WaitBelow = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitBelow was not woken by the rewind")
	}
}

func TestTrackerWaitBelowContextCancel(t *testing.T) {
	tr := newTracker()
	tr.Begin(0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- tr.WaitBelow(ctx, 1) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitBelow = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitBelow did not return after cancellation")
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer dcancel()
	if err := tr.WaitBelow(dctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitBelow = %v, want context.DeadlineExceeded", err)
	}
}

func TestTrackerWaitBelowNoGoroutineLeak(t *testing.T) {
	tr := newTracker()
	tokens := []token{tr.Begin(0, 0), tr.Begin(0, 1)}

	before := runtime.NumGoroutine()

	// Cancelled waiters must all exit.
	var wg sync.WaitGroup
	for range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		wg.Go(func() {
			defer cancel()
			_ = tr.WaitBelow(ctx, 1)
		})
	}
	wg.Wait()

	// Woken waiters must all exit too.
	for range 50 {
		wg.Go(func() {
			_ = tr.WaitBelow(context.Background(), 1)
		})
	}
	time.Sleep(20 * time.Millisecond)
	for _, tk := range tokens {
		tr.Done(tk)
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines: before=%d after=%d, want no leak", before, after)
	}
}

// commitLog records every advance returned by Done, per partition, in the
// order Done calls were made. Done is invoked while holding mu so the
// recorded order matches the tracker's internal order.
type commitLog struct {
	mu      sync.Mutex
	tr      *tracker
	last    map[int]int64
	errs    []string
	commits int
}

func newCommitLog(tr *tracker) *commitLog {
	return &commitLog{tr: tr, last: make(map[int]int64)}
}

func (l *commitLog) done(tk token) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.tr.Done(tk)
	if !ok {
		return
	}
	l.commits++
	if prev, seen := l.last[tk.partition]; seen && c <= prev {
		l.errs = append(l.errs, "commit not strictly increasing")
	}
	l.last[tk.partition] = c
}

// offsetsWithHoles returns n strictly increasing offsets with random gaps,
// the way a compacted topic or transaction markers would leave them.
func offsetsWithHoles(rng *rand.Rand, n int) []int64 {
	offsets := make([]int64, n)
	var next int64
	for i := range offsets {
		next += 1 + int64(rng.Intn(3))
		offsets[i] = next
	}
	return offsets
}

func checkCommitLog(t *testing.T, tr *tracker, l *commitLog, maxOffsets map[int]int64) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errs {
		t.Error(e)
	}
	if got := tr.Pending(); got != 0 {
		t.Errorf("final Pending() = %d, want 0", got)
	}
	for p, want := range maxOffsets {
		if got := l.last[p]; got != want {
			t.Errorf("partition %d: final commit = %d, want %d", p, got, want)
		}
	}
	if l.commits == 0 {
		t.Error("no watermark advance was observed")
	}
}

func TestTrackerConcurrentStress(t *testing.T) {
	const (
		partitions = 4
		perPart    = 2500
		workers    = 16
		limit      = 64
	)
	tr := newTracker()
	log := newCommitLog(tr)
	work := make(chan token, 32)
	maxOffsets := make(map[int]int64, partitions)

	var producers sync.WaitGroup
	var maxPending int
	var pendingMu sync.Mutex
	for p := range partitions {
		rng := rand.New(rand.NewSource(int64(p) + 1))
		offsets := offsetsWithHoles(rng, perPart)
		maxOffsets[p] = offsets[len(offsets)-1]
		producers.Go(func() {
			for _, off := range offsets {
				if err := tr.WaitBelow(context.Background(), limit); err != nil {
					t.Errorf("WaitBelow: %v", err)
					return
				}
				work <- tr.Begin(p, off)
			}
		})
	}

	var consumers sync.WaitGroup
	for w := range workers {
		rng := rand.New(rand.NewSource(int64(w) + 100))
		consumers.Go(func() {
			for tk := range work {
				for i := rng.Intn(4); i > 0; i-- {
					runtime.Gosched()
				}
				pendingMu.Lock()
				if n := tr.Pending(); n > maxPending {
					maxPending = n
				}
				pendingMu.Unlock()
				log.done(tk)
				log.done(tk) // duplicate Done must never advance
			}
		})
	}

	producers.Wait()
	close(work)
	consumers.Wait()

	checkCommitLog(t, tr, log, maxOffsets)
	// Each producer may register one message after WaitBelow returned.
	if maxPending > limit+partitions {
		t.Errorf("max Pending() = %d, want <= %d", maxPending, limit+partitions)
	}
}

func TestTrackerConcurrentShuffledDone(t *testing.T) {
	const (
		partitions = 3
		perPart    = 3000
		workers    = 8
	)
	tr := newTracker()
	log := newCommitLog(tr)
	maxOffsets := make(map[int]int64, partitions)

	// Begin runs concurrently across partitions but in order within each.
	tokensByPart := make([][]token, partitions)
	var begin sync.WaitGroup
	for p := range partitions {
		rng := rand.New(rand.NewSource(int64(p) + 50))
		offsets := offsetsWithHoles(rng, perPart)
		maxOffsets[p] = offsets[len(offsets)-1]
		begin.Go(func() {
			for _, off := range offsets {
				tokensByPart[p] = append(tokensByPart[p], tr.Begin(p, off))
			}
		})
	}
	begin.Wait()

	var all []token
	for _, ts := range tokensByPart {
		all = append(all, ts...)
	}
	rand.New(rand.NewSource(7)).Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if got := tr.Pending(); got != partitions*perPart {
		t.Fatalf("Pending() after Begin = %d, want %d", got, partitions*perPart)
	}

	var done sync.WaitGroup
	chunk := (len(all) + workers - 1) / workers
	for lo := 0; lo < len(all); lo += chunk {
		part := all[lo:min(lo+chunk, len(all))]
		done.Go(func() {
			for _, tk := range part {
				log.done(tk)
			}
		})
	}
	done.Wait()

	checkCommitLog(t, tr, log, maxOffsets)
}
