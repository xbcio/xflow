package rstate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestDeadlineMemoIsSafeUnderConcurrentUse hammers the memo the way a busy
// replica does: the creator remembers, readers learn what they saw in Redis, and
// the terminal write evicts, all on the same execution at once. The memo is a
// plain map behind ttlMu, so a path that touches it without the lock is a data
// race (and, for a write, a fatal concurrent map write); run with -race this is
// the test that fails for it.
func TestDeadlineMemoIsSafeUnderConcurrentUse(t *testing.T) {
	ctx := context.Background()
	_, rdb, _ := newMiniredisStore(t)
	state := newReplica(rdb)

	const (
		workers = 8
		rounds  = 2000
	)
	created := time.UnixMilli(1_700_000_000_000)
	read := created.Add(time.Hour)
	shared := types.ExecutionID("memo-concurrent-shared")

	var wg sync.WaitGroup
	start := make(chan struct{})
	run := func(fn func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < rounds; i++ {
				fn(i)
			}
		}()
	}

	for w := 0; w < workers; w++ {
		w := w
		// The creator side: an unconditional overwrite.
		run(func(int) { state.rememberExecDeadline(shared, created) })
		// The reader side: set-if-absent, which must hand back a value somebody
		// wrote, never a torn or zero one.
		run(func(int) {
			got := state.learnExecDeadline(shared, read)
			if !got.Equal(created) && !got.Equal(read) {
				t.Errorf("learnExecDeadline returned %v, want %v or %v", got, created, read)
			}
		})
		// The terminal write.
		run(func(int) { state.evictExecutionCaches(shared) })
		// A full read path: memo check, Redis read, learn. The execution does not
		// exist, so nothing is memoized by it; the memo reads still race the writes.
		run(func(int) { state.execDeadline(ctx, shared) })
		// Distinct executions, enough of them to push the memo through its bound
		// and clear it while the others are writing.
		run(func(i int) {
			id := types.ExecutionID(fmt.Sprintf("memo-concurrent-%d-%d", w, i))
			state.rememberExecDeadline(id, created)
			state.learnExecDeadline(id, read)
		})
	}
	close(start)
	wg.Wait()

	if !t.Failed() {
		state.ttlMu.RLock()
		memo, known := state.execDeadlines[shared]
		size := len(state.execDeadlines)
		state.ttlMu.RUnlock()
		if known && !memo.Equal(created) && !memo.Equal(read) {
			t.Fatalf("memo for the shared execution = %v, want %v or %v", memo, created, read)
		}
		if size > execDeadlineMemoCap {
			t.Fatalf("memo holds %d entries after the run, want at most %d", size, execDeadlineMemoCap)
		}
	}
}
