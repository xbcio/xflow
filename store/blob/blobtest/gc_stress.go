package blobtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

// Shape of the stress run. The contents are few on purpose: with many writers
// per digest, every round has several Puts racing a collector over each object.
const (
	stressContents   = 6
	stressWriters    = 4
	stressCollectors = 2
	stressPuts       = 6 // Puts per writer per round
	stressBatch      = 3 // small, so one Run is a short pass and many interleave
	stressAttempts   = 3 // Put+Link tries before a writer gives up
)

// stressTotals is what the collectors and writers did across all rounds. It is
// logged so a run shows whether the races it exists for actually happened.
type stressTotals struct {
	puts, deleted, fenced, skipped, failed, linkRetries atomic.Int64
}

// RunGCStress hammers one Index with concurrent writers and collectors for the
// given number of rounds, and fails on the one outcome the fence exists to
// prevent: a Ref that a successful Put returned but whose bytes cannot be read.
//
// Time is a controlled variable, not noise. Every round starts by aging the
// clock past the grace period, so whatever earlier rounds left behind is garbage
// the collectors race the writers for; inside a round the clock does not move,
// so anything a writer touches stays inside its grace period. That mirrors the
// real contract (grace outlasts the gap between a Put and its Link) and lets the
// run assert strictly: after Put and again after Link the Ref must read back the
// exact bytes. A writer whose Link reports ErrFenced or ErrNotFound re-Puts, as
// the protocol tells it to, and the number of those is reported.
//
// Run it with -race. Each round is a barrier, so the run is repeatable in its
// shape, not in its interleavings; the deterministic cases in RunGCScenarios are
// the ones that pin a specific interleaving.
func RunGCStress(t *testing.T, newIndex IndexFactory, rounds int) {
	t.Helper()
	r := newGCRig(t, newIndex(t), stressBatch)
	// Widen the window between a claim and its confirm so writers land in it.
	r.hook.AfterMark = func(blob.Fence) {
		for range 3 {
			runtime.Gosched()
		}
	}
	contents := make([][]byte, stressContents)
	for i := range contents {
		if i%2 == 0 {
			contents[i] = compressible(fmt.Sprintf("stress %d", i))
		} else {
			contents[i] = []byte(fmt.Sprintf("a small stress blob, number %d", i))
		}
	}

	var totals stressTotals
	for round := 0; round < rounds && !t.Failed(); round++ {
		r.age()
		stressRound(r, contents, round, &totals)
	}
	if t.Failed() {
		return
	}

	// Drain: with every writer gone and everything aged, the collector must be
	// able to remove it all, and nothing readable may be left behind.
	r.age()
	for pass := 0; ; pass++ {
		if pass == 10*stressContents {
			t.Fatalf("collector still finds candidates after %d passes", pass)
		}
		res := r.run()
		totals.deleted.Add(int64(res.Deleted))
		if res.Candidates == 0 {
			break
		}
	}
	for _, data := range contents {
		ref := r.put(data) // also proves a Put after total collection works
		r.wantReadable("rewritten after the drain", ref, data)
	}
	t.Logf("stress: %d rounds, %d puts, %d link retries; collector deleted %d, lost the fence %d, skipped %d, failed %d",
		rounds, totals.puts.Load(), totals.linkRetries.Load(), totals.deleted.Load(), totals.fenced.Load(), totals.skipped.Load(), totals.failed.Load())
}

// stressRound runs one round: writers and collectors start together and the
// collectors keep going until every writer is done.
func stressRound(r *gcRig, contents [][]byte, round int, totals *stressTotals) {
	start := make(chan struct{})
	stop := make(chan struct{})
	var writers, collectors sync.WaitGroup
	for w := range stressWriters {
		writers.Add(1)
		go func() {
			defer writers.Done()
			rng := rand.New(rand.NewSource(int64(round*stressWriters + w)))
			<-start
			for i := range stressPuts {
				r.stressWrite(rng, fmt.Sprintf("exec-%d-%d-%d", round, w, i), contents[rng.Intn(len(contents))], totals)
			}
		}()
	}
	for range stressCollectors {
		collectors.Add(1)
		go func() {
			defer collectors.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := r.gc.Run(context.Background())
				totals.deleted.Add(int64(res.Deleted))
				totals.fenced.Add(int64(res.Fenced))
				totals.skipped.Add(int64(res.Skipped))
				totals.failed.Add(int64(res.Failed))
				if err != nil {
					r.t.Errorf("round %d: Collector.Run error = %v (result %+v)", round, err, res)
					return
				}
				runtime.Gosched()
			}
		}()
	}
	close(start)
	writers.Wait()
	close(stop)
	collectors.Wait()
}

// stressWrite is one writer operation: Put, Link, read back, with a few
// scheduler yields between the steps so the collector can land in each gap.
func (r *gcRig) stressWrite(rng *rand.Rand, execID string, data []byte, totals *stressTotals) {
	ctx := context.Background()
	for range stressAttempts {
		yield(rng)
		ref, err := r.store.Put(ctx, idxNS, data)
		if err != nil {
			r.t.Errorf("Put error = %v", err)
			return
		}
		totals.puts.Add(1)
		if !r.stressReadable("right after Put", ref, data) {
			return
		}
		yield(rng)
		err = r.hook.Link(ctx, execID, ref, r.h.Clock.Now().Add(grace/2))
		if errors.Is(err, blob.ErrFenced) || errors.Is(err, blob.ErrNotFound) {
			totals.linkRetries.Add(1)
			continue
		}
		if err != nil {
			r.t.Errorf("Link error = %v", err)
			return
		}
		yield(rng)
		r.stressReadable("after Link", ref, data)
		return
	}
	r.t.Errorf("Put and Link kept losing to the collector: %d attempts for one blob", stressAttempts)
}

// stressReadable reports whether ref reads back as data, failing the test (and
// returning false) if not. It is the safe-for-goroutines form of wantReadable.
func (r *gcRig) stressReadable(when string, ref blob.Ref, data []byte) bool {
	got, err := r.store.Get(context.Background(), idxNS, ref)
	if err != nil {
		r.t.Errorf("DANGLING REF %s: Get(%s) %s error = %v", ref.Digest[:8], ref.Namespace, when, err)
		return false
	}
	if !bytes.Equal(got, data) {
		r.t.Errorf("WRONG BYTES %s: Get %s returned %d bytes, want %d", ref.Digest[:8], when, len(got), len(data))
		return false
	}
	return true
}

// yield gives the scheduler a few chances to run another goroutine.
func yield(rng *rand.Rand) {
	for n := rng.Intn(4); n > 0; n-- {
		runtime.Gosched()
	}
}
