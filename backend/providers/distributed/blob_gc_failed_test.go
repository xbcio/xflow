package distributed

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

// gcFailObs records OnGC calls through the embedded gcObs and every OnGCFailed
// call, so one pass can be checked on both halves of the surface.
type gcFailObs struct {
	gcObs
	mu     sync.Mutex
	failed []int
}

func (o *gcFailObs) OnGCFailed(failed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = append(o.failed, failed)
}

func (o *gcFailObs) failures() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.failed...)
}

// requireFailures fails unless the observer saw exactly want.
func requireFailures(t *testing.T, o *gcFailObs, want ...int) {
	t.Helper()
	got := o.failures()
	if len(got) != len(want) {
		t.Fatalf("OnGCFailed calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("OnGCFailed calls = %v, want %v", got, want)
		}
	}
}

// The objects one pass could not delete are its failure count, and the ordinary
// OnGC report still arrives. Mutation: dropping the OnGCFailed call leaves the
// observer empty.
func TestGCLoopReportsObjectDeleteFailures(t *testing.T) {
	obs := &gcFailObs{}
	run := gcAnswers(gcStep{res: blob.CollectResult{Candidates: 5, Deleted: 1, Failed: 3}})
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs, Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick()
	requireFailures(t, obs, 3)
	if got := obs.got(); len(got) != 1 || got[0] != [2]int{1, 0} {
		t.Fatalf("OnGC calls = %v, want the pass's deleted/fenced counts", got)
	}
}

// A pass that failed as a whole counts as one, even with no object failures.
// Mutation: dropping the +1 leaves the observer empty.
func TestGCLoopReportsFailedPassAsOne(t *testing.T) {
	obs := &gcFailObs{}
	run := gcAnswers(gcStep{err: errors.New("index unavailable")})
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs, Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick()
	requireFailures(t, obs, 1)
}

// A pass with both kinds of failure reports their sum. Mutation: reporting only
// one of the two turns this red.
func TestGCLoopAddsPassFailureToObjectFailures(t *testing.T) {
	obs := &gcFailObs{}
	run := gcAnswers(gcStep{
		res: blob.CollectResult{Candidates: 5, Failed: 2},
		err: errors.New("index unavailable"),
	})
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs, Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick()
	requireFailures(t, obs, 3)
}

// A pass cut short by shutdown returns ctx's error, but that is not a store
// failure. Mutation: counting every error turns this red.
func TestGCLoopDoesNotReportShutdownCancellation(t *testing.T) {
	obs := &gcFailObs{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := func(context.Context) (blob.CollectResult, error) {
		cancel()
		return blob.CollectResult{}, context.Canceled
	}
	opts := blobGCOptions{Batch: gcDefaultBatch, MaxRunsPerTick: 1, Observer: obs}
	if goOn := gcTick(ctx, run, opts); !goOn {
		t.Fatal("gcTick ended the loop on a cancelled context, want it to go on")
	}
	requireFailures(t, obs)
}

// A pass with nothing failed reports nothing: the series is created by the first
// failure, not by the first pass. Mutation: dropping the failed > 0 guard in
// gcTick turns this red.
func TestGCLoopDoesNotReportQuietPass(t *testing.T) {
	obs := &gcFailObs{}
	run := gcAnswers(gcStep{res: blob.CollectResult{Candidates: 5, Deleted: 5}})
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs})
	h.awaitIdle()
	h.tick()
	requireFailures(t, obs)
}

// An observer that predates the failure hook must keep working unchanged: the
// type assertion fails quietly and OnGC still sees the pass, failures included.
// Mutation: a type assertion without the ok check would panic here.
func TestGCLoopObserverWithoutFailureHookIsSafe(t *testing.T) {
	obs := &gcObs{}
	run := gcAnswers(gcStep{res: blob.CollectResult{Candidates: 5, Deleted: 2, Fenced: 1, Failed: 4}})
	h := newGCHarness(t, run.run, blobGCOptions{Observer: obs, Logger: &gcRecLogger{}})
	h.awaitIdle()
	h.tick()
	if got := obs.got(); len(got) != 1 || got[0] != [2]int{2, 1} {
		t.Fatalf("OnGC calls = %v, want [[2 1]]", got)
	}
}
