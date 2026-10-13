package timer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// timerFireReport is one drop this package reported.
type timerFireReport struct {
	reason string
	err    error
}

// recordingTimerFireObserver collects the drops this package reports.
type recordingTimerFireObserver struct {
	mu      sync.Mutex
	reports []timerFireReport
}

func (o *recordingTimerFireObserver) OnTriggerFireDropped(_ context.Context, reason string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reports = append(o.reports, timerFireReport{reason: reason, err: err})
}

func (o *recordingTimerFireObserver) seen() []timerFireReport {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]timerFireReport(nil), o.reports...)
}

// installTimerFireObserver puts a recorder in the package's slot for one test.
// The slot is process-wide and install-once, so the release is registered as
// cleanup rather than left to the test body: a failure partway through the body
// would otherwise leave the observer installed and panic the next test that tries.
func installTimerFireObserver(t *testing.T) *recordingTimerFireObserver {
	t.Helper()
	rec := &recordingTimerFireObserver{}
	SetObserver(rec)
	t.Cleanup(func() { SetObserver(nil) })
	return rec
}

// waitForTimerReason polls until the recorder has seen want, or fails. The tick's
// admission runs on the trigger's own goroutine, so the report lands after the
// call that caused it has returned — there is nothing to wait on but the report
// itself.
func waitForTimerReason(t *testing.T, rec *recordingTimerFireObserver, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		for _, report := range rec.seen() {
			if report.reason == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q reported within %s; saw %v", want, within, rec.seen())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTimerTriggerCountsTheDroppedTicks pins one row per drop the two hosting
// paths have, against the same drops the log lines describe.
//
// A tick is a moment: nothing durable records that it was ever offered, so a
// schedule throttled by its own workflow is otherwise indistinguishable from one
// that never fired. The reasons are read apart because they call for opposite
// responses — refused is the workflow declining its own traffic (capacity, or a
// queue mode), while transport is the control plane being unreachable, which is
// lost work.
func TestTimerTriggerCountsTheDroppedTicks(t *testing.T) {
	t.Run("an engine failure on the embedded path", func(t *testing.T) {
		rec := installTimerFireObserver(t)
		rt := triggertest.NewFakeRuntime()
		rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
			return "", engine.ErrConcurrencyLimit
		})

		tr := New().Every(5 * time.Millisecond)
		sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "timer",
			Params:     tr.RawParams().(map[string]any),
			Runtime:    rt,
		})
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		defer func() { _ = sub.Close(context.Background()) }()

		// The reason is emit_error even though the error IS the concurrency
		// limit: this layer cannot ask the question, and the observer narrows it
		// (see fireDropReason in observability/metrics).
		waitForTimerReason(t, rec, types.TriggerFireDroppedEmitError, 3*time.Second)

		// What must survive the trip is the sentinel itself: the narrowing is
		// made on errors.Is, so an error dropped here would LOSE the concurrency
		// label rather than change it.
		var carried bool
		for _, report := range rec.seen() {
			if errors.Is(report.err, engine.ErrConcurrencyLimit) {
				carried = true
			}
		}
		if !carried {
			t.Fatalf("no report carried the engine's concurrency sentinel; saw %v", rec.seen())
		}
	})

	t.Run("refused by the control plane", func(t *testing.T) {
		restore := shrinkRetryWindow(t)
		defer restore()
		rec := installTimerFireObserver(t)

		rt := triggertest.NewFakeEntrySeedRuntime()
		rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
			return types.EntrySeedResponse{Refused: true}, nil
		})
		tr := New().Every(500 * time.Millisecond)
		sub, err := tr.Activate(context.Background(), seedInput(rt, "500ms"))
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		defer func() { _ = sub.Close(context.Background()) }()

		waitForTimerReason(t, rec, types.TriggerFireDroppedRefused, 3*time.Second)
	})

	t.Run("lost to the transport", func(t *testing.T) {
		restore := shrinkRetryWindow(t)
		defer restore()
		rec := installTimerFireObserver(t)

		rt := triggertest.NewFakeEntrySeedRuntime()
		rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
			return types.EntrySeedResponse{}, context.DeadlineExceeded
		})
		tr := New().Every(500 * time.Millisecond)
		sub, err := tr.Activate(context.Background(), seedInput(rt, "500ms"))
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		defer func() { _ = sub.Close(context.Background()) }()

		waitForTimerReason(t, rec, types.TriggerFireDroppedTransport, 3*time.Second)
	})
}

// TestTimerTriggerCountsOnlyTheDrops keeps the counter honest on the path it
// shares with cron: an admitted tick must reach the counter not at all. A counter
// that also counted the successes could not produce a drop rate, which is the only
// thing an operator can alert on.
func TestTimerTriggerCountsOnlyTheDrops(t *testing.T) {
	rec := installTimerFireObserver(t)
	rt := triggertest.NewFakeRuntime()

	tr := New().Every(5 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(time.Second) {
		t.Fatal("timer did not emit")
	}
	// Long enough for several more ticks to have landed had anything been
	// reporting; the point is that nothing is.
	time.Sleep(50 * time.Millisecond)
	if got := rec.seen(); len(got) != 0 {
		t.Fatalf("admitted ticks reported %v, want nothing", got)
	}
}
