package cron

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

// cronFireReport is one drop this package reported.
type cronFireReport struct {
	reason string
	err    error
}

// recordingCronFireObserver collects the drops this package reports.
type recordingCronFireObserver struct {
	mu      sync.Mutex
	reports []cronFireReport
}

func (o *recordingCronFireObserver) OnTriggerFireDropped(_ context.Context, reason string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reports = append(o.reports, cronFireReport{reason: reason, err: err})
}

func (o *recordingCronFireObserver) seen() []cronFireReport {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]cronFireReport(nil), o.reports...)
}

// installFireObserver puts a recorder in the package's slot for one test. The
// slot is process-wide and install-once, so the release is registered as cleanup
// rather than left to the test body: a failure partway through the body would
// otherwise leave the observer installed and panic the next test that tries.
func installFireObserver(t *testing.T) *recordingCronFireObserver {
	t.Helper()
	rec := &recordingCronFireObserver{}
	SetObserver(rec)
	t.Cleanup(func() { SetObserver(nil) })
	return rec
}

// TestCronTriggerCountsTheFiringsItDropped pins the counter against the same
// three drops the log lines describe, one row each, on the embedded path.
//
// The counter exists because none of these three leaves anything durable behind:
// no execution, no offset, nothing the watermark can tell apart from a punctual
// firing. A schedule throttled by its own workflow therefore reads exactly like a
// schedule that never fired, and the log line — which an operator only sees if
// they already suspected something — is the only other trace.
//
// Two of the three are reported as emit_error, and that is the whole story this
// layer can tell: node/ cannot import package engine, so the trigger cannot ask
// whether the error it got was the concurrency limit. The error travels WITH the
// report and the metrics observer narrows it (see fireDropReason in
// observability/metrics). What this test pins is that the error survives the
// trip intact — its identity is what the narrowing is made on.
func TestCronTriggerCountsTheFiringsItDropped(t *testing.T) {
	ctx := context.Background()
	refused := errors.New("engine is closed")
	cases := []struct {
		name string
		// emit is what the runtime answers the admission with.
		emit func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error)
		// plan decides the firing before it reaches the runtime at all.
		plan overlapPlan
		// wantEmit is whether the firing must still have been offered to the
		// runtime. The overlap row's answer must be no: a counter that named
		// overlap while the run it was supposed to hold back had already been
		// submitted would be reporting a decision that was not made.
		wantEmit bool
		want     string
		// wantErr is the failure the report must carry, or nil.
		wantErr error
	}{
		{
			name: "overlap",
			emit: func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
				return "exec-1", nil
			},
			plan:     overlapPlan{policy: overlapSkip, reader: &stubStatusReader{status: types.ExecutionStatusRunning, found: true}},
			wantEmit: false,
			want:     types.TriggerFireDroppedOverlap,
		},
		{
			name: "the concurrency limit is reported as an emit error, with its sentinel",
			emit: func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
				return "", engine.ErrConcurrencyLimit
			},
			plan:     overlapPlan{policy: overlapAllow},
			wantEmit: true,
			want:     types.TriggerFireDroppedEmitError,
			wantErr:  engine.ErrConcurrencyLimit,
		},
		{
			name: "any other engine failure",
			emit: func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
				return "", refused
			},
			plan:     overlapPlan{policy: overlapAllow},
			wantEmit: true,
			want:     types.TriggerFireDroppedEmitError,
			wantErr:  refused,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := installFireObserver(t)
			rt := newEmbeddedRuntime()
			rt.SetEmitFunc(tc.emit)
			in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "nightly", Runtime: rt}
			state := rt.State(ctx, watermarkScope("wf-1", "nightly"))
			// The run the overlap policy asks about: recorded as started, and still
			// going according to the reader above.
			if err := state.Set(ctx, lastExecutionKey, []byte("exec-running"), 0); err != nil {
				t.Fatalf("record last execution: %v", err)
			}

			now := time.Now().UTC()
			New().emitScheduledFire(ctx, in, minuteSchedule(t), cronFire{
				scheduled: now.Truncate(time.Minute),
				observed:  now,
			}, catchupConfig{}, tc.plan, state)

			reports := rec.seen()
			if len(reports) != 1 || reports[0].reason != tc.want {
				t.Fatalf("dropped firings = %v, want exactly one report with reason %s: a drop "+
					"counted twice, or under the wrong reason, is a schedule whose throttle "+
					"cannot be told from a fault", reports, tc.want)
			}
			if !errors.Is(reports[0].err, tc.wantErr) || (tc.wantErr == nil) != (reports[0].err == nil) {
				t.Fatalf("the report carried error %v, want %v: the observer narrows the reason "+
					"from this error, so losing it turns the concurrency limit into a broken engine",
					reports[0].err, tc.wantErr)
			}
			if emitted := rt.EmitCount() == 1; emitted != tc.wantEmit {
				t.Fatalf("the runtime was offered the firing = %v, want %v", emitted, tc.wantEmit)
			}
			// A dropped firing is decided, so the instant moves behind the trigger
			// either way — the counter reports where it went, it does not hold it
			// back for a retry.
			if _, ok := readWatermark(ctx, state); !ok {
				t.Fatal("a dropped firing left no watermark; the instant would be re-offered")
			}
		})
	}
}

// TestCronEntrySeedCountsTheFiringsTheControlPlaneDropped is the other half: on a
// runner that admits through the control plane there is no Emit to fail, so the
// two drops this path has are the control plane's own refusal and a transport
// failure past the retry bound. Here the classification IS decidable — the
// response says which it was — so both reach the counter under their own reason,
// and an admitted firing must reach it not at all. A counter that also counted
// the successes would make the drop rate unreadable against the firing rate.
func TestCronEntrySeedCountsTheFiringsTheControlPlaneDropped(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		respond func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error)
		// want is the one reason expected; empty means nothing may be reported.
		want string
	}{
		{
			name: "an admitted firing is not a drop",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Accepted: true}, nil
			},
		},
		{
			name: "refused",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{Refused: true}, nil
			},
			want: types.TriggerFireDroppedRefused,
		},
		{
			name: "transport",
			respond: func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
				return types.EntrySeedResponse{}, context.DeadlineExceeded
			},
			want: types.TriggerFireDroppedTransport,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := shrinkRetryWindow(t)
			defer restore()
			rec := installFireObserver(t)

			rt := triggertest.NewFakeEntrySeedRuntime()
			rt.SetResponder(tc.respond)
			in := seedInput(rt, "*/1 * * * *")
			now := time.Now()
			New().admitScheduledFire(ctx, in, rt, minuteSchedule(t), cronFire{
				scheduled: now,
				observed:  now,
			}, watermarkState(t, rt))

			reports := rec.seen()
			if tc.want == "" {
				if len(reports) != 0 {
					t.Fatalf("an admitted firing reported %v, want nothing: successes in the "+
						"drop counter make its rate meaningless", reports)
				}
				return
			}
			if len(reports) != 1 || reports[0].reason != tc.want {
				t.Fatalf("dropped firings = %v, want exactly one report with reason %s", reports, tc.want)
			}
			// A refusal is a decision and carries no failure; a transport drop does.
			if tc.want == types.TriggerFireDroppedRefused && reports[0].err != nil {
				t.Fatalf("a refused firing carried error %v, want nil", reports[0].err)
			}
			if tc.want == types.TriggerFireDroppedTransport && reports[0].err == nil {
				t.Fatal("a transport drop carried no error; the log line and the metric would " +
					"both lose the cause")
			}
		})
	}
}

// TestCronTriggerReportsNothingWithoutAnObserver is the deployment-side default:
// a process that installs no observer must keep firing normally. The slot is read
// on the firing path, so an implementation that required an observer would turn
// "no metrics configured" into "no schedules run".
func TestCronTriggerReportsNothingWithoutAnObserver(t *testing.T) {
	ctx := context.Background()
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "exec-1", nil
	})

	tr := New().Cron("@every 1s")
	sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params:     map[string]any{"expression": "@every 1s"},
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if !rt.WaitEmit(3 * time.Second) {
		t.Fatal("the schedule did not fire with no observer installed")
	}
}
