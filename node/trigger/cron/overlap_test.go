package cron

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestParseOverlapPolicy pins what the param accepts, including the value it
// deliberately does not: a definition that names "queue" is told where
// serialisation actually lives instead of being accepted and ignored.
func TestParseOverlapPolicy(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		want    overlapPolicy
		wantErr string
	}{
		{name: "absent is allow", value: nil, want: overlapAllow},
		{name: "empty is allow", value: "", want: overlapAllow},
		{name: "allow", value: "allow", want: overlapAllow},
		{name: "skip", value: "skip", want: overlapSkip},
		{name: "wrong case is refused", value: "Skip", wantErr: "overlap must be one of"},
		{name: "a typo is refused", value: "skip-runs", wantErr: "overlap must be one of"},
		{name: "a bool is refused", value: true, wantErr: "overlap must be one of"},
		{
			name:  "queue is refused by name",
			value: "queue",
			// The value is not accepted, but the message must not read like a typo:
			// an operator who asked for serialised runs needs to be told that the
			// workflow setting is where that is expressed.
			wantErr: "settings.concurrency mode=queue with limit=1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseOverlapPolicy(tc.value)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseOverlapPolicy(%v) error = %v, want it to contain %q", tc.value, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOverlapPolicy(%v) error = %v", tc.value, err)
			}
			if got != tc.want {
				t.Fatalf("parseOverlapPolicy(%v) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

// TestOverlapAdmits pins the policy's one question against every answer a store
// can give, because the wrong side of this decision is not symmetric: an extra
// run is work done twice, a dropped firing is work not done at all, and the
// parameter exists to trade the first for the second only when the previous run
// is KNOWN to still be going.
func TestOverlapAdmits(t *testing.T) {
	ctx := context.Background()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "nightly"}
	const running = types.ExecutionID("exec-running")
	// The status a real store would answer for the execution the trigger started
	// last: what it is, and whether it is still there at all.
	store := func(status types.ExecutionStatus, found bool) *stubStatusReader {
		return &stubStatusReader{status: status, found: found}
	}

	cases := []struct {
		name string
		plan overlapPlan
		// last is what the trigger recorded, empty for "nothing recorded".
		last types.ExecutionID
		want bool
	}{
		{
			name: "allow never asks",
			plan: overlapPlan{policy: overlapAllow, reader: store(types.ExecutionStatusRunning, true)},
			last: running,
			want: true,
		},
		{
			name: "skip with nothing recorded has nothing to wait for",
			plan: overlapPlan{policy: overlapSkip, reader: store(types.ExecutionStatusRunning, true)},
			want: true,
		},
		{
			name: "skip lets the firing through once the run finished",
			plan: overlapPlan{policy: overlapSkip, reader: store(types.ExecutionStatusSuccess, true)},
			last: running,
			want: true,
		},
		{
			name: "skip drops the firing while the run is going",
			plan: overlapPlan{policy: overlapSkip, reader: store(types.ExecutionStatusRunning, true)},
			last: running,
		},
		{
			name: "a run parked in the queue still counts as unfinished",
			plan: overlapPlan{policy: overlapSkip, reader: store(types.ExecutionStatusQueued, true)},
			last: running,
		},
		{
			name: "skip lets the firing through when the store no longer has the execution",
			plan: overlapPlan{policy: overlapSkip, reader: store("", false)},
			last: running,
			want: true,
		},
		{
			name: "skip lets the firing through when the status cannot be read",
			plan: overlapPlan{policy: overlapSkip, reader: &stubStatusReader{err: errors.New("redis: connection refused")}},
			last: running,
			want: true,
		},
		{
			name: "a plan with no reader cannot drop anything",
			plan: overlapPlan{policy: overlapSkip},
			last: running,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := newEmbeddedRuntime()
			state := rt.State(ctx, watermarkScope("wf-1", "nightly"))
			if tc.last != "" {
				if err := state.Set(ctx, lastExecutionKey, []byte(tc.last), 0); err != nil {
					t.Fatalf("record last execution: %v", err)
				}
			}
			if got := tc.plan.admits(ctx, state, in); got != tc.want {
				t.Fatalf("admits = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestActivateRecordsTheExecutionItStarted is the half that makes the policy
// work at all: the id the runtime returned from the admission is what the next
// instant's decision is about. Without this write the policy sees no run of its
// own, admits every firing, and reads exactly like overlap=allow.
func TestActivateRecordsTheExecutionItStarted(t *testing.T) {
	ctx := context.Background()
	rt := newEmbeddedRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "exec-7", nil
	})
	now := time.Now().UTC()
	scope := watermarkScope("wf-1", "nightly")
	if err := writeWatermark(ctx, rt.State(ctx, scope), now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("writeWatermark: %v", err)
	}

	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params: map[string]any{
			"expression":     "*/1 * * * *",
			"misfire":        "fire_all",
			"max_catchup":    float64(1),
			"catchup_window": "1h",
		},
		Runtime: rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	recorded, ok := readLastExecution(ctx, rt.State(ctx, scope))
	if !ok {
		t.Fatal("the admitted firing left no record of the execution it started")
	}
	if recorded != "exec-7" {
		t.Fatalf("recorded execution = %q, want the id the admission returned (exec-7)", recorded)
	}
}

// TestActivateSkipsAFiringWhileTheLastRunIsGoing drives the policy end to end on
// the embedded path, through the same activation-time catch-up the rest of this
// package's firing tests use: a record of a run that is still going, and one
// firing due. The control is the same activation with the default policy — which
// must fire — so a "skip" that came from the schedule rather than from the policy
// cannot pass this test.
func TestActivateSkipsAFiringWhileTheLastRunIsGoing(t *testing.T) {
	ctx := context.Background()

	activate := func(t *testing.T, params map[string]any) *embeddedRuntime {
		t.Helper()
		rt := newEmbeddedRuntime()
		rt.setExecutionStatus("exec-running", types.ExecutionStatusRunning)
		scope := watermarkScope("wf-1", "nightly")
		state := rt.State(ctx, scope)
		// A firing is due (the record is two minutes old, the cadence is a
		// minute), and the run this trigger started last (exec-running) is what
		// the Emit callback below reports every admission to be.
		if err := writeWatermark(ctx, state, time.Now().UTC().Add(-2*time.Minute)); err != nil {
			t.Fatalf("writeWatermark: %v", err)
		}
		if err := state.Set(ctx, lastExecutionKey, []byte("exec-running"), 0); err != nil {
			t.Fatalf("record last execution: %v", err)
		}
		rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
			return "exec-running", nil
		})

		full := map[string]any{
			"expression":     "*/1 * * * *",
			"misfire":        "fire_all",
			"max_catchup":    float64(1),
			"catchup_window": "1h",
		}
		for k, v := range params {
			full[k] = v
		}
		sub, err := New().Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "nightly",
			Params:     full,
			Runtime:    rt,
		})
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		defer func() { _ = sub.Close(ctx) }()
		return rt
	}

	t.Run("the default policy fires", func(t *testing.T) {
		if got := activate(t, nil).EmitCount(); got != 1 {
			t.Fatalf("emitted %d events, want 1: with no overlap policy named the run starts anyway", got)
		}
	})

	t.Run("skip drops the firing", func(t *testing.T) {
		rt := activate(t, map[string]any{"overlap": "skip"})
		if got := rt.EmitCount(); got != 0 {
			t.Fatalf("emitted %d events, want none: the execution this trigger started last has not finished", got)
		}
		// The instant is behind the trigger even though nothing ran: a firing the
		// policy dropped is decided, and re-offering it at the next activation
		// would make the policy a delay rather than a skip.
		recorded, ok := readWatermark(ctx, rt.State(ctx, watermarkScope("wf-1", "nightly")))
		if !ok {
			t.Fatal("the skipped firing left no watermark")
		}
		if want := time.Now().UTC().Truncate(time.Minute); !recorded.Equal(want) {
			t.Fatalf("watermark = %s, want %s: the skipped instant is behind the trigger", recorded, want)
		}
	})

	t.Run("skip fires once the run is finished", func(t *testing.T) {
		rt := newEmbeddedRuntime()
		rt.setExecutionStatus("exec-running", types.ExecutionStatusFailed)
		scope := watermarkScope("wf-1", "nightly")
		state := rt.State(ctx, scope)
		if err := writeWatermark(ctx, state, time.Now().UTC().Add(-2*time.Minute)); err != nil {
			t.Fatalf("writeWatermark: %v", err)
		}
		if err := state.Set(ctx, lastExecutionKey, []byte("exec-running"), 0); err != nil {
			t.Fatalf("record last execution: %v", err)
		}

		sub, err := New().Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "nightly",
			Params: map[string]any{
				"expression":     "*/1 * * * *",
				"misfire":        "fire_all",
				"max_catchup":    float64(1),
				"catchup_window": "1h",
				"overlap":        "skip",
			},
			Runtime: rt,
		})
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		defer func() { _ = sub.Close(ctx) }()

		if got := rt.EmitCount(); got != 1 {
			t.Fatalf("emitted %d events, want 1: the previous run ended, so the next firing starts", got)
		}
	})
}

// TestActivateDegradesOverlapWithoutTheCapability is the documented degradation:
// a runtime with no way to read an execution's status cannot enforce skip, so the
// activation says so once and runs as allow. The alternative — refusing to
// activate, or dropping firings on a question it cannot ask — would turn a missing
// capability into a schedule that stops.
func TestActivateDegradesOverlapWithoutTheCapability(t *testing.T) {
	ctx := context.Background()
	buf := captureCronLog(t)
	// FakeRuntime is a types.TriggerRuntime without the optional status reader,
	// which is exactly the runtime this degradation is about.
	rt := triggertest.NewFakeRuntime()

	sub, err := New().Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "nightly",
		Params: map[string]any{
			"expression": "@every 1s",
			"overlap":    "skip",
		},
		Runtime: rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if !strings.Contains(buf.String(), "cannot read execution status") {
		t.Fatalf("the degradation was not reported; captured:\n%s", buf.String())
	}

	// And the fired schedule is untouched: the next instant still fires.
	if !rt.WaitEmit(3 * time.Second) {
		t.Fatal("the schedule stopped firing after overlap=skip degraded to allow")
	}
}

// TestActivateRefusesAnOverlapValueItCannotEnforce pins the two definitions that
// must not activate: a policy name that does not exist, and the one that is a
// workflow setting rather than a trigger parameter.
func TestActivateRefusesAnOverlapValueItCannotEnforce(t *testing.T) {
	cases := map[string]string{
		"a typo":  "skipp",
		"queue":   "settings.concurrency",
		"a value": "3",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New().Activate(context.Background(), &types.TriggerActivateInput{
				WorkflowID: "wf-1",
				NodeName:   "nightly",
				Params:     map[string]any{"expression": "*/1 * * * *", "overlap": value},
				Runtime:    triggertest.NewFakeRuntime(),
			})
			if err == nil {
				t.Fatalf("Activate(overlap=%q) succeeded; want a refusal naming the accepted values", value)
			}
			want := "overlap must be one of"
			if value == "queue" {
				want = "settings.concurrency"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Activate(overlap=%q) error = %v, want it to contain %q", value, err, want)
			}
		})
	}
}

// stubStatusReader is the capability under a test's control, including the
// unreadable case a real store produces when it is unreachable.
type stubStatusReader struct {
	status types.ExecutionStatus
	found  bool
	err    error
}

func (s *stubStatusReader) GetExecutionStatus(context.Context, types.ExecutionID) (types.ExecutionStatus, bool, error) {
	return s.status, s.found, s.err
}
