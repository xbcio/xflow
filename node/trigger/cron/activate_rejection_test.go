package cron

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestActivateRefusesAScheduleItCannotRun pins the three ways cron.Activate can
// refuse a definition — cron.go:63-65, :70-73 and :76-84 — none of which any
// test in this package drives. Every existing Activate call passes a schedule
// the parser accepts ("@every 1s" in cron_test.go:26, "* * * * *" in
// dedup_window_test.go), so all three error returns are free to delete.
//
// The one that matters most is the third. cronlib.AddFunc is where a
// mistyped expression is caught, and there is nothing after it: with that
// error swallowed, Activate calls c.Start() on a scheduler holding zero
// entries and hands back a perfectly ordinary subscription. The activation
// reconciler records the trigger as hosted, the runner reports healthy, and
// the workflow simply never fires. There is no error, no log line and no
// metric that distinguishes it from a schedule that legitimately has not come
// due yet — which for a nightly job means the first evidence is the report
// nobody received the next morning.
//
// The subscription is closed on the "should not happen" path rather than
// leaked, so a mutation that starts a scheduler also has its goroutine
// stopped and does not bleed into the rest of the package.
//
// How each of the three guards actually behaves when removed, measured rather
// than assumed, because one of the three is weaker than it looks:
//
//   - The empty-expression guard (cron.go:63-65) is NOT what rejects an empty
//     expression. cronlib rejects it anyway, with "empty spec string". All this
//     guard decides is the wording, so the only teeth this test has on that case
//     are the wantIn check — and that is what goes red, not the "returned no
//     error" check above it. Recorded rather than dressed up: this case pins a
//     message, not a behaviour.
//   - The timezone guard (cron.go:70-73) is worse than a degradation. With the
//     error swallowed, loc is nil, cronlib.WithLocation(nil) is accepted, and
//     c.Start() spawns a goroutine that immediately panics at cron.go:318 on
//     time.Now().In(nil). That panic is on cronlib's goroutine, not the caller's,
//     so nothing can recover it and the assertion below never gets to report —
//     the run dies with a bare panic and no FAIL line. A workflow with a typo'd
//     timezone does not fail to activate; it takes the runner down.
//   - The AddFunc guard (cron.go:76-84) is the one that fails cleanly, and the
//     one whose absence is silent in production: no panic, no error, just a
//     scheduler holding zero entries.
func TestActivateRefusesAScheduleItCannotRun(t *testing.T) {
	cases := []struct {
		name       string
		expression string
		timezone   string
		wantIn     string
	}{
		{
			name:       "empty expression",
			expression: "",
			timezone:   "UTC",
			wantIn:     "expression",
		},
		{
			name:       "timezone that does not exist",
			expression: "* * * * *",
			timezone:   "Mars/Olympus_Mons",
			wantIn:     "Mars/Olympus_Mons",
		},
		{
			// Five fields, so it looks like a cron expression, but 61 is out of
			// range for minutes. This is the shape of a real typo rather than
			// obvious garbage.
			name:       "expression the parser rejects",
			expression: "61 * * * *",
			timezone:   "UTC",
			wantIn:     "61",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := triggertest.NewFakeRuntime()
			tr := New().Cron(tc.expression).InTimezone(tc.timezone)

			sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
				WorkflowID: "wf-1",
				NodeName:   "cron",
				Params:     tr.RawParams().(map[string]any),
				Runtime:    rt,
			})
			if sub != nil {
				_ = sub.Close(context.Background())
			}
			if err == nil {
				t.Fatalf("Activate(expression=%q, timezone=%q) returned no error: the "+
					"trigger is now recorded as hosted and will never fire, which is "+
					"indistinguishable from a schedule that has not come due",
					tc.expression, tc.timezone)
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("Activate() error = %v, want it to name %q: the operator has to "+
					"be able to tell which part of the definition was rejected",
					err, tc.wantIn)
			}
		})
	}

	// The other direction, so a mutation that rejects every definition cannot
	// pass this test either.
	t.Run("a valid schedule still activates", func(t *testing.T) {
		rt := triggertest.NewFakeRuntime()
		tr := New().Cron("* * * * *").InTimezone("UTC")
		sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "cron",
			Params:     tr.RawParams().(map[string]any),
			Runtime:    rt,
		})
		if err != nil {
			t.Fatalf("Activate() with a valid schedule error = %v, want nil", err)
		}
		if sub == nil {
			t.Fatal("Activate() returned a nil subscription with a nil error")
		}
		if err := sub.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}
