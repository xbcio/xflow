package timer

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestActivateRefusesANonPositiveInterval pins timer.go:54-57:
//
//	interval, err := conv.PositiveDuration(in.Params["interval"])
//	if err != nil {
//		return nil, err
//	}
//	ticker := time.NewTicker(interval)
//
// Every Activate in this package passes a valid interval — timer_test.go and
// dedup_window_test.go between them use "50ms", "80ms" and "1s" — so the error
// return has never been taken. conv.PositiveDuration itself is tested in
// node/internal/utils/conv, but nothing connects it to this call site.
//
// The connection is what matters, because the next line is
// time.NewTicker(interval) and time.NewTicker panics on any duration that is
// not positive. conv.PositiveDuration returns 0 for all four of the malformed
// shapes below, so without the check every one of them is a panic rather than a
// rejected activation. Activate runs on the runner's trigger activation path,
// which is not wrapped in a recover — the only three in the repository are
// service/runner's activation tracker and acker and engine/hooks.go, none of
// which covers this — so a single workflow definition with a typo'd interval
// takes the runner process down at activation time, along with every other
// trigger it was hosting.
//
// Stated so the result is not over-read: under the mutation that removes this
// check the failure arrives as a panic ("non-positive interval for NewTicker"),
// which aborts the package binary before the assertions below run. It is raised
// on the caller's own goroutine, so `go test` does still print a FAIL line for
// the first sub-test — but that FAIL is the panic's, not the assertion's. That
// proves the guard is load-bearing rather than proving the assertions have
// independent teeth. What the assertions do carry
// on their own is the set: all four shapes must be refused, and a valid one
// must still be accepted, so neither "reject only the nil case" nor "reject
// everything" passes.
func TestActivateRefusesANonPositiveInterval(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{name: "missing", value: nil},
		{name: "empty string", value: ""},
		{name: "zero", value: "0s"},
		{name: "negative", value: "-5s"},
		{name: "not a duration", value: "every friday"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := triggertest.NewFakeRuntime()
			params := map[string]any{}
			if tc.value != nil {
				params["interval"] = tc.value
			}

			sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
				WorkflowID: "wf-1",
				NodeName:   "timer",
				Params:     params,
				Runtime:    rt,
			})
			if sub != nil {
				_ = sub.Close(context.Background())
			}
			if err == nil {
				t.Fatalf("Activate(interval=%#v) returned no error: the value reaches "+
					"time.NewTicker, which panics on anything that is not positive, and "+
					"nothing on the runner's activation path recovers from it",
					tc.value)
			}
		})
	}

	// The other direction: a valid interval must still activate, so a mutation
	// that refuses every definition cannot pass either.
	t.Run("a valid interval still activates", func(t *testing.T) {
		rt := triggertest.NewFakeRuntime()
		sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "timer",
			Params:     map[string]any{"interval": "1s"},
			Runtime:    rt,
		})
		if err != nil {
			t.Fatalf("Activate() with a valid interval error = %v, want nil", err)
		}
		if sub == nil {
			t.Fatal("Activate() returned a nil subscription with a nil error")
		}
		if err := sub.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}
