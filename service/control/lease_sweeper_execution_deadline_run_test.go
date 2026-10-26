package control

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// TestLeaseSweeperRunDrivesTheExecutionDeadlinePass pins that Run reaches the
// deadline pass on both of its paths: once at startup, so a restarted control
// plane does not wait a period before it expires anything, and again on each
// tick. The index scan count is what shows it: 1 after startup, then one more
// per period the clock has moved.
func TestLeaseSweeperRunDrivesTheExecutionDeadlinePass(t *testing.T) {
	h := newDeadlineHarness(LeaseSweeperConfig{})
	var scans []int
	h.sleepFunc = func(context.Context, time.Duration) error {
		scans = append(scans, h.state.scanCalls())
		if len(scans) == 3 {
			return context.Canceled
		}
		*h.now = h.now.Add(DefaultSweepPeriod)
		return nil
	}

	h.Run(context.Background())

	if want := []int{1, 2, 3}; !reflect.DeepEqual(scans, want) {
		t.Fatalf("deadline index scans seen at each tick = %v, want %v: 1 at startup, one more per period", scans, want)
	}
}
