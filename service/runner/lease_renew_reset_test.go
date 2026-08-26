package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// scriptedRenewer answers a fixed sequence of outcomes and then succeeds
// forever, closing done once the whole script has been consumed. The existing
// fakeRenewer cannot express this: its fail/reject flags are set once before
// the loop starts and never flipped, so every existing test drives a loop whose
// every round has the same outcome.
type scriptedRenewer struct {
	mu     sync.Mutex
	steps  []error // nil means "renewed"
	next   int
	done   chan struct{}
	closed bool
}

func (r *scriptedRenewer) RenewLease(context.Context, string, string, time.Duration) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next >= len(r.steps) {
		return true, nil
	}
	err := r.steps[r.next]
	r.next++
	if r.next == len(r.steps) && !r.closed {
		close(r.done)
		r.closed = true
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *scriptedRenewer) consumed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

// TestRenewLeaseLoopForgivesErrorsThatAreNotConsecutive pins
// lease_renew.go:119, `consecutiveErrors = 0`.
//
// renewLeaseLoop's own doc comment states the contract as "MaxRetries
// consecutive transport errors", and that one line is the entire difference
// between consecutive and cumulative. Delete it and the counter only ever
// climbs, so with the default MaxRetries=3 the *third transport error in the
// lease's whole life* cancels the handler — no matter how many successful
// renewals sat between them, and no matter how many minutes apart they were.
//
// What that cancel does is not cosmetic. It is the same terminal action as a
// genuine fence (`renewed == false`): the loop calls cancel() and returns, the
// handler's context dies mid-execution, and renewal for that lease stops for
// good. A long-running handler on a flaky network gets killed as if another
// attempt had taken its node, and the runner then reports a failure for work
// that was proceeding normally. On a link with occasional blips, "occasional"
// becomes fatal at the third blip.
//
// No existing test can see it. fakeRenewer's `fail` and `reject` are atomics
// set once before the loop starts and never flipped, and renewCapturingClient
// in runner_renew_wiring_test.go has the same shape with a single `refuse`
// bool. So every renewal loop in the suite is all-success, all-fail or
// all-reject; none of them alternates, which is the only sequence in which
// resetting and not resetting differ.
//
// The script here is three errors separated by successes: never three in a row,
// so a correct loop must never cancel. The assertion is a positive wait on the
// script being fully consumed rather than a sleep-then-peek — a loop that
// cancels early stops calling the renewer, so the failure shows up as "the
// context died before the script finished" with the exact step it died on,
// instead of as a timeout.
func TestRenewLeaseLoopForgivesErrorsThatAreNotConsecutive(t *testing.T) {
	boom := errors.New("transport error")
	r := &scriptedRenewer{
		steps: []error{boom, nil, boom, nil, boom, nil},
		done:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease := &engine.TaskLease{LeaseID: "l1", LeaseToken: "t1"}
	// MaxRetries is the production default, spelled out so the script's three
	// errors are exactly enough to trip a cumulative counter and never enough
	// to trip a consecutive one.
	cfg := RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 3}

	go renewLeaseLoop(ctx, r, lease, time.Second, cfg, cancel)

	select {
	case <-r.done:
	case <-ctx.Done():
		t.Fatalf("the loop cancelled the handler after %d of %d renewals; the "+
			"three errors in this script are never consecutive, so a loop that "+
			"resets its counter on success must not reach MaxRetries=%d — "+
			"cancelling here kills a healthy handler exactly as if its lease "+
			"had been fenced", r.consumed(), len(r.steps), cfg.MaxRetries)
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d of %d scripted renewals happened in 5s at a %v "+
			"interval; the loop is not running", r.consumed(), len(r.steps), cfg.Interval)
	}

	// The script ended on a success, and the loop succeeds forever afterwards,
	// so nothing may have cancelled by now.
	if err := ctx.Err(); err != nil {
		t.Fatalf("context ended with %v after a fully-consumed script whose "+
			"errors were never consecutive", err)
	}
	if got := r.consumed(); got != len(r.steps) {
		t.Fatalf("consumed %d steps, want %d", got, len(r.steps))
	}
}
