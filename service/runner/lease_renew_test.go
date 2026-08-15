package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

type fakeRenewer struct {
	renewed atomic.Int32
	fail    atomic.Bool
	reject  atomic.Bool
}

func (f *fakeRenewer) RenewLease(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	if f.fail.Load() {
		return false, errors.New("transport error")
	}
	if f.reject.Load() {
		return false, nil
	}
	f.renewed.Add(1)
	return true, nil
}

func TestRenewGroupLease_RenewsSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	renewer := &fakeRenewer{}
	lease := &engine.TaskLease{LeaseID: "l1", LeaseToken: "t1"}

	// Use a very short interval for testing.
	cfg := RenewalConfig{Interval: 10 * time.Millisecond, MaxRetries: 3}

	go renewGroupLease(ctx, renewer, lease, time.Second, cfg, cancel)

	time.Sleep(50 * time.Millisecond)
	cancel()

	if renewer.renewed.Load() < 2 {
		t.Fatalf("renewed %d times, want >= 2", renewer.renewed.Load())
	}
}

func TestRenewGroupLease_CancelsOnRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	renewer := &fakeRenewer{}
	renewer.reject.Store(true)
	lease := &engine.TaskLease{LeaseID: "l2", LeaseToken: "t2"}

	cfg := RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 3}
	done := make(chan struct{})
	go func() {
		renewGroupLease(ctx, renewer, lease, time.Second, cfg, cancel)
		close(done)
	}()

	select {
	case <-done:
		// Good — renewal loop exited.
	case <-time.After(time.Second):
		t.Fatal("renewal loop did not exit after rejection")
	}

	// Context should be canceled.
	if ctx.Err() == nil {
		t.Fatal("context not canceled after rejection")
	}
}

func TestRenewGroupLease_CancelsAfterMaxRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	renewer := &fakeRenewer{}
	renewer.fail.Store(true)
	lease := &engine.TaskLease{LeaseID: "l3", LeaseToken: "t3"}

	cfg := RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 2}
	done := make(chan struct{})
	go func() {
		renewGroupLease(ctx, renewer, lease, time.Second, cfg, cancel)
		close(done)
	}()

	select {
	case <-done:
		// Good — renewal loop exited after retries exhausted.
	case <-time.After(time.Second):
		t.Fatal("renewal loop did not exit after max retries")
	}

	if ctx.Err() == nil {
		t.Fatal("context not canceled after max retries")
	}
}

func TestRenewGroupLease_ExitsOnContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	renewer := &fakeRenewer{}
	lease := &engine.TaskLease{LeaseID: "l4", LeaseToken: "t4"}

	cfg := RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 3}
	done := make(chan struct{})
	go func() {
		renewGroupLease(ctx, renewer, lease, time.Second, cfg, cancel)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// Good.
	case <-time.After(time.Second):
		t.Fatal("renewal loop did not exit on context cancel")
	}
}

func TestDefaultRenewalInterval(t *testing.T) {
	tests := []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{30 * time.Second, 10 * time.Second},
		{9 * time.Second, 3 * time.Second},
		{60 * time.Second, 10 * time.Second},
	}
	for _, tc := range tests {
		got := defaultRenewalInterval(tc.ttl)
		if got != tc.want {
			t.Errorf("defaultRenewalInterval(%v) = %v, want %v", tc.ttl, got, tc.want)
		}
	}
}
