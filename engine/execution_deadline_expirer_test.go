package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// fakeDeadlineExpirer implements only the two methods of
// ExecutionDeadlineExpirer. It is deliberately not a StateStore: the capability
// is optional, so its method set must stand alone.
type fakeDeadlineExpirer struct {
	listed  []DeadlineExpiredExecution
	acked   []types.ExecutionID
	gotLim  int
	gotTime time.Time
}

var _ ExecutionDeadlineExpirer = (*fakeDeadlineExpirer)(nil)

func (f *fakeDeadlineExpirer) ListDeadlineExceededExecutions(_ context.Context, before time.Time, limit int) ([]DeadlineExpiredExecution, error) {
	f.gotTime, f.gotLim = before, limit
	return f.listed, nil
}

func (f *fakeDeadlineExpirer) AckDeadlineExpired(_ context.Context, id types.ExecutionID) error {
	f.acked = append(f.acked, id)
	return nil
}

// TestExecutionDeadlineExpirerShape pins the capability's wire-level shape: what
// a list entry carries and how the two methods are driven through the interface.
func TestExecutionDeadlineExpirerShape(t *testing.T) {
	deadline := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	fake := &fakeDeadlineExpirer{listed: []DeadlineExpiredExecution{{
		ExecutionID: "exec-1",
		Namespace:   namespace.Namespace("ns-a"),
		Deadline:    deadline,
	}}}
	var expirer ExecutionDeadlineExpirer = fake

	ctx := context.Background()
	before := deadline.Add(time.Second)
	got, err := expirer.ListDeadlineExceededExecutions(ctx, before, 7)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions: %v", err)
	}
	if fake.gotLim != 7 || !fake.gotTime.Equal(before) {
		t.Fatalf("arguments not passed through: limit=%d before=%v", fake.gotLim, fake.gotTime)
	}
	if len(got) != 1 || got[0].ExecutionID != "exec-1" || got[0].Namespace != "ns-a" || !got[0].Deadline.Equal(deadline) {
		t.Fatalf("entry = %+v", got)
	}

	// Ack is idempotent by contract; the interface must allow repeating it.
	for i := 0; i < 2; i++ {
		if err := expirer.AckDeadlineExpired(ctx, got[0].ExecutionID); err != nil {
			t.Fatalf("AckDeadlineExpired #%d: %v", i, err)
		}
	}
	if len(fake.acked) != 2 {
		t.Fatalf("acked = %v, want two calls", fake.acked)
	}
}

// TestExecutionDeadlineExceededErrorConstant pins the terminal reason text. It
// is observable outside the process (the execution snapshot, and the lease
// refusal a runner receives, which spells the same string), so a change here is
// a wire change.
func TestExecutionDeadlineExceededErrorConstant(t *testing.T) {
	if ExecutionDeadlineExceededError != "execution deadline exceeded" {
		t.Fatalf("ExecutionDeadlineExceededError = %q, want %q",
			ExecutionDeadlineExceededError, "execution deadline exceeded")
	}
	if ExecutionDeadlineExceededError == ConcurrencyQueueTimeoutError {
		t.Fatalf("the execution deadline reason must differ from the queue timeout reason %q",
			ConcurrencyQueueTimeoutError)
	}
}
