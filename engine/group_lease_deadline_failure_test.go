package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// groupDeadlineReadFailState makes the full-snapshot read fail while the
// activeness check and the entry-input read still succeed. groupDeadlineState
// has neither ExecutionStatusReader nor ExecutionInputContextReader, so the
// engine would answer both through GetExecution and the injected failure would
// surface from loadActiveGraph or buildInput before the execution deadline is
// ever asked for. The two narrow reads keep GetExecution to a single caller on
// these paths: executionDeadline.
type groupDeadlineReadFailState struct {
	*groupDeadlineState
	readErr     error
	failedReads atomic.Int64
}

func (s *groupDeadlineReadFailState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	if s.readErr != nil {
		s.failedReads.Add(1)
		return nil, s.readErr
	}
	return s.groupDeadlineState.GetExecution(ctx, id)
}

func (s *groupDeadlineReadFailState) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return "", false, err
	}
	return snap.Status, true, nil
}

func (s *groupDeadlineReadFailState) GetExecutionInputContext(ctx context.Context, id types.ExecutionID) (*ExecutionInputContext, bool, error) {
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return nil, false, err
	}
	return &ExecutionInputContext{
		Status:  snap.Status,
		Params:  snap.Params,
		Runtime: snap.Runtime,
		Scope:   snap.Scope,
		TraceID: snap.TraceID,
		SpanID:  snap.SpanID,
	}, true, nil
}

// requireDeadlineReadError checks the error shape a failed execution deadline
// read must have: the injected cause stays reachable and the entry point names
// itself, so an operator reading the log can tell which call gave up.
func requireDeadlineReadError(t *testing.T, err, cause error, prefix string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want one wrapping %v", cause)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want it to wrap %v", err, cause)
	}
	for _, want := range []string{prefix, "get execution"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err.Error(), want)
		}
	}
}

// TestGroupLeaseDeadlineReadFailure pins what a failed execution deadline read
// leaves behind. Only a graph declaring settings.timeout reads the deadline, and
// BuildGroupLease reads it before AcquireGroupLease so that a failure cannot
// strand a lease nothing will release. The observation points below are the ones
// fakeGroupLeaseState and groupDeadlineState really provide: acquired, committed,
// leaseID/token and renewCalls.
func TestGroupLeaseDeadlineReadFailure(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("execution store unavailable")

	t.Run("build fails before the lease is acquired", func(t *testing.T) {
		_, state, task := groupClampRig(t, true, 45*time.Second, time.Time{})
		fail := &groupDeadlineReadFailState{groupDeadlineState: state, readErr: readErr}
		lease, payload, err := New(fail, &fakeQueue{}).BuildGroupLease(ctx, task)
		requireDeadlineReadError(t, err, readErr, "build group lease")
		if lease != nil || payload != nil {
			t.Fatalf("BuildGroupLease returned lease=%v payload=%v alongside the error", lease, payload)
		}
		if state.acquired {
			t.Fatal("AcquireGroupLease was called before the deadline read failed: the group lease is orphaned")
		}
		if got := fail.failedReads.Load(); got != 1 {
			t.Fatalf("failed snapshot reads = %d, want 1 (the deadline read)", got)
		}
	})

	t.Run("recover fails without writing", func(t *testing.T) {
		built, state, task := groupClampRig(t, true, 45*time.Second, time.Time{})
		lease, _, err := built.BuildGroupLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildGroupLease: %v", err)
		}
		state.issuedAt = lease.IssuedAt
		fail := &groupDeadlineReadFailState{groupDeadlineState: state, readErr: readErr}
		// A fresh engine has no deadline memo, so recovery must go to the store.
		taskLease, payload, err := New(fail, &fakeQueue{}).RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx)
		requireDeadlineReadError(t, err, readErr, "recover group lease")
		if taskLease != nil || payload != nil {
			t.Fatalf("RecoverGroupLease returned lease=%v payload=%v alongside the error", taskLease, payload)
		}
		if state.renewCalls != 0 {
			t.Fatalf("store renew calls = %d, want 0", state.renewCalls)
		}
		if state.committed {
			t.Fatal("recovery committed the group")
		}
		if state.leaseID != lease.LeaseID || state.token != lease.LeaseToken {
			t.Fatalf("stored lease = %v/%v, want the original %v/%v", state.leaseID, state.token, lease.LeaseID, lease.LeaseToken)
		}
		if got := fail.failedReads.Load(); got != 1 {
			t.Fatalf("failed snapshot reads = %d, want 1 (the deadline read)", got)
		}
	})

	t.Run("renew fails without touching the store", func(t *testing.T) {
		built, state, task := groupClampRig(t, true, 45*time.Second, time.Time{})
		lease, _, err := built.BuildGroupLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildGroupLease: %v", err)
		}
		fail := &groupDeadlineReadFailState{groupDeadlineState: state, readErr: readErr}
		renewed, err := New(fail, &fakeQueue{}).RenewGroupLease(ctx, lease, 5*time.Minute)
		requireDeadlineReadError(t, err, readErr, "renew group lease")
		if renewed {
			t.Fatal("RenewGroupLease reported the lease renewed")
		}
		if state.renewCalls != 0 {
			t.Fatalf("store renew calls = %d, want 0: the renewal went ahead without the execution deadline", state.renewCalls)
		}
		if got := fail.failedReads.Load(); got != 1 {
			t.Fatalf("failed snapshot reads = %d, want 1 (the deadline read)", got)
		}
	})

	// The same failing snapshot read, on a graph that does not declare
	// settings.timeout: none of the three entry points asks for the deadline, so
	// none of them notices.
	t.Run("a graph without settings.timeout never reads the deadline", func(t *testing.T) {
		_, state, task := groupClampRig(t, false, 45*time.Second, time.Time{})
		fail := &groupDeadlineReadFailState{groupDeadlineState: state, readErr: readErr}

		lease, _, err := New(fail, &fakeQueue{}).BuildGroupLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildGroupLease: %v", err)
		}
		if !state.acquired {
			t.Fatal("BuildGroupLease succeeded without acquiring the lease")
		}
		state.issuedAt = lease.IssuedAt
		if _, _, err := New(fail, &fakeQueue{}).RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx); err != nil {
			t.Fatalf("RecoverGroupLease: %v", err)
		}
		if renewed, err := New(fail, &fakeQueue{}).RenewGroupLease(ctx, lease, time.Minute); err != nil || !renewed {
			t.Fatalf("RenewGroupLease = %v, %v", renewed, err)
		}
		if got := fail.failedReads.Load(); got != 0 {
			t.Fatalf("failed snapshot reads = %d, want 0: the unbounded graph asked the store for a deadline", got)
		}
	})
}
