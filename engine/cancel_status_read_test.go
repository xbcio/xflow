package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
)

// cancelStatusState is fakeState plus ExecutionStatusReader, with two
// deliberately inconsistent reads so a test can tell which one
// isCancelingOrCanceled used. The snapshot read never answers a cancellation
// status — it only counts calls, and fails when configured — while
// GetExecutionStatus carries the answer the helper must act on.
type cancelStatusState struct {
	*fakeState
	status    types.ExecutionStatus
	found     bool
	statusErr error

	statusReads   int
	snapshotReads int
}

func (s *cancelStatusState) GetExecution(context.Context, types.ExecutionID) (*ExecutionSnapshot, error) {
	s.snapshotReads++
	return nil, nil
}

func (s *cancelStatusState) GetExecutionStatus(context.Context, types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.statusReads++
	return s.status, s.found, s.statusErr
}

// TestIsCancelingOrCanceledUsesNarrowStatusRead pins that the "did Cancel win"
// decision reads only the lifecycle status. The old implementation read the
// full execution snapshot; a regression to that is visible here because the
// snapshot read never reports canceling while GetExecutionStatus does.
//
// The error arm also pins the preservation decision: a failed status read is
// not evidence of cancellation, so the helper answers false exactly as the
// failed GetExecution did.
func TestIsCancelingOrCanceledUsesNarrowStatusRead(t *testing.T) {
	cases := []struct {
		name   string
		status types.ExecutionStatus
		found  bool
		err    error
		want   bool
	}{
		{name: "canceling", status: types.ExecutionStatusCanceling, found: true, want: true},
		{name: "canceled", status: types.ExecutionStatusCanceled, found: true, want: true},
		{name: "success", status: types.ExecutionStatusSuccess, found: true, want: false},
		{name: "not found", want: false},
		{name: "read error", status: types.ExecutionStatusCanceling, found: true, err: errors.New("status read failed"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &cancelStatusState{
				fakeState: newFakeState(),
				status:    tc.status,
				found:     tc.found,
				statusErr: tc.err,
			}
			eng := New(state, &fakeQueue{})
			if got := eng.isCancelingOrCanceled(context.Background(), "exec-1"); got != tc.want {
				t.Fatalf("isCancelingOrCanceled() = %v, want %v", got, tc.want)
			}
			if state.statusReads == 0 {
				t.Error("GetExecutionStatus was never called: the helper fell back to the full snapshot read")
			}
			if state.snapshotReads != 0 {
				t.Errorf("GetExecution called %d times, want 0: cancel checking must use the narrow status read", state.snapshotReads)
			}
		})
	}
}

// TestIsCancelingOrCanceledFallbackBehaviour pins the behaviour when the
// backend predates ExecutionStatusReader: the GetExecution fallback answers
// the same questions, and an unreadable or absent execution is still "not
// canceling" so completeExecution keeps its completion notification and
// eviction — the exact behavior the old direct GetExecution read had.
func TestIsCancelingOrCanceledFallbackBehaviour(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		status types.ExecutionStatus
		create bool
		err    error
		want   bool
	}{
		{name: "canceling", status: types.ExecutionStatusCanceling, create: true, want: true},
		{name: "canceled", status: types.ExecutionStatusCanceled, create: true, want: true},
		{name: "success", status: types.ExecutionStatusSuccess, create: true, want: false},
		{name: "not found", want: false},
		{name: "read error", create: true, err: errors.New("snapshot read failed"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &snapshotFaultState{fakeState: newFakeState(), err: tc.err}
			if tc.create {
				if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: "exec-1", Status: tc.status}); err != nil {
					t.Fatalf("CreateExecution() error = %v", err)
				}
			}
			eng := New(state, &fakeQueue{})
			if got := eng.isCancelingOrCanceled(ctx, "exec-1"); got != tc.want {
				t.Fatalf("isCancelingOrCanceled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// snapshotFaultState is fakeState without ExecutionStatusReader, whose snapshot
// read can be made to fail. It keeps the fallback arm's error behavior pinned:
// nothing may implement both paths by accident.
type snapshotFaultState struct {
	*fakeState
	err error
}

func (s *snapshotFaultState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.fakeState.GetExecution(ctx, id)
}
