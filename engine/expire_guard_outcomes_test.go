package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// outcomeCase is one answer ExpireExecution can give a commit guard, and the
// store edit that makes it give that answer. The edit runs once, after the
// guard's own read of the execution deadline and before ExpireExecution reads
// the execution, so the guard sees a reached deadline and the expiry then finds
// the execution in the state under test. Editing the store before the commit
// would not do: a commit that finds the execution gone or concluded stops at its
// own activeness check and never reaches the guard.
type outcomeCase struct {
	name string
	want ExpireOutcome
	// handled reports whether the guard must settle the commit (true) or hand
	// it back to the ordinary on_error / fatal path (false).
	handled bool
	// wantStatus is the stored execution status after the commit, "" meaning
	// the execution is gone. Only read for handled cases.
	wantStatus types.ExecutionStatus
	flip       func(f *fakeState, id types.ExecutionID)
}

func editExecution(f *fakeState, id types.ExecutionID, edit func(*ExecutionSnapshot)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if snap := f.executions[id]; snap != nil {
		edit(snap)
	}
}

var outcomeCases = []outcomeCase{
	{name: "expired", want: ExpireOutcomeExpired, handled: true, wantStatus: types.ExecutionStatusTimeout},
	{
		name: "resumed", want: ExpireOutcomeResumed, handled: true, wantStatus: types.ExecutionStatusTimeout,
		flip: func(f *fakeState, id types.ExecutionID) {
			editExecution(f, id, func(s *ExecutionSnapshot) {
				s.Status = types.ExecutionStatusTimeout
				s.Error = ExecutionDeadlineExceededError
			})
		},
	},
	{
		name: "already terminal", want: ExpireOutcomeAlreadyTerminal, handled: true, wantStatus: types.ExecutionStatusSuccess,
		flip: func(f *fakeState, id types.ExecutionID) {
			editExecution(f, id, func(s *ExecutionSnapshot) { s.Status = types.ExecutionStatusSuccess })
		},
	},
	{
		name: "canceling", want: ExpireOutcomeCanceling, handled: true, wantStatus: types.ExecutionStatusCanceling,
		flip: func(f *fakeState, id types.ExecutionID) {
			editExecution(f, id, func(s *ExecutionSnapshot) { s.Status = types.ExecutionStatusCanceling })
		},
	},
	{
		name: "gone", want: ExpireOutcomeGone, handled: true,
		flip: func(f *fakeState, id types.ExecutionID) {
			f.mu.Lock()
			delete(f.executions, id)
			f.mu.Unlock()
		},
	},
	{
		name: "not due", want: ExpireOutcomeNotDue, handled: false, wantStatus: types.ExecutionStatusRunning,
		flip: func(f *fakeState, id types.ExecutionID) {
			editExecution(f, id, func(s *ExecutionSnapshot) { s.Deadline = time.Now().Add(time.Hour) })
		},
	},
}

// forgetExecutionDeadline drops the engine's memo of the execution deadline, so
// the guard's next read goes to the store and the edit armed on that read fires.
func forgetExecutionDeadline(e *Engine, id types.ExecutionID) {
	e.mu.Lock()
	delete(e.executionDeadlines, id)
	e.mu.Unlock()
}

// outcomeEntry is one node-path entry point. outcome is what the entry answers,
// or "" for the ones that answer only an error.
type outcomeEntry struct {
	name string
	run  func(t *testing.T, r *guardRig) CommitOutcome
}

var outcomeEntries = []outcomeEntry{
	{"CommitTaskTimeout", func(t *testing.T, r *guardRig) CommitOutcome {
		t.Helper()
		if err := r.eng.CommitTaskTimeout(context.Background(), r.lease, nil); err != nil {
			t.Fatalf("CommitTaskTimeout() error = %v", err)
		}
		return ""
	}},
	{"CommitTaskResultWithOutcome", func(t *testing.T, r *guardRig) CommitOutcome {
		t.Helper()
		result := TaskResult{Error: types.NewPermanentError(types.NodeTimeoutErrorCode, "node execution exceeded its 1s timeout")}
		outcome, err := r.eng.CommitTaskResultWithOutcome(context.Background(), r.lease, result)
		if err != nil {
			t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
		}
		return outcome
	}},
}

// TestEveryOutcomeTheCommitGuardSeesIsSettledOrHandedBack pins what a node
// timeout past the execution deadline does for each answer ExpireExecution can
// give. Every answer but not-due leaves the commit with nothing to do: the node
// is never committed and nothing downstream is scheduled, even though on_error
// is continue and would otherwise carry the execution on. Not-due is the one
// answer that hands the commit back, and on_error then decides as before. The
// runner-reporting entry must also answer execution_inactive for a settled
// commit, which its caller releases the lease on.
func TestEveryOutcomeTheCommitGuardSeesIsSettledOrHandedBack(t *testing.T) {
	for _, entry := range outcomeEntries {
		for _, tc := range outcomeCases {
			t.Run(entry.name+"/"+tc.name, func(t *testing.T) {
				rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(-time.Minute))
				forgetExecutionDeadline(rig.eng, rig.id)
				if tc.flip != nil {
					flip := tc.flip
					rig.state.mu.Lock()
					rig.state.afterRead = func() { flip(rig.state.fakeState, rig.id) }
					rig.state.mu.Unlock()
				}

				got := entry.run(t, rig)

				if !tc.handled {
					requireOnErrorApplied(t, rig, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
					if entry.name == "CommitTaskResultWithOutcome" && got != CommitOutcomeAccepted {
						t.Fatalf("outcome = %q, want %q: the guard handed the commit back", got, CommitOutcomeAccepted)
					}
					return
				}
				if entry.name == "CommitTaskResultWithOutcome" && got != CommitOutcomeExecutionInactive {
					t.Fatalf("outcome = %q, want %q: a settled commit is answered as inactive", got, CommitOutcomeExecutionInactive)
				}
				if n := rig.state.nodeCommitCount(); n != 0 {
					t.Fatalf("node commits reaching the store = %d, want 0: the node must not be committed", n)
				}
				if names := taskNames(rig.queue.Drain()); len(names) != 0 {
					t.Fatalf("downstream enqueued %v, want nothing", names)
				}
				snap, err := rig.state.GetExecution(context.Background(), rig.id)
				if err != nil {
					t.Fatalf("GetExecution() error = %v", err)
				}
				switch {
				case tc.wantStatus == "" && snap != nil:
					t.Fatalf("execution status = %q, want the execution gone", snap.Status)
				case tc.wantStatus != "" && (snap == nil || snap.Status != tc.wantStatus):
					t.Fatalf("execution = %+v, want status %q", snap, tc.wantStatus)
				}
			})
		}
	}
}

// TestOutcomeCasesYieldTheirOutcome keeps the table above honest: each store
// edit must make ExpireExecution itself answer the outcome the case is named
// for, or the commit-level assertions would be pinning some other answer.
func TestOutcomeCasesYieldTheirOutcome(t *testing.T) {
	for _, tc := range outcomeCases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newGuardRig(t, guardSpec{onError: "continue", bounded: true}, time.Now().Add(-time.Minute))
			if tc.flip != nil {
				tc.flip(rig.state.fakeState, rig.id)
			}
			got, err := rig.eng.ExpireExecution(context.Background(), rig.id)
			if err != nil {
				t.Fatalf("ExpireExecution() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("ExpireExecution() = %q, want %q", got, tc.want)
			}
		})
	}
}

// groupFlipState is the group deadline fake with a one-shot hook that runs after
// the Nth GetExecution call since it was armed. The snapshot of that call is a
// copy taken before the hook runs, because fakeState hands out its stored
// pointer and the hook edits what it points at.
type groupFlipState struct {
	*groupReasonState

	mu    sync.Mutex
	reads int
	after int
	hook  func()
}

func (s *groupFlipState) arm(after int, hook func()) {
	s.mu.Lock()
	s.reads, s.after, s.hook = 0, after, hook
	s.mu.Unlock()
}

func (s *groupFlipState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	snap, err := s.groupReasonState.GetExecution(ctx, id)
	if snap != nil {
		cp := *snap
		snap = &cp
	}
	s.mu.Lock()
	s.reads++
	var hook func()
	if s.hook != nil && s.reads == s.after {
		hook, s.hook = s.hook, nil
	}
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return snap, err
}

// groupFlipRig is groupGuardRig over a store that can run an edit between the
// guard's deadline read and ExpireExecution's read.
func groupFlipRig(t *testing.T) (*Engine, *groupFlipState, *TaskLease) {
	t.Helper()
	_, state, task := groupClampRig(t, true, 0, time.Now().Add(-time.Minute))
	g := state.fakeState.executions[task.ExecutionID].Graph
	if g == nil {
		t.Fatal("the stored execution carries no graph")
	}
	flip := &groupFlipState{groupReasonState: &groupReasonState{groupDeadlineState: state}}
	eng := New(flip, &fakeQueue{})
	eng.cacheExecutionGraph(task.ExecutionID, g)
	lease, _, err := eng.BuildGroupLease(context.Background(), task)
	if err != nil {
		t.Fatalf("BuildGroupLease() error = %v", err)
	}
	return eng, flip, lease
}

// TestEveryOutcomeTheGroupGuardSeesIsSettledOrHandedBack is the group twin of
// the node test above: every answer but not-due settles the commit as
// execution_inactive without committing the group, and not-due leaves the group
// to its fatal commit.
func TestEveryOutcomeTheGroupGuardSeesIsSettledOrHandedBack(t *testing.T) {
	for _, tc := range outcomeCases {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, lease := groupFlipRig(t)
			id := lease.Task.ExecutionID
			forgetExecutionDeadline(eng, id)
			if tc.flip != nil {
				flip := tc.flip
				// The store has no narrow status read, so the activeness check
				// is the first GetExecution and the guard's deadline read the
				// second: the edit lands before ExpireExecution's own read.
				state.arm(2, func() { flip(state.fakeState, id) })
			}

			got, err := eng.CommitGroupResult(context.Background(), lease, GroupResult{Outcome: GroupOutcomeTimeout, Error: "deadline exceeded"})
			if err != nil {
				t.Fatalf("CommitGroupResult() error = %v", err)
			}

			if !tc.handled {
				if got != CommitOutcomeAccepted || !state.committed || !state.lastCommit.Fatal {
					t.Fatalf("outcome = %q, committed = %v, fatal = %v, want an accepted fatal group commit",
						got, state.committed, state.lastCommit.Fatal)
				}
				return
			}
			if got != CommitOutcomeExecutionInactive {
				t.Fatalf("outcome = %q, want %q: a settled commit is answered as inactive", got, CommitOutcomeExecutionInactive)
			}
			if state.committed {
				t.Fatal("the group was committed: a settled commit must not take a group commit")
			}
			snap, err := state.fakeState.GetExecution(context.Background(), id)
			if err != nil {
				t.Fatalf("GetExecution() error = %v", err)
			}
			switch {
			case tc.wantStatus == "" && snap != nil:
				t.Fatalf("execution status = %q, want the execution gone", snap.Status)
			case tc.wantStatus != "" && (snap == nil || snap.Status != tc.wantStatus):
				t.Fatalf("execution = %+v, want status %q", snap, tc.wantStatus)
			}
		})
	}
}

// TestGroupOutcomeCasesYieldTheirOutcome is TestOutcomeCasesYieldTheirOutcome
// over the group fake, whose store is not the node tests' one.
func TestGroupOutcomeCasesYieldTheirOutcome(t *testing.T) {
	for _, tc := range outcomeCases {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, lease := groupFlipRig(t)
			id := lease.Task.ExecutionID
			if tc.flip != nil {
				tc.flip(state.fakeState, id)
			}
			got, err := eng.ExpireExecution(context.Background(), id)
			if err != nil {
				t.Fatalf("ExpireExecution() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("ExpireExecution() = %q, want %q", got, tc.want)
			}
		})
	}
}
