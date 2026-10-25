package local

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

var _ engine.ExecutionPauser = (*memoryState)(nil)

// PauseExecution suspends dispatch of a running execution.
//
// The hold itself is not written anywhere: it IS the status. Every read of the
// ready set goes through readyOutboxIDsLocked, which refuses to name an intent
// while the execution is paused, so a single status write suspends the whole
// ready set — including the intents a task already in flight appends after the
// pause landed, which is the case a "park the intents I can see" implementation
// would miss.
//
// Nothing about the intents is touched, which is what makes the resume exact: a
// delayed intent keeps the deadline it was written with, rather than being
// released to "now" by a resume that had to reconstruct it. See
// engine.ExecutionPauser for why the enforcement point is the store rather than
// the engine.
func (s *memoryState) PauseExecution(ctx context.Context, id types.ExecutionID) error {
	var (
		err     error
		project bool
	)
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		entry, ok := s.executions[id]
		if !ok {
			err = fmt.Errorf("pause execution %q: %w", id, engine.ErrExecutionNotPausable)
			return
		}
		switch entry.snap.Status {
		case types.ExecutionStatusPaused:
			// A repeat is the ordinary shape of a lost response, not a conflict.
			// rstate projects the already-paused answer as well: the operator's
			// listing must read paused whether this call applied the transition or
			// found it already applied.
			project = true
		case types.ExecutionStatusRunning:
			entry.snap.Status = types.ExecutionStatusPaused
			entry.snap.UpdatedAt = time.Now().UTC()
			project = true
		default:
			// A queued execution holds no state to suspend and a terminal one is
			// over; "paused" would be a lie about either.
			err = fmt.Errorf("pause execution %q in status %q: %w", id, entry.snap.Status, engine.ErrExecutionNotPausable)
		}
	}()
	if err != nil {
		return err
	}
	if project {
		s.mirrorExecutionStatus(ctx, id)
	}
	return nil
}

// ResumeExecution returns a paused execution to dispatch.
//
// It is a status write and nothing else, because the pause was too: the ready
// set was never moved, so the work that was held is due again the instant the
// status changes. What it cannot do is deliver it — that is FlushOutbox's job,
// and the engine calls it after this returns.
func (s *memoryState) ResumeExecution(ctx context.Context, id types.ExecutionID) error {
	var err error
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		entry, ok := s.executions[id]
		if !ok || entry.snap.Status != types.ExecutionStatusPaused {
			// Covers the execution that finished or was cancelled while paused:
			// there is nothing to resume, and reporting success would promise work
			// that will never run.
			err = fmt.Errorf("resume execution %q: %w", id, engine.ErrExecutionNotPaused)
			return
		}
		entry.snap.Status = types.ExecutionStatusRunning
		entry.snap.UpdatedAt = time.Now().UTC()
	}()
	if err != nil {
		return err
	}

	s.mirrorExecutionStatus(ctx, id)
	return nil
}

// dispatchHeldLocked reports whether the execution's ready set is held by a
// pause.
//
// The read paths ask this instead of inspecting the status themselves so that a
// paused execution has exactly one meaning in this file, and so the two
// dispatch reads cannot drift apart on it: readyOutboxIDsLocked is shared by
// ListOutbox and LeaseOutbox, and claimOutbox falls back from one to the other.
func (s *memoryState) dispatchHeldLocked(id types.ExecutionID) bool {
	entry := s.executions[id]
	return entry != nil && entry.snap.Status == types.ExecutionStatusPaused
}
