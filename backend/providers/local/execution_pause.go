package local

import (
	"context"
	"fmt"

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
func (s *memoryState) PauseExecution(_ context.Context, id types.ExecutionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok {
		return fmt.Errorf("pause execution %q: %w", id, engine.ErrExecutionNotPausable)
	}
	switch entry.snap.Status {
	case types.ExecutionStatusPaused:
		// A repeat is the ordinary shape of a lost response, not a conflict.
		return nil
	case types.ExecutionStatusRunning:
		entry.snap.Status = types.ExecutionStatusPaused
		return nil
	default:
		// A queued execution holds no state to suspend and a terminal one is
		// over; "paused" would be a lie about either.
		return fmt.Errorf("pause execution %q in status %q: %w", id, entry.snap.Status, engine.ErrExecutionNotPausable)
	}
}

// ResumeExecution returns a paused execution to dispatch.
//
// It is a status write and nothing else, because the pause was too: the ready
// set was never moved, so the work that was held is due again the instant the
// status changes. What it cannot do is deliver it — that is FlushOutbox's job,
// and the engine calls it after this returns.
func (s *memoryState) ResumeExecution(_ context.Context, id types.ExecutionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.executions[id]
	if !ok || entry.snap.Status != types.ExecutionStatusPaused {
		// Covers the execution that finished or was cancelled while paused:
		// there is nothing to resume, and reporting success would promise work
		// that will never run.
		return fmt.Errorf("resume execution %q: %w", id, engine.ErrExecutionNotPaused)
	}
	entry.snap.Status = types.ExecutionStatusRunning
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
