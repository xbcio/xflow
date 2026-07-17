package engine

import (
	"context"

	"github.com/xbcio/xflow/types"
)

// DeadLetterReplayOutcome classifies the result of a dead-letter replay
// attempt. Backends return one of these so callers can record audit/metric
// outcomes without inspecting error text.
type DeadLetterReplayOutcome string

const (
	// ReplayReplayed means the entry was moved atomically from dead-letter
	// storage back to the ready set and will be redelivered by the next
	// OutboxDispatcher scan. Its delivery attempt counter was reset to zero.
	ReplayReplayed DeadLetterReplayOutcome = "replayed"
	// ReplayNotFound means the entry was not present in dead-letter storage.
	// This is a stable idempotent no-op: concurrent replays of the same entry
	// collapse to exactly one ReplayReplayed and the rest return ReplayNotFound.
	ReplayNotFound DeadLetterReplayOutcome = "not_found"
	// ReplayRejectedTerminal means the execution is already in a terminal
	// state (success/failed/canceled/timeout); replaying would only produce a
	// stale intent, so the backend rejects it.
	ReplayRejectedTerminal DeadLetterReplayOutcome = "rejected_terminal"
	// ReplayRejectedInactive means the execution has expired or is otherwise
	// inactive (its status key is gone); replaying is rejected.
	ReplayRejectedInactive DeadLetterReplayOutcome = "rejected_inactive"
)

// DeadLetterStore is an optional StateStore capability that exposes
// dead-lettered durable outbox entries for operational inspection and safe
// replay. Replay moves an entry atomically from dead-letter storage back to
// the ready set so the OutboxDispatcher can redeliver it.
//
// Implementations must guarantee:
//   - Concurrent replays of the same entry are idempotent: exactly one
//     returns ReplayReplayed, the rest return ReplayNotFound.
//   - Replay is rejected (or is a stable no-op) when the execution is
//     terminal, canceled, expired, or otherwise inactive.
//   - The original immutable body is preserved across the move.
//   - The delivery attempt counter is reset to zero on replay so the entry
//     is not immediately re-dead-lettered.
//
// CLI/admin tooling must go through this capability rather than constructing
// Redis keys directly, so the atomic contract is owned by the backend.
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, id types.ExecutionID, limit int) ([]OutboxEntry, error)
	ReplayDeadLetter(ctx context.Context, id types.ExecutionID, entryID string) (DeadLetterReplayOutcome, error)
}

// ListDeadLetters returns up to limit dead-lettered outbox entries for an
// execution. It returns ErrDeadLetterUnsupported when the StateStore does not
// implement DeadLetterStore.
func (e *Engine) ListDeadLetters(ctx context.Context, id types.ExecutionID, limit int) ([]OutboxEntry, error) {
	state, err := e.atomicState()
	if err != nil {
		return nil, err
	}
	store, ok := state.(DeadLetterStore)
	if !ok {
		return nil, ErrDeadLetterUnsupported
	}
	return store.ListDeadLetters(ctx, id, limit)
}

// ReplayDeadLetter moves a dead-lettered entry back to the ready set via the
// DeadLetterStore capability and notifies observers of the outcome. It is the
// programmatic entry point for admin/API replay; the CLI wraps it to record an
// audit trail (operator/reason/entry/outcome).
func (e *Engine) ReplayDeadLetter(ctx context.Context, id types.ExecutionID, entryID string) (DeadLetterReplayOutcome, error) {
	state, err := e.atomicState()
	if err != nil {
		return "", err
	}
	store, ok := state.(DeadLetterStore)
	if !ok {
		return "", ErrDeadLetterUnsupported
	}
	outcome, err := store.ReplayDeadLetter(ctx, id, entryID)
	if err != nil {
		e.notifyOutboxError(ctx, "replay", err)
		return "", err
	}
	e.notifyOutboxReplayed(ctx, outcome)
	return outcome, nil
}

// ErrDeadLetterUnsupported is returned when the configured StateStore does not
// implement DeadLetterStore (e.g. a minimal or embedded store without
// dead-letter storage). Callers should surface it as a configuration error
// rather than retrying.
var ErrDeadLetterUnsupported = errDeadLetterUnsupported{}

type errDeadLetterUnsupported struct{}

func (errDeadLetterUnsupported) Error() string { return "engine: StateStore does not implement DeadLetterStore" }
func (errDeadLetterUnsupported) Is(target error) bool {
	_, ok := target.(errDeadLetterUnsupported)
	return ok
}
