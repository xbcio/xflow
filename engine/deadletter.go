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
	// ReplayAlreadyReplayed means a prior replay for the same RequestID (or
	// the same entry under a different RequestID) already produced a receipt.
	// Retrying after a lost response returns this stable outcome together with
	// the original AuditID, so the caller can prove the operation happened
	// exactly once instead of degrading to an unprovable not_found.
	ReplayAlreadyReplayed DeadLetterReplayOutcome = "already_replayed"
	// ReplayNotFound means the entry was not present in dead-letter storage and
	// no prior receipt exists for the RequestID. This is a stable no-op.
	ReplayNotFound DeadLetterReplayOutcome = "not_found"
	// ReplayRejectedTerminal means the execution is already in a terminal
	// state (success/failed/canceled/timeout); replaying would only produce a
	// stale intent, so the backend rejects it.
	ReplayRejectedTerminal DeadLetterReplayOutcome = "rejected_terminal"
	// ReplayRejectedInactive means the execution has expired or is otherwise
	// inactive (its status key is gone); replaying is rejected.
	ReplayRejectedInactive DeadLetterReplayOutcome = "rejected_inactive"
	// ReplayRejectedNodeTerminal means the entry's node is already in a
	// terminal state (success/failed/skipped/canceled/continued); replaying a
	// stale activation would advance a node that has already moved on.
	ReplayRejectedNodeTerminal DeadLetterReplayOutcome = "rejected_node_terminal"
	// ReplayRejectedActivationMismatch means the entry's activation does not
	// match the node's current activation — a stale activation from a prior
	// cyclic re-entry. Replaying it would unsafely resurrect dead intent.
	ReplayRejectedActivationMismatch DeadLetterReplayOutcome = "rejected_activation_mismatch"
	// ReplayInvalidRequest means the request was malformed (missing required
	// fields, over-length reason, etc.). Produced by the manager layer before
	// touching Redis; never by the Lua script.
	ReplayInvalidRequest DeadLetterReplayOutcome = "invalid_request"
	// ReplayUnauthorized means the caller lacked the deadletter.replay scope.
	// Produced by the manager layer (B3 authz); never enters the Lua script.
	ReplayUnauthorized DeadLetterReplayOutcome = "unauthorized"
)

// ReplayDeadLetterRequest is the activation-safe replay request. The store
// performs node/activation guards and writes an immutable receipt keyed by
// RequestID so a lost response can be recovered by retrying with the same
// RequestID.
//
// Operator is injected from an authenticated principal by the DeadLetterManager
// — never self-reported by the caller. Reason is required and length-bounded.
type ReplayDeadLetterRequest struct {
	ExecutionID types.ExecutionID
	EntryID    string
	RequestID  string // caller-supplied idempotency key; empty means the store mints one
	Operator   string // from authenticated principal; never unverified free text
	Reason     string // required, length-bounded
}

// ReplayDeadLetterResult is the stable replay result. AuditID identifies the
// immutable Redis receipt written atomically with the dead→ready move; it is
// returned for both ReplayReplayed and ReplayAlreadyReplayed so a retry after
// a lost response recovers the original outcome.
type ReplayDeadLetterResult struct {
	Outcome      DeadLetterReplayOutcome
	AuditID      string
	ExecutionID  types.ExecutionID
	NodeID       string
	ActivationID string
}

// DeadLetterPage is a cursor-pagination request for ListDeadLetters. Cursor is
// opaque and stable; an empty cursor starts from the oldest entry. Limit is
// bounded by the implementation.
type DeadLetterPage struct {
	Cursor string
	Limit  int
}

// DeadLetterList is a page of dead-letter entries plus the cursor for the next
// page. NextCursor is empty when the page is the last.
type DeadLetterList struct {
	Entries    []OutboxEntry
	NextCursor string
}

// DeadLetterStore is an optional StateStore capability that exposes
// dead-lettered durable outbox entries for operational inspection and safe
// replay. Replay moves an entry atomically from dead-letter storage back to
// the ready set so the OutboxDispatcher can redeliver it.
//
// Implementations must guarantee:
//   - Replay is activation-safe: it rejects entries whose node is terminal or
//     whose activation no longer matches the node's current activation, so a
//     stale activation cannot be resurrected to advance a node that moved on.
//   - Replay is idempotent under RequestID: a retry with the same RequestID
//     after a lost response returns ReplayAlreadyReplayed with the original
//     AuditID, not an unprovable not_found.
//   - Concurrent replays of the same entry collapse to exactly one
//     ReplayReplayed; the rest return ReplayAlreadyReplayed.
//   - Replay is rejected when the execution is terminal, canceled, expired,
//     or otherwise inactive.
//   - The original immutable body is preserved across the move; the delivery
//     attempt counter is reset to zero on replay.
//   - An immutable receipt is written atomically with the move, recording
//     entry, execution, node, activation, operator, reason, outcome, time.
//   - ListDeadLetters uses a stable opaque cursor and a bounded limit; it never
//     returns the full set in one call.
//
// CLI/admin tooling must go through the DeadLetterManager (which wraps this
// capability with metrics and audit) rather than constructing Redis keys
// directly, so the atomic contract is owned by the backend.
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, id types.ExecutionID, page DeadLetterPage) (DeadLetterList, error)
	ReplayDeadLetter(ctx context.Context, req ReplayDeadLetterRequest) (ReplayDeadLetterResult, error)
}

// DeadLetterAuditSink is an append-only projection of replay receipts. The
// Redis receipt written by the DeadLetterStore is authoritative; this sink is
// a secondary projection (stdout/logger for G0, SQL for G1 reconcile) and must
// not be the sole record. A nil/erroring sink does not block replay.
type DeadLetterAuditSink interface {
	RecordReplay(ctx context.Context, res ReplayDeadLetterResult, req ReplayDeadLetterRequest) error
}

// ListDeadLetters returns one page of dead-lettered outbox entries for an
// execution. It returns ErrDeadLetterUnsupported when the StateStore does not
// implement DeadLetterStore.
func (e *Engine) ListDeadLetters(ctx context.Context, id types.ExecutionID, page DeadLetterPage) (DeadLetterList, error) {
	state, err := e.atomicState()
	if err != nil {
		return DeadLetterList{}, err
	}
	store, ok := state.(DeadLetterStore)
	if !ok {
		return DeadLetterList{}, ErrDeadLetterUnsupported
	}
	return store.ListDeadLetters(ctx, id, page)
}

// ReplayDeadLetter moves a dead-lettered entry back to the ready set via the
// DeadLetterStore capability and notifies observers of the outcome. It is the
// programmatic in-process entry point for replay. CLI/HTTP callers should go
// through the DeadLetterManager instead so metrics and audit are recorded at a
// single outlet.
func (e *Engine) ReplayDeadLetter(ctx context.Context, req ReplayDeadLetterRequest) (ReplayDeadLetterResult, error) {
	state, err := e.atomicState()
	if err != nil {
		return ReplayDeadLetterResult{}, err
	}
	store, ok := state.(DeadLetterStore)
	if !ok {
		return ReplayDeadLetterResult{}, ErrDeadLetterUnsupported
	}
	res, err := store.ReplayDeadLetter(ctx, req)
	if err != nil {
		e.notifyOutboxError(ctx, "replay", err)
		return ReplayDeadLetterResult{}, err
	}
	e.notifyOutboxReplayed(ctx, res.Outcome)
	return res, nil
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
