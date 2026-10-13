package engine

import (
	"context"
	"errors"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// ErrConcurrencyLimit is returned by a submission that a workflow's
// settings.concurrency refused. In REJECT mode nothing was persisted for it;
// the HTTP layer maps it to 429 + Retry-After.
var ErrConcurrencyLimit = errors.New("workflow concurrency limit reached")

// ErrConcurrencyUnsupported is returned when a workflow declares a concurrency
// limit but the wired StateStore cannot enforce one.
//
// It fails closed by design: silently running unlimited would defeat a limit
// the author declared to protect a downstream, and a rolling deployment is
// exactly the situation where that failure would otherwise hide. The check is
// live (a type assertion at submission time), not a registration-time flag, so
// a backend that gains the capability stops failing without a restart.
var ErrConcurrencyUnsupported = errors.New("state store does not support execution concurrency limits")

// ErrConcurrencyQueueUnsupported is returned when a workflow asks for QUEUE
// mode and the wired StateStore cannot park an execution.
//
// It exists so an over-limit submission is never silently downgraded: a store
// that can enforce a limit but cannot queue must refuse, because admitting the
// submission would break the limit the definition declared, and rejecting it
// would fail a submission the author expected to be held. The refusal is the
// store's decision, not the engine's — only the store knows whether it can park
// an execution — and it is what a store must return when it cannot.
var ErrConcurrencyQueueUnsupported = errors.New("state store does not support concurrency queue mode")

// ErrConcurrencyQueueFull is returned by a QUEUE-mode submission that would
// exceed the workflow's max_queued backlog. Nothing was persisted for it.
//
// It is a distinct sentinel from ErrConcurrencyLimit because the two say
// different things to a caller: a full queue is a backlog to wait out, and a
// reached limit is a slot to wait out. Both are retryable, and the HTTP layer
// gives them different codes so a caller can tell which one it hit.
var ErrConcurrencyQueueFull = errors.New("workflow concurrency queue is full")

// ConcurrencyQueueTimeoutError is the execution-level reason recorded when a
// queued execution waits past its queue_timeout and is driven into the timeout
// terminal state.
//
// It is a constant rather than a literal at each site because it is the only
// thing a caller has to tell this timeout from the execution's own: every
// backend writes this exact text, and the contract tests read it back.
const ConcurrencyQueueTimeoutError = "concurrency queue timeout"

// ConcurrencyAdmission describes the slot one submission contends for.
//
// Limit and Mode are resolved from the submitting execution's own compiled
// graph, so a submission is judged by the definition it was submitted under:
// lowering the limit never preempts a running execution, and raising it does
// not retroactively re-judge one.
type ConcurrencyAdmission struct {
	// Namespace scopes the slot set, like every other key in the store.
	Namespace string
	// Workflow is the workflow NAME, not the version: slots are keyed by
	// namespace+workflow so a version bump cannot orphan the slots the previous
	// version's executions still hold (graph.ConcurrencyKey).
	Workflow string
	// Limit is the maximum number of concurrently running executions. Callers
	// only reach the store when it is > 0.
	Limit int
	// Mode is types.ConcurrencyModeReject or types.ConcurrencyModeQueue.
	Mode string
	// MaxQueued caps the QUEUE-mode backlog. 0 means unbounded. Ignored in
	// reject mode.
	MaxQueued int
	// QueueTimeout bounds how long a QUEUED execution may wait for a slot. The
	// clock starts at admission. 0 means no bound. Ignored in reject mode.
	QueueTimeout time.Duration
}

// ConcurrencyAdmissionResult reports what the admission did.
type ConcurrencyAdmissionResult struct {
	// Queued is true when the execution was parked instead of started. The
	// caller must not flush its initial outbox: the store holds those intents
	// until promotion makes them deliverable.
	Queued bool
	// QueuePosition is the queued execution's 1-based position, or 0 when
	// Queued is false. Position 1 is the head: the execution the next release
	// promotes.
	QueuePosition int
}

// ConcurrencyLimitedStateStore is the optional StateStore capability that
// enforces settings.concurrency. It extends AtomicStateStore because slot
// admission has to commit in the SAME atomic step as the execution's creation:
// a slot reserved by an execution that failed to persist would leak, and an
// execution persisted without its slot would over-admit.
//
// Implementations own both halves of the contract:
//   - admission (this method) charges a slot in the same transition that
//     persists the execution and its initial outbox, and returns
//     ErrConcurrencyLimit without persisting anything when a reject-mode
//     submission is over the limit;
//   - release happens inside the store's own terminal-status writers, so no
//     engine call site can forget it. Every path that moves an execution into
//     types.IsTerminalExecutionStatus releases its slot idempotently, and a
//     non-terminal transition (canceling) keeps it.
type ConcurrencyLimitedStateStore interface {
	AtomicStateStore
	// CreateExecutionWithConcurrency persists a new execution under the
	// supplied workflow's concurrency policy. See the interface doc for the
	// atomicity contract.
	CreateExecutionWithConcurrency(ctx context.Context, execution *ExecutionSnapshot, entries []OutboxEntry, admission ConcurrencyAdmission) (ConcurrencyAdmissionResult, error)
}

// ConcurrencyQueueExpirer is the optional StateStore capability that enforces
// queue_timeout: it drives every queued execution whose timeout has passed into
// the timeout terminal state.
//
// It is a separate capability from ConcurrencyLimitedStateStore because the two
// are driven from different places. Admission and release happen on the
// submission path; expiry happens on a clock, in whatever background loop the
// deployment owns — the control plane's concurrency maintenance in cluster
// mode, the local backend's own ticker in-process. The split is not cosmetic:
// an expiry driven from the engine would be per-process, so a process that
// admitted a queued execution and then died would leave it queued past its
// bound with nobody left to notice.
type ConcurrencyQueueExpirer interface {
	// ExpireQueuedExecutions drives every queued execution whose queue timeout
	// has passed into types.ExecutionStatusTimeout with
	// ConcurrencyQueueTimeoutError as its reason, and reports how many it
	// expired. Bounded by limit (0 means the store's own batch size), and safe
	// to call concurrently: an execution a release or a cancel has already
	// started is no longer queued and is not expired.
	ExpireQueuedExecutions(ctx context.Context, limit int) (int, error)
}

// ConcurrencyQueueInspector is the optional StateStore capability that answers
// where one queued execution stands in line.
//
// It is separate from ConcurrencyLimitedStateStore because the queue is read
// long after the submission that filled it: the admission result tells the
// submitter its position once, and only the store can say how far the line has
// moved since. It is a read-only capability for the same reason — nothing about
// an inspection may change the queue.
//
// A store that queues without implementing it simply reports no position:
// callers must treat 0 as "unknown or not queued", never as "at the head".
type ConcurrencyQueueInspector interface {
	// ConcurrencyQueuePosition returns the 1-based position of a queued
	// execution (1 is the head, the one the next release promotes), or 0 when
	// the execution is not queued — already promoted, canceled, expired, or
	// never queued at all. It is not an error to ask about an execution that
	// is not queued.
	ConcurrencyQueuePosition(ctx context.Context, id types.ExecutionID) (int, error)
}

// ConcurrencyLimitedEntryAdmissionStore is the optional StateStore capability
// that applies a workflow's concurrency policy to the entry-seed path.
//
// It is separate from EntryAdmissionStore for the same reason
// ConcurrencyLimitedStateStore is separate from AtomicStateStore: the two are
// independent capabilities, and a backend that can seed entries without being
// able to enforce a limit must be able to say so rather than being forced to
// implement both. The engine routes to it only when the seeded graph declares a
// limit and fails closed when it is absent.
//
// An implementation owns the same contract as CreateExecutionWithConcurrency,
// with the seed's own creation step in place of the ordinary one:
//   - a REJECT-mode admission over the limit persists nothing and answers that
//     it was refused (AdmissionStateOverLimit);
//   - a QUEUE-mode admission over the limit persists the execution in the
//     queued status with its intents held, exactly as a queued submission is
//     persisted, and answers accepted;
//   - a refused full queue persists nothing and answers
//     AdmissionStateQueueFull;
//   - an admission this call did not create — a duplicate or a conflict —
//     leaves the slot state exactly as the admission that did create it left
//     it, so a redelivered batch neither charges a second slot nor loses the
//     original execution's place in line.
//
// Release needs no method here: the store's own terminal writers already
// release whatever a seeded execution holds, because the seed writes the same
// slot back-reference an ordinary admission writes.
type ConcurrencyLimitedEntryAdmissionStore interface {
	EntryAdmissionStore
	// SeedExecutionFromEntryWithConcurrency admits one entry-unit seed under
	// the supplied workflow's concurrency policy. See the interface doc for the
	// atomicity contract.
	SeedExecutionFromEntryWithConcurrency(ctx context.Context, req SeedExecutionFromEntryRequest, admission ConcurrencyAdmission) (SeedExecutionFromEntryResponse, error)
}

// concurrencyAdmissionFor builds the admission one submission contends for from
// its own compiled graph, or nil when the graph declares no limit.
//
// It is deliberately the only place the two are joined: the engine has the
// graph and the namespace, the store has the slots, and a second join site
// could read a different definition than the one the execution persisted.
func concurrencyAdmissionFor(ctx context.Context, g *graph.Graph) *ConcurrencyAdmission {
	if g == nil {
		return nil
	}
	settings := g.Concurrency()
	if settings == nil || settings.Limit <= 0 {
		return nil
	}
	return &ConcurrencyAdmission{
		Namespace:    string(namespace.FromContext(ctx)),
		Workflow:     g.Name(),
		Limit:        settings.Limit,
		Mode:         settings.EffectiveMode(),
		MaxQueued:    settings.MaxQueued,
		QueueTimeout: settings.QueueTimeout,
	}
}
