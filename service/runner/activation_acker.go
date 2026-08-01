package runner

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// activationAckTimeout bounds how long a single ActivationAck POST may run.
// The send happens in its own goroutine (see activationAcker.ackFailed) so a
// hung server would otherwise leak goroutines indefinitely; this caps that.
const activationAckTimeout = 10 * time.Second

// activationAckClient is the subset of the runner protocol used to report a
// failed activation back to the server. Kept separate from ProtocolClient so
// activationAcker can be constructed and tested without a full runner or a
// transport that implements every RPC (e.g. the gRPC transport does not carry
// activation directives at all yet, so it has no need for this method).
type activationAckClient interface {
	ActivationAck(ctx context.Context, ack protocol.ActivationAck) error
}

// activationAckKey identifies one hosted activation, independent of
// generation, so repeated failures of the same activation collapse to a
// single map entry. WorkflowVersion is part of the key because generation
// sequences are per-EntryActivationKey (which includes version): two records
// with the same (WorkflowID, EntryUnitID) but different versions have
// independent generation counters. Without version in the key, a high
// generation on v1 would suppress a lower-generation ack for v2.
type activationAckKey struct {
	WorkflowID      string
	WorkflowVersion string
	EntryUnitID     string
}

// activationAcker sends ActivationAck for failed activate directives to the
// server. It is wired as ActivationTracker's onActivateFailed callback, which
// runs synchronously in the ProcessDirectives call chain (after t.mu is
// released) — so ackFailed itself must return fast, and the actual network
// call must happen off that goroutine or it would delay every other directive
// in the batch and push back the runner's next heartbeat tick.
//
// acked remembers, per (WorkflowID, WorkflowVersion, EntryUnitID), the highest
// generation already acked. Generation is monotonic within a single
// EntryActivationKey (which includes version) and assigned by the reconciler on
// every redispatch, so a supply that stays unavailable makes the runner
// re-attempt (and re-fail) the SAME generation on every heartbeat — deduping
// on it turns that into exactly one ack per redispatch instead of one per
// retry, which is what keeps a long outage from becoming an ack storm. When a
// higher generation does arrive (a genuine redispatch), the entry is
// overwritten rather than left to accumulate, so the map's size is bounded by
// the number of distinct activations this runner has ever attempted, not by
// how many times any one of them has been retried.
type activationAcker struct {
	client   activationAckClient
	runnerID string
	logger   *slog.Logger

	mu    sync.Mutex
	acked map[activationAckKey]uint64
}

func newActivationAcker(client activationAckClient, runnerID string, logger *slog.Logger) *activationAcker {
	if logger == nil {
		logger = slog.Default()
	}
	return &activationAcker{
		client:   client,
		runnerID: runnerID,
		logger:   logger,
		acked:    make(map[activationAckKey]uint64),
	}
}

// shouldAck reports whether (key, generation) has not yet been acked and, if
// so, records it as acked. Synchronous and cheap — this is what makes
// ackFailed's dedup decision race-free even when called back-to-back from the
// same goroutine: the second call for an identical failure observes the
// first call's bookkeeping before any goroutine is spawned.
func (a *activationAcker) shouldAck(key activationAckKey, generation uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if last, ok := a.acked[key]; ok && last >= generation {
		return false
	}
	a.acked[key] = generation
	return true
}

// ackFailed reports one failed activate directive, deduped per
// (WorkflowID, WorkflowVersion, EntryUnitID, Generation) and sent asynchronously
// so the caller (ActivationTracker's callback, invoked from ProcessDirectives)
// never blocks on network I/O. err.Error() is the only thing that travels in
// the ack body — never the directive's Params or any supply content.
func (a *activationAcker) ackFailed(sessionID string, d protocol.ActivateDirective, err error) {
	key := activationAckKey{WorkflowID: d.WorkflowID, WorkflowVersion: d.WorkflowVersion, EntryUnitID: d.EntryUnitID}
	if !a.shouldAck(key, d.Generation) {
		return
	}

	ack := protocol.ActivationAck{
		RunnerID:        a.runnerID,
		SessionID:       sessionID,
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		GroupID:         d.EntryUnitID,
		Generation:      d.Generation,
		Status:          protocol.ActivationStatusFailed,
		Error:           err.Error(),
	}

	// Fire-and-forget by design: this goroutine is never added to a
	// WaitGroup and is not cancelled by Runner shutdown. activationAckTimeout
	// (10s) is its ONLY lifecycle bound — nothing joins it, so
	// Runner.Run can return (and the process can exit) while a send is still
	// in flight. That is an accepted trade-off, not an oversight: the ack is
	// a best-effort notification, and losing one in flight during shutdown
	// has the same effect as losing it on the wire — the reconciler simply
	// redispatches this activation on its next reconcile cycle regardless.
	// Joining here would mean plumbing a shutdown signal through
	// ActivationTracker's onActivateFailed callback boundary (currently
	// `func(protocol.ActivateDirective, error)`, no context/WaitGroup) and,
	// worse, could make graceful shutdown wait up to activationAckTimeout
	// longer than it does today for a signal whose only purpose is to speed
	// up a redispatch that will happen anyway.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				a.logger.Error("activation ack panicked",
					"workflow_id", d.WorkflowID,
					"group_id", d.EntryUnitID,
					"generation", d.Generation,
					"recover", rec,
				)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), activationAckTimeout)
		defer cancel()
		if sendErr := a.client.ActivationAck(ctx, ack); sendErr != nil {
			a.logger.Warn("activation ack failed",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"error", sendErr,
			)
		}
	}()
}
