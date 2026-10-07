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
// transport that implements every RPC. Both the HTTP and gRPC clients
// implement it today (gRPC via the AckActivation RPC); the in-process
// transport also implements it directly against Core.
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
	ReplicaIndex    uint32
}

// deactivationAckKey includes the runner session. A durable cleanup obligation
// can be explicitly rebound to a replacement session, and an acknowledgement
// from the old fenced session must not suppress delivery from the new one.
type deactivationAckKey struct {
	activationAckKey
	SessionID  string
	Generation uint64
}

// activationAcker sends ActivationAck for failed activate directives to the
// server. It is wired as ActivationTracker's onActivateFailed callback, which
// runs synchronously in the ProcessDirectives call chain (after t.mu is
// released) — so ackFailed itself must return fast, and the actual network
// call must happen off that goroutine or it would delay every other directive
// in the batch and push back the runner's next heartbeat tick.
//
// acked remembers, per (WorkflowID, WorkflowVersion, EntryUnitID, ReplicaIndex),
// the highest generation whose failure ack was successfully DELIVERED.
// Generation is monotonic within a single EntryActivationKey (which includes
// version and replica) and assigned by the reconciler on every redispatch, so
// a supply that stays unavailable makes the runner re-attempt (and re-fail)
// the SAME generation on every heartbeat — deduping on a delivered ack turns
// that into exactly one ack per redispatch instead of one per retry, which is
// what keeps a long outage from becoming an ack storm. When a higher
// generation does arrive (a genuine redispatch), the entry is overwritten
// rather than left to accumulate, so the map's size is bounded by the number
// of distinct activations this runner has ever attempted, not by how many
// times any one of them has been retried.
//
// "Delivered" is the load-bearing word: a failed send must NOT count, or a
// single lost request would permanently suppress the failure signal for that
// (key, generation) — the server would never learn this runner cannot take
// the activation, and its report-driven redelivery would keep missing the
// reason. activationAckInFlight is the other half of that: it dedups
// concurrent/repeated attempts while one send is still outstanding, so
// retrying after a failure cannot turn into an ack storm.
type activationAcker struct {
	client   activationAckClient
	runnerID string
	logger   *slog.Logger

	mu                    sync.Mutex
	acked                 map[activationAckKey]uint64
	activationAckInFlight map[activationAckKey]uint64
	// Deactivation receipts are durable cleanup evidence, unlike failed
	// activation acks. Do not mark one delivered until the HTTP call succeeds;
	// a lost response must be retried when the server re-delivers its obligation.
	deactivated          map[deactivationAckKey]bool
	deactivationInFlight map[deactivationAckKey]bool
}

func newActivationAcker(client activationAckClient, runnerID string, logger *slog.Logger) *activationAcker {
	if logger == nil {
		logger = slog.Default()
	}
	return &activationAcker{
		client:                client,
		runnerID:              runnerID,
		logger:                logger,
		acked:                 make(map[activationAckKey]uint64),
		activationAckInFlight: make(map[activationAckKey]uint64),
		deactivated:           make(map[deactivationAckKey]bool),
		deactivationInFlight:  make(map[deactivationAckKey]bool),
	}
}

// beginActivationAck reserves (key, generation) for one send and reports
// whether the caller should proceed. It returns false when a delivery for the
// same or a newer generation already succeeded, or while one is in flight —
// the same "do not mark delivered until the call succeeds" discipline the
// deactivation receipts use. Synchronous and cheap, so ackFailed's dedup
// decision is race-free even when called back-to-back: the second call
// observes the first call's in-flight entry before any goroutine is spawned.
func (a *activationAcker) beginActivationAck(key activationAckKey, generation uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if last, ok := a.acked[key]; ok && last >= generation {
		return false
	}
	if pending, ok := a.activationAckInFlight[key]; ok && pending >= generation {
		return false
	}
	a.activationAckInFlight[key] = generation
	return true
}

// finishActivationAck releases the in-flight reservation. Only a successful
// send advances acked; a failed one clears the reservation so the next
// onActivateFailed for this activation (every redelivery of the directive
// re-triggers it) can try again.
//
// The delete is conditional on the reservation still being THIS generation's.
// A newer generation can legitimately take the slot while an older send is in
// flight (beginActivationAck admits it because pending < generation), and an
// unconditional delete here would let the stale completion erase the newer
// reservation — a second failure for the newer generation would then dispatch
// a duplicate ack HTTP call. Since begin only ever writes a strictly greater
// value than what it found, the value observed here is either this
// generation's own, a higher generation's, or already gone: deleting only on
// an exact match is safe in every ordering.
func (a *activationAcker) finishActivationAck(key activationAckKey, generation uint64, delivered bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.activationAckInFlight[key]; ok && current == generation {
		delete(a.activationAckInFlight, key)
	}
	if delivered {
		if last, ok := a.acked[key]; !ok || generation > last {
			a.acked[key] = generation
		}
	}
}

// ackFailed reports one failed activate directive, deduped per successfully
// delivered (WorkflowID, WorkflowVersion, EntryUnitID, ReplicaIndex,
// Generation) with an in-flight guard, and sent asynchronously so the caller
// (ActivationTracker's callback, invoked from ProcessDirectives) never blocks
// on network I/O. err.Error() is the only thing that travels in the ack body —
// never the directive's Params or any supply content.
//
// A send that fails releases its reservation instead of recording the ack, so
// the next failure for the same directive retries it: with server-side
// redelivery pacing the directive arrives again (same generation) and the
// handler fails again, which re-enters this method.
func (a *activationAcker) ackFailed(sessionID string, d protocol.ActivateDirective, err error) {
	key := activationAckKey{
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		EntryUnitID:     d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
	}
	if !a.beginActivationAck(key, d.Generation) {
		return
	}

	ack := protocol.ActivationAck{
		RunnerID:        a.runnerID,
		SessionID:       sessionID,
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		GroupID:         d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
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
				a.finishActivationAck(key, d.Generation, false)
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
		sendErr := a.client.ActivationAck(ctx, ack)
		a.finishActivationAck(key, d.Generation, sendErr == nil)
		if sendErr != nil {
			a.logger.Warn("activation ack failed",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"error", sendErr,
			)
		}
	}()
}

// ackDeactivated reports a successful local cleanup for one durable
// DeactivateDirective. It deliberately has stronger retry semantics than
// ackFailed: the server's receipt ledger cannot clear until this exact request
// arrives, so a transport error releases the local in-flight guard and lets the
// next re-delivered directive retry it.
func (a *activationAcker) ackDeactivated(sessionID string, d protocol.DeactivateDirective) {
	key := deactivationAckKey{
		activationAckKey: activationAckKey{
			WorkflowID:      d.WorkflowID,
			WorkflowVersion: d.WorkflowVersion,
			EntryUnitID:     d.EntryUnitID,
			ReplicaIndex:    d.ReplicaIndex,
		},
		SessionID:  sessionID,
		Generation: d.Generation,
	}
	if !a.beginDeactivationAck(key) {
		return
	}
	ack := protocol.ActivationAck{
		RunnerID:        a.runnerID,
		SessionID:       sessionID,
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		GroupID:         d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
		Generation:      d.Generation,
		Status:          protocol.ActivationStatusDeactivated,
	}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				a.finishDeactivationAck(key, false)
				a.logger.Error("deactivation receipt panicked",
					"workflow_id", d.WorkflowID,
					"group_id", d.EntryUnitID,
					"generation", d.Generation,
					"recover", rec,
				)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), activationAckTimeout)
		defer cancel()
		err := a.client.ActivationAck(ctx, ack)
		a.finishDeactivationAck(key, err == nil)
		if err != nil {
			a.logger.Warn("deactivation receipt failed",
				"workflow_id", d.WorkflowID,
				"group_id", d.EntryUnitID,
				"generation", d.Generation,
				"error", err,
			)
		}
	}()
}

func (a *activationAcker) beginDeactivationAck(key deactivationAckKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deactivated[key] || a.deactivationInFlight[key] {
		return false
	}
	a.deactivationInFlight[key] = true
	return true
}

func (a *activationAcker) finishDeactivationAck(key deactivationAckKey, delivered bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.deactivationInFlight, key)
	if delivered {
		a.deactivated[key] = true
	}
}
