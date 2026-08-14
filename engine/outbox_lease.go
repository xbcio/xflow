package engine

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/types"
)

// claimOutbox takes ready delivery intents for exclusive delivery.
//
// A store that leases hands each entry out to one deliverer at a time; a store
// that does not is read from directly, which stays correct because delivery is
// at-least-once, but duplicates when several processes flush the same execution.
func (e *Engine) claimOutbox(ctx context.Context, state AtomicStateStore, id types.ExecutionID, limit int) ([]OutboxEntry, error) {
	now := time.Now().UTC()
	if leaser, ok := state.(OutboxLeaser); ok {
		return leaser.LeaseOutbox(ctx, id, now, limit)
	}
	return state.ListOutbox(ctx, id, now, limit)
}

// outboxLeaseKeeper renews the delivery leases one flush batch holds, for as
// long as it still holds them.
//
// This is what lets OutboxDeliveryLeaseTTL be short. The TTL has to be long
// enough to cover the slowest legitimate delivery, or a live deliverer gets its
// work stolen and the intent is delivered twice; it has to be short, or a
// crashed deliverer strands its entries for the whole timeout. Renewal breaks
// the tie: liveness is proven continuously, so the TTL only ever measures how
// long a DEAD deliverer's entries stay invisible.
//
// Entries drop out of the keeper as they are acked, released, or dead-lettered,
// so a batch that is half delivered renews only the half still in flight.
type outboxLeaseKeeper struct {
	engine *Engine
	state  AtomicStateStore
	id     types.ExecutionID

	mu   sync.Mutex
	held map[string]OutboxEntry

	cancel context.CancelFunc
	done   chan struct{}
}

// startOutboxLeaseKeeper begins renewing the batch's leases. It returns a
// keeper even when the store cannot renew, so callers need no nil checks; that
// keeper simply does nothing.
func (e *Engine) startOutboxLeaseKeeper(ctx context.Context, state AtomicStateStore, id types.ExecutionID, entries []OutboxEntry) *outboxLeaseKeeper {
	k := &outboxLeaseKeeper{engine: e, state: state, id: id}
	if _, ok := state.(OutboxLeaser); !ok || len(entries) == 0 {
		return k
	}
	k.held = make(map[string]OutboxEntry, len(entries))
	for _, entry := range entries {
		k.held[entry.ID] = entry
	}

	// The renewal loop must outlive neither the flush nor the caller's ctx, but
	// it must not inherit a ctx that is already near its deadline for renewal
	// purposes only — a renewal that fails simply lets the lease lapse, which
	// is the safe direction.
	renewCtx, cancel := context.WithCancel(ctx)
	k.cancel = cancel
	k.done = make(chan struct{})
	go k.run(renewCtx)
	return k
}

func (k *outboxLeaseKeeper) run(ctx context.Context) {
	defer close(k.done)
	ticker := time.NewTicker(OutboxLeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !k.renewOnce(ctx) {
				return
			}
		}
	}
}

// renewOnce extends every lease still held. It reports whether renewal should
// continue: an emptied batch has nothing left to renew, and neither does a
// batch whose leases this deliverer has lost.
func (k *outboxLeaseKeeper) renewOnce(ctx context.Context) bool {
	k.mu.Lock()
	if len(k.held) == 0 {
		k.mu.Unlock()
		return false
	}
	entries := make([]OutboxEntry, 0, len(k.held))
	for _, entry := range k.held {
		entries = append(entries, entry)
	}
	k.mu.Unlock()

	leaser, ok := k.state.(OutboxLeaser)
	if !ok {
		return false
	}
	renewed, err := leaser.RenewOutbox(ctx, k.id, entries, time.Now().UTC())
	if err != nil {
		// A failed renewal is not fatal to the delivery in flight: the entries
		// stay claimed until the TTL lapses, and a lapse redelivers rather than
		// loses. Report it and keep trying — a transient Redis blip should not
		// end renewal for the rest of the batch.
		k.engine.notifyOutboxError(ctx, "renew", err)
		return true
	}

	// Each granted renewal carries a fresh deadline, and the next renewal must
	// present it to prove ownership. Recording it is what makes renewal
	// repeatable rather than a one-shot: keeping the original deadline would
	// make every later renewal look like it came from a lapsed holder.
	//
	// An entry the store did not renew is one this deliverer no longer holds.
	// Drop it: continuing to renew it would be an attempt to steal it back from
	// whoever claimed it, and the store refuses that anyway.
	granted := make(map[string]int64, len(renewed))
	for _, entry := range renewed {
		granted[entry.ID] = entry.LeaseDeadlineMs
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for id, entry := range k.held {
		deadline, ok := granted[id]
		if !ok {
			delete(k.held, id)
			continue
		}
		entry.LeaseDeadlineMs = deadline
		k.held[id] = entry
	}
	return len(k.held) > 0
}

// forget drops one entry from renewal. Called once its delivery reached a
// terminal state — acked, released, or dead-lettered — so a lease that no
// longer exists is not renewed.
func (k *outboxLeaseKeeper) forget(entryID string) {
	if k == nil || k.held == nil {
		return
	}
	k.mu.Lock()
	delete(k.held, entryID)
	k.mu.Unlock()
}

// stop ends renewal and waits for the loop to exit, so no renewal outlives the
// flush that owns it.
func (k *outboxLeaseKeeper) stop() {
	if k == nil || k.cancel == nil {
		return
	}
	k.cancel()
	<-k.done
}
