package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// leasingState is a fakeState that leases like a real backend does, so a flush
// holding a batch can be observed from the outside the way a concurrent flush
// would see it.
//
// The lease is fenced on its deadline exactly as the backends fence theirs — a
// renewal presenting a deadline that no longer matches is refused. Without that
// the fake would accept renewals the real stores reject, and the keeper could
// pass here while stealing entries in production.
type leasingState struct {
	*fakeState

	mu       sync.Mutex
	leases   map[string]time.Time
	renewals int
	// refuseRenewal makes every renewal fail, standing in for a store that has
	// gone away mid-flush.
	refuseRenewal bool
}

func newLeasingState() *leasingState {
	return &leasingState{fakeState: newFakeState(), leases: make(map[string]time.Time)}
}

func (s *leasingState) LeaseOutbox(ctx context.Context, id types.ExecutionID, now time.Time, limit int) ([]OutboxEntry, error) {
	entries, err := s.fakeState.ListOutbox(ctx, id, now, limit)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutboxEntry, 0, len(entries))
	deadline := now.Add(OutboxDeliveryLeaseTTL)
	for _, entry := range entries {
		if held, ok := s.leases[entry.ID]; ok && held.After(now) {
			continue
		}
		s.leases[entry.ID] = deadline
		entry.LeaseDeadlineMs = deadline.UnixMilli()
		out = append(out, entry)
	}
	return out, nil
}

func (s *leasingState) RenewOutbox(_ context.Context, _ types.ExecutionID, entries []OutboxEntry, now time.Time) ([]OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.refuseRenewal {
		return nil, nil
	}
	deadline := now.Add(OutboxDeliveryLeaseTTL)
	out := make([]OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		held, ok := s.leases[entry.ID]
		if !ok || !held.After(now) || held.UnixMilli() != entry.LeaseDeadlineMs {
			continue
		}
		s.leases[entry.ID] = deadline
		entry.LeaseDeadlineMs = deadline.UnixMilli()
		out = append(out, entry)
	}
	return out, nil
}

func (s *leasingState) ReleaseOutbox(_ context.Context, _ types.ExecutionID, entry OutboxEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.leases, entry.ID)
	return nil
}

// leasedNow reports the entries currently hidden from other deliverers.
func (s *leasingState) leasedNow(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, until := range s.leases {
		if until.After(now) {
			n++
		}
	}
	return n
}

func (s *leasingState) renewalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewals
}

// blockingQueue holds the flush inside Enqueue until the test releases it, so a
// batch can be observed while it is genuinely in flight.
//
// Blocking is armed explicitly rather than from construction because Submit
// flushes the initial outbox synchronously; a queue that blocks from the start
// would wedge Submit itself, before there is anything to observe.
type blockingQueue struct {
	fakeQueue
	armed   chan struct{}
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingQueue() *blockingQueue {
	return &blockingQueue{
		armed:   make(chan struct{}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (q *blockingQueue) arm() { close(q.armed) }

func (q *blockingQueue) Enqueue(ctx context.Context, task *Task) error {
	select {
	case <-q.armed:
	default:
		return q.fakeQueue.Enqueue(ctx, task)
	}
	q.once.Do(func() { close(q.entered) })
	select {
	case <-q.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return q.fakeQueue.Enqueue(ctx, task)
}

func (q *blockingQueue) EnqueueDelayed(ctx context.Context, task *Task, _ time.Duration) error {
	return q.Enqueue(ctx, task)
}

// A flush that takes longer than OutboxDeliveryLeaseTTL must keep its claim.
//
// This is the whole reason the TTL can be short. Without renewal the operator
// faces an unwinnable choice: a long TTL strands a crashed deliverer's entries
// for its full duration, and a short one lets a concurrent flush steal a batch
// from a deliverer that is merely slow — redelivering work already in flight,
// which is the duplication the lease exists to prevent.
//
// The flush is held inside Enqueue rather than slept past, so the assertion
// observes a batch that is genuinely in flight.
func TestFlushRenewsItsLeasesWhileDeliveryIsStillRunning(t *testing.T) {
	ctx := context.Background()
	state := newLeasingState()
	queue := newBlockingQueue()
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), map[string]any{"request": "r-1"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	queue.Drain()

	// Seed a second batch to hold: Submit already delivered and acked the first.
	if err := state.CreateExecutionWithOutbox(ctx, &ExecutionSnapshot{
		ID: id + "-held", Graph: compileAtomicOutboxGraph(t, nil), Status: types.ExecutionStatusRunning,
	}, []OutboxEntry{{
		ID:   "held/" + string(id) + "/start/1",
		Task: Task{ExecutionID: id + "-held", NodeName: "start", Type: TaskTypeNodeExec, ActivationID: 1},
	}}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}
	held := id + "-held"

	queue.arm()
	flushed := make(chan error, 1)
	go func() { flushed <- eng.FlushOutbox(ctx, held) }()

	select {
	case <-queue.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush never reached the queue")
	}
	// The lease taken when the batch was claimed expires here. Everything after
	// this instant is held only by renewal.
	originalDeadline := time.Now().Add(OutboxDeliveryLeaseTTL)

	// Wait past the original deadline while the flush is still inside Enqueue.
	// A lease that is not renewed has lapsed by now and the entry is up for
	// grabs by any concurrent flush.
	for time.Now().Before(originalDeadline.Add(OutboxLeaseRenewInterval)) {
		time.Sleep(OutboxLeaseRenewInterval / 2)
	}

	if got := state.leasedNow(time.Now()); got == 0 {
		t.Fatalf("entries still leased after the original lease expired = %d, want "+
			"at least 1 — the flush lost its claim while still delivering, so a "+
			"concurrent flush would redeliver work already in flight", got)
	}
	if got := state.renewalCount(); got == 0 {
		t.Fatal("RenewOutbox was never called during a flush that outlived the lease TTL")
	}

	close(queue.release)
	if err := <-flushed; err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}

	// Renewal must stop once the batch is delivered: a keeper that outlives its
	// flush would hold entries no one is working on.
	settled := state.renewalCount()
	time.Sleep(3 * OutboxLeaseRenewInterval)
	if got := state.renewalCount(); got != settled {
		t.Fatalf("renewals continued after the flush returned (%d → %d) — a keeper "+
			"outliving its flush hides entries no deliverer is working on", settled, got)
	}
}

// A store that refuses renewal must not wedge the flush.
//
// Losing a lease is recoverable by design: the entry lapses and is redelivered,
// which at-least-once already covers. Blocking or failing the delivery in
// flight instead would turn a transient store blip into a stalled outbox.
func TestFlushCompletesWhenRenewalIsRefused(t *testing.T) {
	ctx := context.Background()
	state := newLeasingState()
	state.refuseRenewal = true
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileAtomicOutboxGraph(t, nil), map[string]any{"request": "r-1"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v, want delivery to survive a refused renewal", err)
	}
	if queued := queue.Drain(); len(queued) != 1 || queued[0].NodeName != "start" {
		t.Fatalf("queued = %v, want one start task", taskNames(queued))
	}
}
