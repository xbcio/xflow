package rstate

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// outboxNsDelivery is one task the queue received, with the namespace the
// delivering context carried.
type outboxNsDelivery struct {
	ns namespace.Namespace
	id types.ExecutionID
}

// outboxNsQueue is a goroutine-safe task queue for tests that run the real
// OutboxDispatcher loop. StoreTestQueue is not safe for that: the dispatcher
// flushes executions on several goroutines.
//
// It is also a NonBlockingTaskQueue, the way the production queue is, so the
// backpressure path (ErrQueueFull -> release the leases) is the one a full queue
// takes.
type outboxNsQueue struct {
	mu         sync.Mutex
	deliveries []outboxNsDelivery
	attempts   int
	err        error
	full       bool
	// entered is closed the first time an enqueue starts; hold, if set, parks
	// every enqueue until it is closed.
	entered chan struct{}
	hold    chan struct{}
}

func (q *outboxNsQueue) Enqueue(ctx context.Context, task *engine.Task) error {
	q.mu.Lock()
	q.attempts++
	err, entered, hold := q.err, q.entered, q.hold
	q.entered = nil
	q.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if hold != nil {
		<-hold
	}
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deliveries = append(q.deliveries, outboxNsDelivery{ns: namespace.FromContext(ctx), id: task.ExecutionID})
	return nil
}

func (q *outboxNsQueue) TryEnqueue(ctx context.Context, task *engine.Task) error {
	q.mu.Lock()
	full := q.full
	if full {
		q.attempts++
	}
	q.mu.Unlock()
	if full {
		return engine.ErrQueueFull
	}
	return q.Enqueue(ctx, task)
}

func (q *outboxNsQueue) setFull(full bool) {
	q.mu.Lock()
	q.full = full
	q.mu.Unlock()
}

func (q *outboxNsQueue) EnqueueDelayed(ctx context.Context, task *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, task)
}

func (q *outboxNsQueue) snapshot() []outboxNsDelivery {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]outboxNsDelivery(nil), q.deliveries...)
}

func (q *outboxNsQueue) attemptCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.attempts
}

// runOutboxDispatcher starts the production dispatcher loop on a context that
// carries NO namespace -- exactly what Backend.StartBinding does -- and returns
// the function that stops it and waits for it to exit.
func runOutboxDispatcher(t *testing.T, eng *engine.Engine) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.NewOutboxDispatcher(eng, 5*time.Millisecond).Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", within, what)
}

// outboxDrained reports whether the execution's outbox in ns holds nothing: no
// ready member, no body, and no member left in the namespace's readiness index.
func outboxDrained(t *testing.T, rdb *redis.Client, ns namespace.Namespace, id types.ExecutionID) bool {
	t.Helper()
	ctx := context.Background()
	ready, err := rdb.ZCard(ctx, outboxReadyKey(ns, id)).Result()
	if err != nil {
		t.Fatalf("ZCard ready %s/%s: %v", ns, id, err)
	}
	bodies, err := rdb.HLen(ctx, outboxBodyKey(ns, id)).Result()
	if err != nil {
		t.Fatalf("HLen body %s/%s: %v", ns, id, err)
	}
	_, indexed := indexScore(t, ctx, rdb, ns, id)
	return ready == 0 && bodies == 0 && !indexed
}

// TestOutboxDispatcherDeliversInEveryNamespace is the end-to-end regression for
// "outbox never delivered under a non-default namespace".
//
// The dispatcher runs on a context with no namespace, and the store builds every
// outbox key from the namespace on the context it is given. Discovery used to
// answer with bare execution IDs, so the flush of an execution found in
// "tenant-x" claimed against the DEFAULT namespace's keys, found nothing, and
// returned without error -- an empty claim is a normal result. The entry was
// never delivered and the namespace's readiness index only grew.
//
// The default namespace case is the control: there the fallback is coincidentally
// right, which is why the defect was never visible in a single-namespace
// deployment. The same assertions must hold in both.
func TestOutboxDispatcherDeliversInEveryNamespace(t *testing.T) {
	for _, ns := range []namespace.Namespace{namespace.Default, "tenant-x"} {
		t.Run(string(ns), func(t *testing.T) {
			state, rdb, _ := newOutboxIndexTestStore(t)
			ctx := namespace.WithNamespace(context.Background(), ns)
			const id = types.ExecutionID("exec-ns-deliver")
			seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))
			if _, ok := indexScore(t, ctx, rdb, ns, id); !ok {
				t.Fatalf("precondition: %s readiness index does not carry %q after the create", ns, id)
			}

			queue := &outboxNsQueue{}
			stop := runOutboxDispatcher(t, engine.New(state, queue))

			eventually(t, 3*time.Second,
				"the dispatcher to deliver the ready outbox entry of an execution in namespace "+string(ns),
				func() bool { return len(queue.snapshot()) > 0 })
			eventually(t, 3*time.Second,
				"the delivered entry to be acked and the readiness index member pruned in namespace "+string(ns),
				func() bool { return outboxDrained(t, rdb, ns, id) })
			stop()

			want := []outboxNsDelivery{{ns: ns, id: id}}
			if got := queue.snapshot(); len(got) != 1 || got[0] != want[0] {
				t.Fatalf("deliveries = %+v, want exactly %+v (the enqueue must run on the execution's namespace)", got, want)
			}
		})
	}
}

// TestOutboxDispatcherKeepsSameNamedExecutionsApart seeds one execution ID into
// three namespaces. Each has an outbox of its own, so each must be delivered
// under its own namespace; discovery keyed by ID alone collapses them into one
// flush on the default namespace.
func TestOutboxDispatcherKeepsSameNamedExecutionsApart(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	const id = types.ExecutionID("exec-ns-shared")
	namespaces := []namespace.Namespace{namespace.Default, "tenant-x", "tenant-y"}
	for _, ns := range namespaces {
		seedReadyOutbox(t, state, namespace.WithNamespace(context.Background(), ns), id, time.Now().Add(-time.Second))
	}

	queue := &outboxNsQueue{}
	stop := runOutboxDispatcher(t, engine.New(state, queue))
	eventually(t, 3*time.Second, "every namespace's copy of the shared execution to be delivered and pruned",
		func() bool {
			for _, ns := range namespaces {
				if !outboxDrained(t, rdb, ns, id) {
					return false
				}
			}
			return true
		})
	stop()

	got := queue.snapshot()
	sort.Slice(got, func(i, j int) bool { return got[i].ns < got[j].ns })
	want := []outboxNsDelivery{{ns: namespace.Default, id: id}, {ns: "tenant-x", id: id}, {ns: "tenant-y", id: id}}
	if len(got) != len(want) {
		t.Fatalf("deliveries = %+v, want exactly one per namespace %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deliveries = %+v, want %+v", got, want)
		}
	}
}

// TestOutboxDispatcherDeadLettersInTheExecutionsNamespace covers the failure
// half of the delivery: a handoff that keeps failing is released, counted and
// finally dead-lettered, and every one of those writes builds its keys from the
// namespace on the flush context. Only the flush context was ever wrong, so this
// fails when the ref's namespace is not applied before the flush starts, and it
// fails the other way if a dead-letter lands under "default".
func TestOutboxDispatcherDeadLettersInTheExecutionsNamespace(t *testing.T) {
	for _, ns := range []namespace.Namespace{namespace.Default, "tenant-x"} {
		t.Run(string(ns), func(t *testing.T) {
			state, rdb, _ := newOutboxIndexTestStore(t)
			ctx := namespace.WithNamespace(context.Background(), ns)
			const id = types.ExecutionID("exec-ns-dead")
			entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))

			queue := &outboxNsQueue{err: errors.New("queue unavailable")}
			eng := engine.New(state, queue, engine.WithOutboxMaxDeliveryAttempts(2))
			stop := runOutboxDispatcher(t, eng)

			bg := context.Background()
			eventually(t, 3*time.Second, "the failing entry to be dead-lettered in namespace "+string(ns), func() bool {
				n, err := rdb.ZCard(bg, outboxDeadKey(ns, id)).Result()
				if err != nil {
					t.Fatalf("ZCard dead: %v", err)
				}
				return n == 1
			})
			eventually(t, 3*time.Second, "the dead-lettered entry's ready member, body and index member to be gone", func() bool {
				return outboxDrained(t, rdb, ns, id)
			})
			stop()

			if ok, err := rdb.HExists(bg, outboxDeadBodyKey(ns, id), entryID).Result(); err != nil || !ok {
				t.Fatalf("dead-letter body for %q in %s = %v, %v, want it stored", entryID, ns, ok, err)
			}
			if attempts, err := rdb.HLen(bg, outboxAttemptsKey(ns, id)).Result(); err != nil || attempts != 0 {
				t.Fatalf("attempt counters left in %s = %d, %v, want none once dead-lettered", ns, attempts, err)
			}
			for _, other := range []namespace.Namespace{namespace.Default, "tenant-x"} {
				if other == ns {
					continue
				}
				if n, err := rdb.ZCard(bg, outboxDeadKey(other, id)).Result(); err != nil || n != 0 {
					t.Fatalf("namespace %s holds %d dead letters for an execution of %s, want 0", other, n, ns)
				}
			}
			if got := queue.snapshot(); len(got) != 0 {
				t.Fatalf("deliveries = %+v, want none from a queue that refuses every task", got)
			}
		})
	}
}

// TestOutboxDispatcherReleasesLeasesInTheExecutionsNamespace covers backpressure.
// A full queue makes FlushOutbox hand its delivery leases back so the entries are
// deliverable again at once instead of after OutboxDeliveryLeaseTTL. The release
// is a body-guarded write on the namespace's keys: run against the default
// namespace's keys it finds no body, does nothing, and leaves the entry hidden
// for the whole lease.
func TestOutboxDispatcherReleasesLeasesInTheExecutionsNamespace(t *testing.T) {
	for _, ns := range []namespace.Namespace{namespace.Default, "tenant-x"} {
		t.Run(string(ns), func(t *testing.T) {
			state, rdb, _ := newOutboxIndexTestStore(t)
			ctx := namespace.WithNamespace(context.Background(), ns)
			const id = types.ExecutionID("exec-ns-release")
			entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))

			queue := &outboxNsQueue{full: true}
			eng := engine.New(state, queue)
			stop := runOutboxDispatcher(t, eng)
			eventually(t, 3*time.Second, "the dispatcher to hit the full queue", func() bool { return queue.attemptCount() > 0 })
			stop()

			bg := context.Background()
			score, err := rdb.ZScore(bg, outboxReadyKey(ns, id), entryID).Result()
			if err != nil {
				t.Fatalf("ZScore ready %s/%q: %v", ns, entryID, err)
			}
			if horizon := float64(time.Now().UnixMilli()); score > horizon {
				t.Fatalf("entry in %s is still leased for another %.0fms after the queue pushed back; "+
					"the release did not reach the namespace's ready set", ns, score-horizon)
			}

			queue.setFull(false)
			runOutboxDispatcher(t, eng)
			eventually(t, 3*time.Second, "the released entry to be delivered once the queue has room", func() bool {
				return len(queue.snapshot()) == 1
			})
		})
	}
}

// TestOutboxDispatcherKeepsRenewingLeasesInTheExecutionsNamespace covers the lease
// keeper. A delivery that outlasts OutboxLeaseRenewInterval must have its lease
// extended, or a live deliverer's entry is stolen and delivered twice. The keeper
// runs on a context derived from the flush's, so it must renew against the
// execution's own namespace.
func TestOutboxDispatcherKeepsRenewingLeasesInTheExecutionsNamespace(t *testing.T) {
	for _, ns := range []namespace.Namespace{namespace.Default, "tenant-x"} {
		t.Run(string(ns), func(t *testing.T) {
			state, rdb, _ := newOutboxIndexTestStore(t)
			ctx := namespace.WithNamespace(context.Background(), ns)
			const id = types.ExecutionID("exec-ns-renew")
			entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))

			queue := &outboxNsQueue{entered: make(chan struct{}), hold: make(chan struct{})}
			entered := queue.entered
			stop := runOutboxDispatcher(t, engine.New(state, queue))
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatalf("the dispatcher never started delivering in namespace %s", ns)
			}

			bg := context.Background()
			first, err := rdb.ZScore(bg, outboxReadyKey(ns, id), entryID).Result()
			if err != nil {
				t.Fatalf("ZScore ready %s/%q: %v", ns, entryID, err)
			}
			eventually(t, 2*engine.OutboxLeaseRenewInterval+time.Second,
				"the lease keeper to extend the held lease in namespace "+string(ns), func() bool {
					score, err := rdb.ZScore(bg, outboxReadyKey(ns, id), entryID).Result()
					return err == nil && score > first
				})
			close(queue.hold)
			eventually(t, 3*time.Second, "the held delivery to finish and be acked", func() bool {
				return outboxDrained(t, rdb, ns, id)
			})
			stop()
		})
	}
}

// TestEntrySeedOutboxIsDeliveredInTheAdmissionNamespace covers the entry-seed
// path, which creates its execution and outbox inside one Lua script and
// registers the readiness index itself. Core stamps the request with the
// namespace of the authenticated context before calling in, so the request and
// the context agree; the registration must land in that namespace's index and
// the dispatcher must then deliver the seeded downstream task from it.
func TestEntrySeedOutboxIsDeliveredInTheAdmissionNamespace(t *testing.T) {
	for _, ns := range []namespace.Namespace{namespace.Default, "tenant-x"} {
		t.Run(string(ns), func(t *testing.T) {
			state, rdb, _ := newOutboxIndexTestStore(t)
			ctx := namespace.WithNamespace(context.Background(), ns)

			g := seedMetricsGraph(t, "ns-entry-seed-"+string(ns))
			gm := g.Groups()[0]
			sinkIdx, ok := g.NodeIndex("sink")
			if !ok {
				t.Fatal("sink node not found in the compiled graph")
			}
			exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
			req := engine.SeedExecutionFromEntryRequest{
				AdmissionKey:    engine.AdmissionKey("ns-entry-seed-" + string(ns)),
				WorkflowID:      "wf-ns-entry-seed",
				WorkflowVersion: "v1",
				Namespace:       ns,
				EntryUnitID:     gm.Name,
				EntryUnitIdx:    gm.UnitIdx,
				Graph:           g,
				Outcome:         engine.GroupOutcomeSuccess,
				Exits:           exits,
				Downstream: []engine.DownstreamArrival{{
					NodeName: "sink", NodeIdx: sinkIdx, UnitIdx: g.UnitIndexForNode(sinkIdx),
					ArrivalCount: 1, ActiveCount: 1,
				}},
				ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
			}
			resp, err := state.SeedExecutionFromEntry(ctx, req)
			if err != nil || resp.State != engine.AdmissionStateAccepted {
				t.Fatalf("SeedExecutionFromEntry() = %+v, %v, want accepted", resp, err)
			}
			id := resp.ExecutionID
			if _, ok := indexScore(t, ctx, rdb, ns, id); !ok {
				t.Fatalf("the seed did not register %q in the %s readiness index", id, ns)
			}
			for _, other := range []namespace.Namespace{namespace.Default, "tenant-x"} {
				if other == ns {
					continue
				}
				if _, ok := indexScore(t, ctx, rdb, other, id); ok {
					t.Fatalf("the seed registered %q in the %s readiness index, want only %s", id, other, ns)
				}
			}

			queue := &outboxNsQueue{}
			stop := runOutboxDispatcher(t, engine.New(state, queue))
			eventually(t, 3*time.Second, "the seeded downstream task to be delivered", func() bool {
				return len(queue.snapshot()) > 0
			})
			eventually(t, 3*time.Second, "the seeded outbox to drain and its index member to be pruned", func() bool {
				return outboxDrained(t, rdb, ns, id)
			})
			stop()
			if got := queue.snapshot(); len(got) != 1 || got[0] != (outboxNsDelivery{ns: ns, id: id}) {
				t.Fatalf("deliveries = %+v, want exactly one for %q under %s", got, id, ns)
			}
		})
	}
}
