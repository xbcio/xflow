package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// OutboxMetrics reports the pending work as due. The fake keeps no
// availability score and takes no delivery lease -- the two things that make a
// pending entry undeliverable on the Redis path -- so every pending entry is
// ready here, which is also what the in-memory StateStore reports.
func (s *pageRecordingState) OutboxMetrics(ctx context.Context) (OutboxMetricsSnapshot, error) {
	snapshot, err := s.outboxObserverState.OutboxMetrics(ctx)
	if err != nil {
		return snapshot, err
	}
	snapshot.Ready = snapshot.Pending
	return snapshot, nil
}

// drainObserver records both dispatcher observations. It implements
// OutboxObserver as well, because that is the option the dispatcher installs
// it through: the drain observations ride the same observer, behind a type
// assert.
type drainObserver struct {
	mu        sync.Mutex
	drains    []drainObservation
	backlogs  []OutboxMetricsSnapshot
	pendingAt []OutboxMetricsSnapshot
}

type drainObservation struct {
	discovered int
	duration   time.Duration
}

func (o *drainObserver) OnOutboxRetry(context.Context, int)                        {}
func (o *drainObserver) OnOutboxDeadLetter(context.Context)                        {}
func (o *drainObserver) OnOutboxReplayed(context.Context, DeadLetterReplayOutcome) {}
func (o *drainObserver) OnOutboxError(context.Context, string, error)              {}

func (o *drainObserver) OnOutboxPending(_ context.Context, pending, deadLettered int, oldestAge time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pendingAt = append(o.pendingAt, OutboxMetricsSnapshot{
		Pending:         pending,
		DeadLettered:    deadLettered,
		OldestPendingAt: time.Now().Add(-oldestAge),
	})
}

func (o *drainObserver) OnOutboxDrain(_ context.Context, discovered int, duration time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.drains = append(o.drains, drainObservation{discovered: discovered, duration: duration})
}

func (o *drainObserver) OnOutboxBacklog(_ context.Context, snapshot OutboxMetricsSnapshot) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.backlogs = append(o.backlogs, snapshot)
}

func (o *drainObserver) drainObservations() []drainObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]drainObservation(nil), o.drains...)
}

func (o *drainObserver) pendingObservations() []OutboxMetricsSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]OutboxMetricsSnapshot(nil), o.pendingAt...)
}

func (o *drainObserver) backlogObservations() []OutboxMetricsSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]OutboxMetricsSnapshot(nil), o.backlogs...)
}

// TestOutboxDispatcherReportsDrainDurationAndDiscovery pins the two numbers an
// operator needs to tell "delivery is slow" from "discovery cannot see the
// backlog". Before this, neither was exported: the only way to read them was
// to count Redis keys by hand, which is why a dispatch ceiling below the
// creation rate stayed invisible for two releases.
func TestOutboxDispatcherReportsDrainDurationAndDiscovery(t *testing.T) {
	ctx := context.Background()
	state := newPageRecordingState()
	observer := &drainObserver{}
	// A queue outage keeps the entry pending across the drain, which is the
	// shape the backlog gauges are for: work that exists and was not delivered.
	eng := New(state, &toggleOutboxQueue{err: errOutboxQueueUnavailable}, WithOutboxObserver(observer))

	id := types.ExecutionID("outbox-drain-observed")
	entry := OutboxEntry{
		ID:   "root/" + string(id) + "/start/0",
		Task: Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	dispatcher := NewOutboxDispatcher(eng, time.Hour)
	dispatcher.drain(ctx)
	waitForBacklogScan(t, dispatcher, state.metricsCallCount, 1)

	drains := observer.drainObservations()
	if len(drains) != 1 {
		t.Fatalf("drain observations = %d, want 1", len(drains))
	}
	if drains[0].discovered != 1 {
		t.Fatalf("discovered = %d, want 1 -- the one execution with ready work", drains[0].discovered)
	}
	if drains[0].duration <= 0 {
		t.Fatalf("drain duration = %v, want positive", drains[0].duration)
	}
	// The backlog scan rides the same drain, and it is the only place the
	// due-now count is known.
	backlogs := observer.backlogObservations()
	if len(backlogs) != 1 {
		t.Fatalf("backlog observations = %d, want 1", len(backlogs))
	}
	if backlogs[0].Pending != 1 || backlogs[0].Ready != 1 {
		t.Fatalf("backlog observation = %+v, want pending=1 ready=1 -- the undelivered entry "+
			"is both still pending and due", backlogs[0])
	}
	if pending := observer.pendingObservations(); len(pending) != 1 {
		t.Fatalf("legacy pending observations = %d, want 1 -- widening the backlog "+
			"observation must not remove the callback every existing observer implements",
			len(pending))
	}
}

// TestOutboxDispatcherReportsADrainThatDiscoveredNothing is the other half of
// the pair: an empty drain is a real observation, not a missing one. Zero
// discovered against a non-zero ready backlog is exactly the discovery-bound
// signal the metric exists to expose, so a drain that finds nothing must still
// report -- including when discovery itself fails.
func TestOutboxDispatcherReportsADrainThatDiscoveredNothing(t *testing.T) {
	ctx := context.Background()
	state := newPageRecordingState()
	observer := &drainObserver{}
	eng := New(state, &toggleOutboxQueue{}, WithOutboxObserver(observer))

	NewOutboxDispatcher(eng, time.Hour).drain(ctx)

	drains := observer.drainObservations()
	if len(drains) != 1 {
		t.Fatalf("drain observations = %d, want 1 -- an empty drain still happened", len(drains))
	}
	if drains[0].discovered != 0 {
		t.Fatalf("discovered = %d, want 0", drains[0].discovered)
	}
}
