package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// blockingBacklogState blocks the backlog scan until it is released, which is
// what a keyspace-wide scan does on a large or slow instance: it does not fail,
// it just takes longer than the delivery tick.
type blockingBacklogState struct {
	*pageRecordingState
	entered chan struct{}
	gate    chan struct{}
}

func (s *blockingBacklogState) OutboxMetrics(ctx context.Context) (OutboxMetricsSnapshot, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
	}
	return s.pageRecordingState.OutboxMetrics(ctx)
}

// waitForBacklogScan waits until the store has served wantCalls scans and no
// scan is still in flight.
//
// Both halves are needed. The scan runs off the delivery goroutine, so the count
// is not incremented by the time drain returns; and a scan still in flight makes
// the next drain skip its own, because at most one runs at a time.
func waitForBacklogScan(t *testing.T, d *OutboxDispatcher, calls func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calls() == want && !d.metricsRunning.Load() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("backlog scans = %d (in flight = %v), want %d settled",
		calls(), d.metricsRunning.Load(), want)
}

// TestOutboxDispatcherDeliversWhileTheBacklogScanIsRunning pins that the
// backlog scan cannot starve delivery.
//
// The scan walks the whole keyspace and does per-key round trips, so its cost
// grows with the backlog it measures. Run inline it made the delivery loop wait
// on the measurement of its own backlog: while one scan ran, nothing was
// delivered — which is the state that keeps the backlog, and the executions
// behind it, from ever draining. Observed as every dispatch stalling for
// minutes at a time while the outbox stayed full.
func TestOutboxDispatcherDeliversWhileTheBacklogScanIsRunning(t *testing.T) {
	ctx := context.Background()
	state := &blockingBacklogState{
		pageRecordingState: newPageRecordingState(),
		entered:            make(chan struct{}, 1),
		gate:               make(chan struct{}),
	}
	observer := &drainObserver{}
	eng := New(state, &toggleOutboxQueue{}, WithOutboxObserver(observer))

	id := types.ExecutionID("outbox-metrics-not-inline")
	entry := OutboxEntry{
		ID:   "execute/" + string(id) + "/sink/0",
		Task: Task{ExecutionID: id, NodeName: "sink", NodeIdx: 0, Type: TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx,
		&ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning}, []OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		NewOutboxDispatcher(eng, time.Hour).drain(ctx)
	}()

	select {
	case <-state.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the backlog scan never started; this test cannot say anything about delivery")
	}

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		close(state.gate)
		t.Fatal("the drain did not return while the backlog scan was still running: delivery is " +
			"serialised behind a scan whose cost grows with the backlog it measures, so a large " +
			"backlog stops itself from draining")
	}

	drains := observer.drainObservations()
	if len(drains) != 1 || drains[0].discovered != 1 {
		t.Fatalf("drain observations = %+v, want one drain that discovered the one execution "+
			"with ready work", drains)
	}

	close(state.gate)
}
