package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// pageRecordingState is the outbox-observer fake with discovery instrumented:
// it records the limit every ListOutboxExecutions call was given, so a test can
// assert what the dispatcher actually asked the store for rather than what it
// was configured with.
type pageRecordingState struct {
	*outboxObserverState
	mu     sync.Mutex
	limits []int
	// delay is how long every discovery call blocks, which is how an
	// overrunning drain is produced deterministically.
	delay time.Duration
}

func newPageRecordingState() *pageRecordingState {
	return &pageRecordingState{outboxObserverState: newOutboxObserverState()}
}

func (s *pageRecordingState) ListOutboxExecutions(ctx context.Context, limit int) ([]types.ExecutionID, error) {
	s.mu.Lock()
	s.limits = append(s.limits, limit)
	delay := s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
	return s.outboxObserverState.ListOutboxExecutions(ctx, limit)
}

func (s *pageRecordingState) discoveryLimits() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.limits...)
}

func (s *pageRecordingState) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.limits)
}

// TestOutboxDispatcherAsksForTheConfiguredDiscoveryPage pins the knob itself.
// The page is the dispatcher's discovery ceiling on a keyspace-scanned store:
// SCAN's COUNT counts keys examined, so the share of the ready backlog one
// drain reaches is page over total keys, and a deployment whose keyspace has
// outgrown a fixed 256 discovers work more slowly than it is created. The host
// has to be able to raise it, and the dispatcher has to actually ask for it.
func TestOutboxDispatcherAsksForTheConfiguredDiscoveryPage(t *testing.T) {
	tests := []struct {
		name string
		opts []OutboxDispatcherOption
		want int
	}{
		{name: "default", want: DefaultOutboxDiscoveryPage},
		{name: "configured", opts: []OutboxDispatcherOption{WithOutboxDiscoveryPage(8192)}, want: 8192},
		{name: "zero keeps the default", opts: []OutboxDispatcherOption{WithOutboxDiscoveryPage(0)}, want: DefaultOutboxDiscoveryPage},
		{name: "negative keeps the default", opts: []OutboxDispatcherOption{WithOutboxDiscoveryPage(-1)}, want: DefaultOutboxDiscoveryPage},
		{name: "nil option is ignored", opts: []OutboxDispatcherOption{nil}, want: DefaultOutboxDiscoveryPage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newPageRecordingState()
			eng := New(state, &toggleOutboxQueue{})
			// A one-hour interval keeps this to a single drain.
			NewOutboxDispatcher(eng, time.Hour, tt.opts...).drain(context.Background())

			limits := state.discoveryLimits()
			if len(limits) != 1 {
				t.Fatalf("ListOutboxExecutions() called %d times, want 1", len(limits))
			}
			if limits[0] != tt.want {
				t.Fatalf("discovery limit = %d, want %d", limits[0], tt.want)
			}
		})
	}
}

// TestDefaultOutboxDiscoveryPageCoversAKeyspaceInFewerDrains records why the
// default is not 256 any more, in the form a reviewer can check: the number of
// drains a full cursor round trip takes is keyspace over page. At the keyspace
// size the throughput defect was measured on (a five-figure key count), 256
// needs tens of drains per round trip while 2048 needs ten.
func TestDefaultOutboxDiscoveryPageCoversAKeyspaceInFewerDrains(t *testing.T) {
	const keyspace = 20000
	const measuredPage = 256
	old := (keyspace + measuredPage - 1) / measuredPage
	got := (keyspace + DefaultOutboxDiscoveryPage - 1) / DefaultOutboxDiscoveryPage
	if got >= old {
		t.Fatalf("DefaultOutboxDiscoveryPage = %d covers a %d-key keyspace in %d drains, "+
			"which is not fewer than the %d drains the previous %d took",
			DefaultOutboxDiscoveryPage, keyspace, got, old, measuredPage)
	}
	if got > 10 {
		t.Fatalf("DefaultOutboxDiscoveryPage = %d needs %d drains to cover %d keys, want at "+
			"most 10 -- beyond that a new execution waits seconds before any dispatcher sees it",
			DefaultOutboxDiscoveryPage, got, keyspace)
	}
}
