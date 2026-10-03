package examples_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// suspensionHooks counts OnNodeSuspended per node name. The approval examples
// share it for two jobs: a test sends each signal only once the gate is waiting
// for it, and the counts of a finished run are asserted exactly — the waits can
// only check >=, so the exact numbers are what pin a duplicated or missing
// suspension. The hook fires on an engine worker goroutine while the test
// goroutine polls, so the map is guarded.
type suspensionHooks struct {
	engine.BaseHooks

	mu     sync.Mutex
	counts map[string]int
}

func newSuspensionHooks() *suspensionHooks {
	return &suspensionHooks{counts: make(map[string]int)}
}

func (h *suspensionHooks) OnNodeSuspended(_ context.Context, _ types.ExecutionID, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[name]++
}

// count reads one node's counter.
func (h *suspensionHooks) count(nodeName string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[nodeName]
}

// finalCounts reads two counters under one lock hold. Called after the run has
// finished, so the values are terminal rather than a threshold crossing.
func (h *suspensionHooks) finalCounts(a, b string) (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[a], h.counts[b]
}

// waitForSuspensions blocks until the node's counter reaches want. ctx bounds
// the wait: a suspension that never arrives must fail the caller rather than
// park it until the package timeout.
func (h *suspensionHooks) waitForSuspensions(t *testing.T, ctx context.Context, nodeName string, want int) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if h.count(nodeName) >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s suspension count >= %d: %v", nodeName, want, ctx.Err())
		case <-ticker.C:
		}
	}
}
