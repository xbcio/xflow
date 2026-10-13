package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestBackendSweepsQueueTimeouts drives the loop the in-process deployment
// owns. The store knows how to expire a queued execution; something has to ask
// it to, and in local mode this backend is the only background owner there is.
// Without it queue_timeout works in cluster mode and is silently ignored
// in-process — the same definition behaving differently per deployment, with
// nothing in the API to tell the two apart.
func TestBackendSweepsQueueTimeouts(t *testing.T) {
	ctx := context.Background()
	b := New()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "queue-timeout-sweep",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	holder := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeReject,
	}
	waiting := holder
	waiting.Mode = types.ConcurrencyModeQueue
	waiting.QueueTimeout = time.Millisecond

	firstSnap, firstEntries := sweepSubmission(t, g)
	if _, err := b.state.CreateExecutionWithConcurrency(ctx, firstSnap, firstEntries, holder); err != nil {
		t.Fatalf("holder submission error = %v", err)
	}
	queuedSnap, queuedEntries := sweepSubmission(t, g)
	res, err := b.state.CreateExecutionWithConcurrency(ctx, queuedSnap, queuedEntries, waiting)
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("over-limit submission = %+v, want it parked", res)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runQueueTimeoutSweeper(runCtx, 5*time.Millisecond)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, err := b.state.GetExecution(ctx, queuedSnap.ID)
		if err != nil {
			t.Fatalf("GetExecution() error = %v", err)
		}
		if snap != nil && snap.Status == types.ExecutionStatusTimeout {
			if snap.Error != engine.ConcurrencyQueueTimeoutError {
				t.Fatalf("expired execution reason = %q, want %q", snap.Error, engine.ConcurrencyQueueTimeoutError)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued execution status after the sweep deadline = %v, want %q", snap, types.ExecutionStatusTimeout)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The holder is untouched: expiring a waiter must not disturb the
	// execution that owns the slot.
	snap, err := b.state.GetExecution(ctx, firstSnap.ID)
	if err != nil {
		t.Fatalf("GetExecution(holder) error = %v", err)
	}
	if snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("holder after the sweep = %+v, want it still running", snap)
	}
}

// sweepSubmission builds one submission the way the engine would: a fresh ID, a
// running snapshot and its single root intent.
func sweepSubmission(t *testing.T, g *graph.Graph) (*engine.ExecutionSnapshot, []engine.OutboxEntry) {
	t.Helper()
	id := engine.NewExecutionID()
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	entries := []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeExec,
		},
	}}
	return snap, entries
}
