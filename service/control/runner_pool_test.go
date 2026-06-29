package control

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestRunnerPoolAssignsMatchingLeaseAndPollReturnsFIFO(t *testing.T) {
	pool := NewRunnerPool()
	pool.Register("runner-1", 2, []protocol.Capability{{NodeType: "xflow.function"}})

	first := engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "first"},
		NodeType: "xflow.function",
	}
	second := engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-2"),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "second"},
		NodeType: "xflow.function",
	}

	if !pool.Assign(first) {
		t.Fatal("expected first assignment to matching runner")
	}
	if !pool.Assign(second) {
		t.Fatal("expected second assignment to matching runner")
	}

	got, ok := pool.Poll("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})
	if !ok {
		t.Fatal("expected assigned lease")
	}
	if got.LeaseID != first.LeaseID {
		t.Fatalf("first poll lease = %q, want %q", got.LeaseID, first.LeaseID)
	}

	got, ok = pool.Poll("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})
	if !ok {
		t.Fatal("expected second assigned lease")
	}
	if got.LeaseID != second.LeaseID {
		t.Fatalf("second poll lease = %q, want %q", got.LeaseID, second.LeaseID)
	}
}

func TestRunnerPoolRejectsNonMatchingCapability(t *testing.T) {
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.http"}})

	lease := engine.TaskLease{
		LeaseID:  engine.LeaseID("lease-1"),
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		NodeType: "xflow.function",
	}
	if pool.Assign(lease) {
		t.Fatal("expected assignment to fail without matching runner")
	}

	if got, ok := pool.Poll("runner-1", 1, []protocol.Capability{{NodeType: "xflow.http"}}); ok {
		t.Fatalf("poll returned unexpected lease: %+v", got)
	}
}

func TestRunnerPoolHeartbeatUpdatesCapacityInFlightAndTimestamp(t *testing.T) {
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	now := time.Unix(123, 0)
	if !pool.Heartbeat("runner-1", 4, 2, now) {
		t.Fatal("expected heartbeat for registered runner")
	}

	snapshot, ok := pool.Runner("runner-1")
	if !ok {
		t.Fatal("runner not found")
	}
	if snapshot.Capacity != 4 || snapshot.InFlight != 2 {
		t.Fatalf("capacity/inflight = %d/%d, want 4/2", snapshot.Capacity, snapshot.InFlight)
	}
	if !snapshot.LastHeartbeat.Equal(now) {
		t.Fatalf("last heartbeat = %s, want %s", snapshot.LastHeartbeat, now)
	}
}
