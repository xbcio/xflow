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

	if err := pool.Assign(first); err != nil {
		t.Fatalf("first Assign() = %v, want nil", err)
	}
	if err := pool.Assign(second); err != nil {
		t.Fatalf("second Assign() = %v, want nil", err)
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

func TestRunnerPoolAssignReturnsNoCapacityWhenAllRunnersSaturated(t *testing.T) {
	pool := NewRunnerPool()
	pool.Register("runner-1", 1, []protocol.Capability{{NodeType: "xflow.function"}})

	lease := engine.TaskLease{
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "first"},
		NodeType: "xflow.function",
	}
	if err := pool.Assign(lease); err != nil {
		t.Fatalf("first Assign() = %v, want nil", err)
	}
	if err := pool.Assign(lease); err != ErrNoCapacity {
		t.Fatalf("second Assign() = %v, want ErrNoCapacity", err)
	}
}

func TestRunnerPoolAssignPicksRunnerWithMostHeadroom(t *testing.T) {
	pool := NewRunnerPool()
	pool.Register("runner-busy", 1, []protocol.Capability{{NodeType: "xflow.function"}})
	pool.Register("runner-free", 4, []protocol.Capability{{NodeType: "xflow.function"}})

	lease := engine.TaskLease{
		Task:     engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "n"},
		NodeType: "xflow.function",
	}
	if err := pool.Assign(lease); err != nil {
		t.Fatalf("Assign() = %v, want nil", err)
	}

	got, ok := pool.Poll("runner-free", 4, nil)
	if !ok || got.Task.NodeName != "n" {
		t.Fatalf("expected runner-free to receive lease, got ok=%v lease=%+v", ok, got)
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
	if err := pool.Assign(lease); err == nil {
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
