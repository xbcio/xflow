package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

func TestMemoryRunnerDirectoryEnqueueClaimFinalizeAndRelease(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if session.SessionID == "" {
		t.Fatalf("SessionID is empty")
	}

	assignment := Assignment{
		AssignmentID: "exec-1/node-a/activation-1",
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     "node-a",
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{NodeType: "xflow.function"},
	}
	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatalf("first EnqueueAssignment() enqueued=false, want true")
	}
	enqueued, err = dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("duplicate EnqueueAssignment() error = %v", err)
	}
	if enqueued {
		t.Fatalf("duplicate EnqueueAssignment() enqueued=true, want false")
	}

	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Now:          time.Unix(11, 0),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimForRunner() ok=%v err=%v, want ok", ok, err)
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("claim assignment = %+v, want %+v", claim.Assignment, assignment)
	}

	if err := dir.FinalizeClaim(ctx, claim.ClaimID, &engine.TaskLease{LeaseID: "lease-1"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		AssignmentID: assignment.AssignmentID,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("ReleaseLeased() error = %v", err)
	}
}

func TestMemoryRunnerDirectorySessionFencesStaleRequests(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	first, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	second, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("second Register() error = %v", err)
	}
	if first.SessionID == second.SessionID {
		t.Fatalf("sessions should differ, both %q", first.SessionID)
	}
	if err := dir.Heartbeat(context.Background(), HeartbeatRequest{
		RunnerID:  "runner-1",
		SessionID: first.SessionID,
		Capacity:  1,
		InFlight:  0,
	}); !errors.Is(err, ErrRunnerSessionStale) {
		t.Fatalf("stale Heartbeat() error = %v, want ErrRunnerSessionStale", err)
	}
}

func TestMemoryRunnerDirectoryClaimHeadroomCountsActiveClaimsAndFinalizedLeases(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)

	// A large observed InFlight must NOT reduce headroom: it is a heartbeat
	// observation only. Headroom is Concurrency minus the directory's own
	// in-flight accounting (active claims + finalized leases), so with capacity
	// 2 and nothing claimed yet the runner can still take two tasks.
	if err := dir.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  "runner-1",
		SessionID: session.SessionID,
		Capacity:  2,
		InFlight:  5,
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	first := testAssignment("exec-1/node-a/activation-1")
	second := testAssignment("exec-1/node-b/activation-1")
	third := testAssignment("exec-1/node-c/activation-1")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)
	mustEnqueueAssignment(t, ctx, dir, third)

	// headroom = 2 - 0 claims - 0 leases = 2 (InFlight=5 ignored).
	firstClaim := mustClaimAssignment(t, ctx, dir, session)
	if firstClaim.Assignment.AssignmentID != first.AssignmentID {
		t.Fatalf("first claim assignment = %q, want %q", firstClaim.Assignment.AssignmentID, first.AssignmentID)
	}
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, &engine.TaskLease{LeaseID: "lease-1"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	// headroom = 2 - 0 claims - 1 lease = 1.
	secondClaim := mustClaimAssignment(t, ctx, dir, session)
	if secondClaim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("second claim assignment = %q, want %q", secondClaim.Assignment.AssignmentID, second.AssignmentID)
	}

	// headroom = 2 - 1 claim - 1 lease = 0: no more capacity.
	if _, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 2)); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want no claim when active claims and finalized leases exhaust headroom")
	}

	// Releasing the active claim frees one slot: headroom = 2 - 0 - 1 = 1.
	if err := dir.ReleaseClaim(ctx, secondClaim.ClaimID, ReleaseClaimDrop); err != nil {
		t.Fatalf("ReleaseClaim() error = %v", err)
	}

	thirdClaim := mustClaimAssignment(t, ctx, dir, session)
	if thirdClaim.Assignment.AssignmentID != third.AssignmentID {
		t.Fatalf("third claim assignment = %q, want %q", thirdClaim.Assignment.AssignmentID, third.AssignmentID)
	}
}

func TestMemoryRunnerDirectoryReleaseClaimSemantics(t *testing.T) {
	tests := []struct {
		name              string
		reason            ReleaseClaimReason
		wantNext          AssignmentID
		wantReenqueueAOne bool
	}{
		{
			name:              "requeue keeps seen and returns assignment to front",
			reason:            ReleaseClaimRequeue,
			wantNext:          AssignmentID("exec-1/node-a/activation-1"),
			wantReenqueueAOne: false,
		},
		{
			name:              "drop clears seen and discards assignment",
			reason:            ReleaseClaimDrop,
			wantNext:          AssignmentID("exec-1/node-b/activation-1"),
			wantReenqueueAOne: true,
		},
		{
			name:              "keep seen frees accounting without requeue",
			reason:            ReleaseClaimKeepSeen,
			wantNext:          AssignmentID("exec-1/node-b/activation-1"),
			wantReenqueueAOne: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := NewMemoryRunnerDirectory()
			session := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 1)

			first := testAssignment("exec-1/node-a/activation-1")
			second := testAssignment("exec-1/node-b/activation-1")
			mustEnqueueAssignment(t, ctx, dir, first)
			mustEnqueueAssignment(t, ctx, dir, second)

			claim := mustClaimAssignment(t, ctx, dir, session)
			if claim.Assignment.AssignmentID != first.AssignmentID {
				t.Fatalf("claim assignment = %q, want %q", claim.Assignment.AssignmentID, first.AssignmentID)
			}

			if err := dir.ReleaseClaim(ctx, claim.ClaimID, tt.reason); err != nil {
				t.Fatalf("ReleaseClaim() error = %v", err)
			}

			nextClaim := mustClaimAssignment(t, ctx, dir, session)
			if nextClaim.Assignment.AssignmentID != tt.wantNext {
				t.Fatalf("next claim assignment = %q, want %q", nextClaim.Assignment.AssignmentID, tt.wantNext)
			}

			enqueued, err := dir.EnqueueAssignment(ctx, first)
			if err != nil {
				t.Fatalf("EnqueueAssignment() error = %v", err)
			}
			if enqueued != tt.wantReenqueueAOne {
				t.Fatalf("EnqueueAssignment() after %s = %v, want %v", tt.reason, enqueued, tt.wantReenqueueAOne)
			}
		})
	}
}

func TestMemoryRunnerDirectoryReleaseLeasedControlsSeenRemoval(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)

	first := testAssignment("exec-1/node-a/activation-1")
	second := testAssignment("exec-1/node-b/activation-1")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	firstClaim := mustClaimAssignment(t, ctx, dir, session)
	secondClaim := mustClaimAssignment(t, ctx, dir, session)
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, &engine.TaskLease{LeaseID: "lease-1"}); err != nil {
		t.Fatalf("FinalizeClaim(first) error = %v", err)
	}
	if err := dir.FinalizeClaim(ctx, secondClaim.ClaimID, &engine.TaskLease{LeaseID: "lease-2"}); err != nil {
		t.Fatalf("FinalizeClaim(second) error = %v", err)
	}

	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		AssignmentID: first.AssignmentID,
		RemoveSeen:   false,
	}); err != nil {
		t.Fatalf("ReleaseLeased(first) error = %v", err)
	}
	if err := dir.ReleaseLeased(ctx, ReleaseLeasedRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		AssignmentID: second.AssignmentID,
		RemoveSeen:   true,
	}); err != nil {
		t.Fatalf("ReleaseLeased(second) error = %v", err)
	}

	if enqueued, err := dir.EnqueueAssignment(ctx, first); err != nil {
		t.Fatalf("EnqueueAssignment(first) error = %v", err)
	} else if enqueued {
		t.Fatal("EnqueueAssignment(first) enqueued=true, want false when seen marker remains")
	}
	if enqueued, err := dir.EnqueueAssignment(ctx, second); err != nil {
		t.Fatalf("EnqueueAssignment(second) error = %v", err)
	} else if !enqueued {
		t.Fatal("EnqueueAssignment(second) enqueued=false, want true after RemoveSeen")
	}
}

func TestMemoryRunnerDirectoryReregisterPreservesFinalizedLeasesAndRequeuesActiveClaims(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	firstSession := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)

	first := testAssignment("exec-1/node-a/activation-1")
	second := testAssignment("exec-1/node-b/activation-1")
	third := testAssignment("exec-1/node-c/activation-1")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)
	mustEnqueueAssignment(t, ctx, dir, third)

	firstClaim := mustClaimAssignment(t, ctx, dir, firstSession)
	if err := dir.FinalizeClaim(ctx, firstClaim.ClaimID, &engine.TaskLease{LeaseID: "lease-1"}); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}

	secondClaim := mustClaimAssignment(t, ctx, dir, firstSession)
	if secondClaim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("second claim assignment = %q, want %q", secondClaim.Assignment.AssignmentID, second.AssignmentID)
	}

	secondSession := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)
	if firstSession.SessionID == secondSession.SessionID {
		t.Fatalf("sessions should differ, both %q", firstSession.SessionID)
	}

	reclaimed := mustClaimAssignment(t, ctx, dir, secondSession)
	if reclaimed.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("reclaimed assignment = %q, want %q", reclaimed.Assignment.AssignmentID, second.AssignmentID)
	}

	if _, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(secondSession, 2)); err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() ok=true, want no second claim while finalized lease still consumes headroom after re-registration")
	}
}

func TestMemoryRunnerDirectoryReregisterDoesNotLetStaleInflightGateHeadroom(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	firstSession := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)

	// The prior session reported one task in flight, but holds no finalized
	// lease or active claim (e.g. the task completed and was released just
	// before the reconnect). InFlight is a pure observation, so it must NOT be
	// carried forward as a headroom gate across re-registration — otherwise the
	// replacement session's effective concurrency is silently suppressed.
	if err := dir.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  "runner-1",
		SessionID: firstSession.SessionID,
		Capacity:  2,
		InFlight:  1,
		Now:       time.Unix(12, 0),
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	first := testAssignment("exec-1/node-a/activation-1")
	second := testAssignment("exec-1/node-b/activation-1")
	mustEnqueueAssignment(t, ctx, dir, first)
	mustEnqueueAssignment(t, ctx, dir, second)

	secondSession := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 2)
	if firstSession.SessionID == secondSession.SessionID {
		t.Fatalf("sessions should differ, both %q", firstSession.SessionID)
	}

	// Full capacity 2 is available: no real in-flight work (lease/claim)
	// survives, and the stale InFlight=1 must not reduce headroom.
	if claim := mustClaimAssignment(t, ctx, dir, secondSession); claim.Assignment.AssignmentID != first.AssignmentID {
		t.Fatalf("first claim = %q, want %q", claim.Assignment.AssignmentID, first.AssignmentID)
	}
	if claim, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(secondSession, 2)); err != nil || !ok {
		t.Fatalf("second ClaimForRunner() ok=%v err=%v, want claim (stale InFlight must not gate headroom)", ok, err)
	} else if claim.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("second claim = %q, want %q", claim.Assignment.AssignmentID, second.AssignmentID)
	}
}

func mustRegisterMemoryRunner(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, runnerID string, capacity int) RunnerSession {
	t.Helper()

	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     capacity,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

func mustEnqueueAssignment(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, assignment Assignment) {
	t.Helper()

	enqueued, err := dir.EnqueueAssignment(ctx, assignment)
	if err != nil {
		t.Fatalf("EnqueueAssignment() error = %v", err)
	}
	if !enqueued {
		t.Fatalf("EnqueueAssignment() enqueued=false for %q", assignment.AssignmentID)
	}
}

func mustClaimAssignment(t *testing.T, ctx context.Context, dir *MemoryRunnerDirectory, session RunnerSession) Claim {
	t.Helper()

	claim, ok, err := dir.ClaimForRunner(ctx, testClaimRequest(session, 4))
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() ok=false, want claim")
	}
	return claim
}

func testClaimRequest(session RunnerSession, capacity int) ClaimRequest {
	return ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     capacity,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Now:          time.Unix(11, 0),
	}
}

func testAssignment(id AssignmentID) Assignment {
	return Assignment{
		AssignmentID: id,
		Task: engine.Task{
			ExecutionID:  "exec-1",
			NodeName:     string(id),
			NodeIdx:      0,
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{NodeType: "xflow.function"},
	}
}

func TestMemoryRunnerDirectoryNamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()

	// Runner A serves only namespace A.
	sessionA, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-a",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Namespaces:   []namespace.Namespace{"namespace-a"},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register(runner-a) error = %v", err)
	}

	// Place a namespace-b assignment.
	bAssignment := testAssignment("exec-b/node-b/activation-1")
	bAssignment.Namespace = "namespace-b"
	mustEnqueueAssignment(t, ctx, dir, bAssignment)

	// Runner A must not claim the cross-namespace assignment.
	_, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-a",
		SessionID:    sessionA.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if ok {
		t.Fatal("runner-a claimed namespace-b assignment, want cross-namespace isolation")
	}

	// Runner B serving namespace-b can claim it.
	sessionB, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-b",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Namespaces:   []namespace.Namespace{"namespace-b"},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register(runner-b) error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-b",
		SessionID:    sessionB.SessionID,
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(runner-b) error = %v", err)
	}
	if !ok {
		t.Fatal("runner-b did not claim namespace-b assignment")
	}
	if claim.Assignment.Namespace != "namespace-b" {
		t.Fatalf("claimed assignment namespace = %q, want namespace-b", claim.Assignment.Namespace)
	}
}

func TestMemoryRunnerDirectoryDefaultNamespaceBackCompat(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	// No namespaces configured -> defaults to ["default"].
	session := mustRegisterMemoryRunner(t, ctx, dir, "runner-1", 1)

	assignment := testAssignment("exec-1/node-a/activation-1")
	assignment.Namespace = namespace.Default
	mustEnqueueAssignment(t, ctx, dir, assignment)

	claim := mustClaimAssignment(t, ctx, dir, session)
	if claim.Assignment.Namespace != namespace.Default {
		t.Fatalf("namespace = %q, want default", claim.Assignment.Namespace)
	}
}
