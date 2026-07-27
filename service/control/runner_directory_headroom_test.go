package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// runnerDirectoryFactory builds a fresh RunnerDirectory for a contract test.
type runnerDirectoryFactory struct {
	name  string
	build func(t *testing.T) RunnerDirectory
}

func runnerDirectoryContractFactories() []runnerDirectoryFactory {
	return []runnerDirectoryFactory{
		{
			name:  "memory",
			build: func(t *testing.T) RunnerDirectory { return NewMemoryRunnerDirectory() },
		},
		{
			name: "redis",
			build: func(t *testing.T) RunnerDirectory {
				// miniredis-backed: runs in the standard unit test suite without a
				// real Redis daemon, so the Redis Lua path is covered here too.
				_, rdb := newRedisRunnerDirectoryTestClient(t)
				return NewRedisRunnerDirectory(rdb)
			},
		},
	}
}

func headroomTestAssignment(id AssignmentID) Assignment {
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

// TestRunnerDirectoryHeadroomIgnoresInflightAndMatchesConcurrency pins the H2
// contract on BOTH directory implementations: with N tasks in flight a runner
// of capacity C can claim exactly C-N more tasks. The heartbeat-reported
// InFlight observation must never reduce that headroom, and a poll's advertised
// Capacity must never overwrite the authoritative total written by
// Register/Heartbeat.
//
// In-flight tasks here are modeled as active (unfinalized) claims so the two
// backends stay directly comparable: the Redis directory replays finalized
// leases before claiming new queued work, so mixing finalized leases into a
// shared contract would diverge from the memory directory. Lease counting in
// the headroom gate is covered per-backend (see the memory directory test).
func TestRunnerDirectoryHeadroomIgnoresInflightAndMatchesConcurrency(t *testing.T) {
	const concurrency = 4
	caps := []protocol.Capability{{NodeType: "xflow.function"}}

	claimReq := func(session RunnerSession, capacity int) ClaimRequest {
		return ClaimRequest{
			RunnerID:     session.RunnerID,
			SessionID:    session.SessionID,
			Capacity:     capacity,
			Capabilities: caps,
			Now:          time.Unix(11, 0),
		}
	}

	for _, factory := range runnerDirectoryContractFactories() {
		t.Run(factory.name, func(t *testing.T) {
			ctx := context.Background()
			dir := factory.build(t)

			session, err := dir.Register(ctx, RegisterRunnerRequest{
				RunnerID:     "runner-1",
				Capacity:     concurrency,
				Capabilities: caps,
				Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
				Now:          time.Unix(10, 0),
			})
			if err != nil {
				t.Fatalf("Register() error = %v", err)
			}

			// A large observed InFlight must not shrink headroom: it is a pure
			// observation, not a gate.
			if err := dir.Heartbeat(ctx, HeartbeatRequest{
				RunnerID:  "runner-1",
				SessionID: session.SessionID,
				Capacity:  concurrency,
				InFlight:  99,
				Now:       time.Unix(11, 0),
			}); err != nil {
				t.Fatalf("Heartbeat() error = %v", err)
			}

			ids := []AssignmentID{
				"exec-1/node-a/activation-1",
				"exec-1/node-b/activation-1",
				"exec-1/node-c/activation-1",
				"exec-1/node-d/activation-1",
				"exec-1/node-e/activation-1",
			}
			for _, id := range ids {
				enqueued, err := dir.EnqueueAssignment(ctx, headroomTestAssignment(id))
				if err != nil {
					t.Fatalf("EnqueueAssignment(%q) error = %v", id, err)
				}
				if !enqueued {
					t.Fatalf("EnqueueAssignment(%q) enqueued=false", id)
				}
			}

			// Claim one assignment and leave it active (in flight #1). The claim
			// deliberately advertises a bogus remainder Capacity=1; the directory
			// must ignore it and keep using the authoritative total (4).
			active, ok, err := dir.ClaimForRunner(ctx, claimReq(session, 1))
			if err != nil || !ok {
				t.Fatalf("first ClaimForRunner() ok=%v err=%v, want claim", ok, err)
			}

			// The advertised total capacity must be untouched by the bogus poll.
			if snap, ok := dir.Runner(ctx, "runner-1"); !ok {
				t.Fatal("Runner() ok=false, want snapshot")
			} else if snap.Capacity != concurrency {
				t.Fatalf("snapshot Capacity = %d, want %d (poll must not overwrite authoritative total)", snap.Capacity, concurrency)
			}

			// One task in flight → headroom = 4-1 = 3. Fill the remaining three.
			for i := 0; i < concurrency-1; i++ {
				if _, ok, err := dir.ClaimForRunner(ctx, claimReq(session, concurrency)); err != nil || !ok {
					t.Fatalf("fill claim #%d ok=%v err=%v, want claim (headroom not exhausted)", i, ok, err)
				}
			}

			// Headroom is now exactly 0 (C tasks in flight); the next claim fails
			// even though a queued assignment remains.
			if _, ok, err := dir.ClaimForRunner(ctx, claimReq(session, concurrency)); err != nil {
				t.Fatalf("over-capacity ClaimForRunner() error = %v", err)
			} else if ok {
				t.Fatal("ClaimForRunner() ok=true past capacity; headroom must equal Concurrency-inflight")
			}

			// Release the first active claim: headroom frees exactly one slot.
			if err := dir.ReleaseClaim(ctx, active.ClaimID, ReleaseClaimDrop); err != nil {
				t.Fatalf("ReleaseClaim() error = %v", err)
			}
			if _, ok, err := dir.ClaimForRunner(ctx, claimReq(session, concurrency)); err != nil || !ok {
				t.Fatalf("post-release ClaimForRunner() ok=%v err=%v, want claim", ok, err)
			}
		})
	}
}
