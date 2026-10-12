package control

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// cronReplicaFireKey is the admission key node/trigger/cron derives for one
// scheduled fire: the kind segment is "cron" and the id is the scheduled minute
// in UTC. Two replicas of the same activation evaluate the same schedule and
// must derive this exact string, which is the whole reason the key is built from
// the SCHEDULED time rather than from the moment the firing was observed.
func cronReplicaFireKey(workflowID types.WorkflowID, entryUnitID, scheduledMinuteUTC string) engine.AdmissionKey {
	return engine.AdmissionKey("trigger:" + string(workflowID) + ":" + entryUnitID + ":cron:" + scheduledMinuteUTC)
}

// TestCoreEntrySeedCronTwoReplicasFireOnce drives the control plane's own
// admission — the real engine and its state store, not a fake — with the same
// cron fire key submitted by several replicas at once, and asserts the property
// the key exists for: ONE execution per scheduled fire, and every replica that
// lost the race is told the id of the execution that exists.
//
// It is the control-plane half of the two-replica guarantee. The trigger half,
// cron's TestCronEntrySeedTwoReplicasFireOnce, only proves both replicas DERIVE
// the same key — it runs against an in-process fake that deduplicates by that
// key, so it would still pass if the server folded nothing. This half is what
// makes the loser's answer safe: a replica told "duplicate" can commit its
// watermark or drop its tick knowing the event was handled exactly once.
//
// Concurrency is the point rather than a detail: a sequential accept-then-dedup
// test passes even if admission were a read-modify-write with a window between
// the read and the write, so it could not fail for the reason this test exists.
func TestCoreEntrySeedCronTwoReplicasFireOnce(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	def := entrySeedResolveDef()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Server-side topology resolution is deliberately in play (a remote seed
	// carries only the identity), because that is how a replica reaches the
	// control plane: the runner that hosts the trigger does not send a graph.
	be := local.New()
	eng := engine.New(be.State(), be.Queue())
	reg := be.WorkflowRegistry()
	rec, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             "wf-cron",
		Key:            "default/trig-down@v1",
		Namespace:      string(namespace.Default),
		Name:           "trig-down",
		Version:        "v1",
		DefinitionHash: "sha256:test",
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	core := &Core{engine: eng, workflowRegistry: reg}

	outcome := engine.GroupOutcomeSuccess

	// Two is the shape named by the design ("two replicas"); four is the same
	// claim under a wider race, without which a two-way test could pass by
	// accident on a scheduling order that happens to serialize. Each row fires a
	// DIFFERENT scheduled minute, because that is what successive fires look
	// like: a fresh key every minute, and one execution behind each.
	for _, tc := range []struct {
		replicas        int
		scheduledMinute string
	}{
		{replicas: 2, scheduledMinute: "2026-10-12T03:04:00Z"},
		{replicas: 4, scheduledMinute: "2026-10-12T03:05:00Z"},
	} {
		t.Run(fmt.Sprintf("%d replicas", tc.replicas), func(t *testing.T) {
			exits := []engine.BoundaryExit{{NodeName: "trig", Port: "main", Data: map[string]any{"scheduled_time": tc.scheduledMinute}}}
			fireKey := cronReplicaFireKey(rec.ID, "trig", tc.scheduledMinute)
			req := engine.SeedExecutionFromEntryRequest{
				AdmissionKey:    fireKey,
				WorkflowID:      rec.ID,
				WorkflowVersion: "v1",
				EntryUnitID:     "trig",
				Outcome:         outcome,
				Exits:           exits,
				ResultHash:      engine.ComputeResultHash(outcome, exits),
			}

			// Every replica submits the SAME fire key concurrently.
			responses := make([]engine.SeedExecutionFromEntryResponse, tc.replicas)
			errs := make([]error, tc.replicas)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < tc.replicas; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					responses[i], errs[i] = core.SeedExecutionFromEntry(ctx, req)
				}(i)
			}
			close(start)
			wg.Wait()

			accepted, duplicated, execIDs := 0, 0, map[types.ExecutionID]int{}
			for i, resp := range responses {
				if errs[i] != nil {
					t.Fatalf("replica %d: SeedExecutionFromEntry: %v", i, errs[i])
				}
				if resp.ExecutionID == "" {
					t.Fatalf("replica %d: response carries no execution id (%+v)", i, resp)
				}
				execIDs[resp.ExecutionID]++
				switch {
				case resp.State == engine.AdmissionStateAccepted && !resp.Duplicate:
					accepted++
				case resp.Duplicate:
					duplicated++
				default:
					t.Fatalf("replica %d: state=%q duplicate=%v, want accepted or duplicate", i, resp.State, resp.Duplicate)
				}
			}
			if accepted != 1 {
				t.Fatalf("accepted = %d, want exactly 1: %d replicas firing one schedule produced %d executions",
					accepted, tc.replicas, accepted)
			}
			if duplicated != tc.replicas-1 {
				t.Fatalf("duplicate = %d, want %d", duplicated, tc.replicas-1)
			}
			if len(execIDs) != 1 {
				t.Fatalf("responses named %d distinct executions (%v), want 1", len(execIDs), execIDs)
			}
			for id := range execIDs {
				if want := engine.DeterministicExecutionID(fireKey); id != want {
					t.Fatalf("execution id = %q, want the deterministic %q", id, want)
				}
			}
		})
	}
}
