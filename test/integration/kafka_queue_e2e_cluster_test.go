//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// e2eCountHandler records, per execution, how many times it ran and on which
// process it ran. Every process of the cluster registers its own instance under
// the same node type, so the per-process counters show who executed what.
type e2eCountHandler struct {
	typ  string
	proc string
	mu   *sync.Mutex
	// runs maps execution id to the processes that executed the node.
	runs map[types.ExecutionID][]string
}

func (h *e2eCountHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.typ} }

func (h *e2eCountHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	id := types.ExecutionID(in.ExecutionID)
	h.mu.Lock()
	h.runs[id] = append(h.runs[id], h.proc)
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"proc": h.proc}}, nil
}

// waitGroupMembers polls until the consumer group has at least want members.
func waitGroupMembers(ctx context.Context, t *testing.T, env *kafkaE2EEnv, group string, want int) {
	t.Helper()
	cl := &kafka.Client{Addr: kafka.TCP(env.brokers...), Timeout: 10 * time.Second}
	pollUntil(ctx, t, group+" to reach the expected member count", func() (bool, error) {
		resp, err := cl.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{Addr: cl.Addr, GroupIDs: []string{group}})
		if err != nil {
			return false, err
		}
		for _, g := range resp.Groups {
			if g.Error == nil && g.GroupState == "Stable" && len(g.Members) >= want {
				return true, nil
			}
		}
		return false, nil
	})
}

// submitBurst submits n executions concurrently through producer and returns
// their ids. The two-node workflow is start -> work, so a successful execution
// has exactly one handler run of type typ.
func submitBurst(t *testing.T, env *kafkaE2EEnv, producer *kafkaE2ENode, typ string, n int) []types.ExecutionID {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:        "kafka-e2e-cluster",
		Nodes:       []types.NodeDef{{Name: "start", Type: "xflow.start"}, {Name: "work", Type: typ}},
		Connections: types.Connections{"start": {"main": {Targets: []types.Connection{{Node: "work", Input: "main"}}}}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := env.ctx(t, 60*time.Second)
	ids := make([]types.ExecutionID, n)
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := producer.eng.Submit(ctx, g, map[string]any{"i": i})
			if err != nil {
				failed.Add(1)
				return
			}
			ids[i] = id
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d of %d submits failed", failed.Load(), n)
	}
	return ids
}

// TestKafkaQueueE2E_Cluster runs one deployment as three processes sharing a
// suffix: A only produces, B and C consume. Work submitted on A must be executed
// on B/C, each node exactly once, and every execution must reach success.
func TestKafkaQueueE2E_Cluster(t *testing.T) {
	const typ = "test.e2e.count"
	env := newKafkaE2EEnv(t)
	var mu sync.Mutex
	runs := map[types.ExecutionID][]string{}
	reg := func(proc string) func(engine.HandlerRegistrar) {
		return func(r engine.HandlerRegistrar) {
			r.RegisterGlobal(typ, &e2eCountHandler{typ: typ, proc: proc, mu: &mu, runs: runs})
		}
	}
	a := startKafkaE2ENode(t, env, kafkaE2EOpts{producerOnly: true, register: reg("A")})
	b := startKafkaE2ENode(t, env, kafkaE2EOpts{concurrency: 2, register: reg("B")})
	ctx := env.ctx(t, 90*time.Second)
	group := env.groups()[0]
	waitGroupMembers(ctx, t, env, group, 1)

	check := func(t *testing.T, ids []types.ExecutionID, allowed ...string) map[string]int {
		t.Helper()
		perProc := map[string]int{}
		for _, id := range ids {
			res := waitForCompletion(ctx, t, a.b.State(), id, "work")
			if res.Status != types.ExecutionStatusSuccess {
				t.Fatalf("execution %s ended %s, want success", id, res.Status)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ids {
			got := runs[id]
			if len(got) != 1 {
				t.Fatalf("execution %s: node ran %d times on %v, want exactly once", id, len(got), got)
			}
			ok := false
			for _, p := range allowed {
				ok = ok || got[0] == p
			}
			if !ok {
				t.Fatalf("execution %s ran on %q, want one of %v", id, got[0], allowed)
			}
			perProc[got[0]]++
		}
		return perProc
	}

	t.Run("producer_submits_consumer_executes", func(t *testing.T) {
		ids := submitBurst(t, env, a, typ, 3)
		if got := check(t, ids, "B"); got["B"] != 3 {
			t.Fatalf("per-process runs = %v, want all 3 on B", got)
		}
	})

	t.Run("second_consumer_joins", func(t *testing.T) {
		startKafkaE2ENode(t, env, kafkaE2EOpts{concurrency: 2, register: reg("C")})
		waitGroupMembers(ctx, t, env, group, 2)
		ids := submitBurst(t, env, a, typ, 12)
		got := check(t, ids, "B", "C")
		t.Logf("12 executions: per-process runs = %v", got)
	})
	_ = b
}
