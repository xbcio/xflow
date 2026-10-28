//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// e2eWaitDef is start -> pre -> w (xflow.wait) -> post, with w's timeout port
// leading to on_timeout. waitParams is w's parameter map, so one definition
// serves the timer, signal and signal-timeout runs.
func e2eWaitDef(waitParams map[string]any) *types.WorkflowDef {
	fn := func(name, code string) types.NodeDef {
		return types.NodeDef{Name: name, Type: "xflow.function", Parameters: map[string]any{"code": code}}
	}
	link := func(to string) types.PortConnections {
		return types.PortConnections{Targets: []types.Connection{{Node: to, Input: "main"}}}
	}
	return &types.WorkflowDef{
		Name: "kafka-e2e-wait",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			fn("pre", `{"stage": "before-wait"}`),
			{Name: "w", Type: "xflow.wait", Parameters: waitParams},
			fn("post", `{"stage": "after-wait"}`),
			fn("on_timeout", `{"stage": "timed-out"}`),
			{Name: "end_ok", Type: "xflow.end"},
			{Name: "end_to", Type: "xflow.end"},
		},
		Connections: types.Connections{
			"start":      {"main": link("pre")},
			"pre":        {"main": link("w")},
			"w":          {"main": link("post"), "timeout": link("on_timeout")},
			"post":       {"main": link("end_ok")},
			"on_timeout": {"main": link("end_to")},
		},
	}
}

// submitWait compiles and submits e2eWaitDef and returns the execution id.
func submitWait(t *testing.T, env *kafkaE2EEnv, node *kafkaE2ENode, waitParams map[string]any) types.ExecutionID {
	t.Helper()
	g, err := graph.Compile(e2eWaitDef(waitParams))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id, err := node.eng.Submit(env.ctx(t, 60*time.Second), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return id
}

// nodeStatusOf reads one node's status; "" when it has no snapshot yet.
func nodeStatusOf(t *testing.T, env *kafkaE2EEnv, node *kafkaE2ENode, id types.ExecutionID, name string) types.NodeStatus {
	t.Helper()
	snap, err := node.b.State().GetNode(env.ctx(t, 10*time.Second), id, name)
	if err != nil {
		t.Fatalf("GetNode(%s): %v", name, err)
	}
	if snap == nil {
		return ""
	}
	return snap.Status
}

// TestKafkaQueueE2E_Resume checks that a suspended node is resumed through
// outbox -> Kafka and that the workflow continues past it, for the timer mode,
// the signal mode, and the signal-timeout mode (timeout port).
func TestKafkaQueueE2E_Resume(t *testing.T) {
	env := newKafkaE2EEnv(t)
	node := startKafkaE2ENode(t, env, kafkaE2EOpts{})
	st := node.b.State()
	defaultTopic := env.topics()[0]

	t.Run("timer", func(t *testing.T) {
		before := topicRecordCount(t, env, defaultTopic)
		begin := time.Now()
		id := submitWait(t, env, node, map[string]any{"mode": "timer", "duration": "500ms"})
		ctx := env.ctx(t, 60*time.Second)
		res := waitForCompletion(ctx, t, st, id, "w", "post")
		elapsed := time.Since(begin)
		if res.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %s, want success", res.Status)
		}
		if elapsed < 500*time.Millisecond {
			t.Fatalf("finished after %v, before the 500ms timer could fire", elapsed)
		}
		if res.Output["post"] == nil || nodeStatusOf(t, env, node, id, "on_timeout") != types.NodeStatusSkipped {
			t.Fatalf("post output=%v on_timeout=%q, want post ran and on_timeout skipped",
				res.Output["post"], nodeStatusOf(t, env, node, id, "on_timeout"))
		}
		// start, pre, w, the resume of w, post, end_ok: six tasks, the sixth being
		// the resume that the outbox dispatcher published to Kafka.
		delta := topicRecordCount(t, env, defaultTopic) - before
		t.Logf("timer run took %v, default topic got %d records", elapsed.Round(time.Millisecond), delta)
		if delta != 6 {
			t.Fatalf("default topic received %d records, want 6 (5 node tasks + 1 resume)", delta)
		}
	})

	t.Run("signal", func(t *testing.T) {
		id := submitWait(t, env, node, map[string]any{"mode": "signal", "signal_name": "go"})
		ctx := env.ctx(t, 60*time.Second)
		pollUntil(ctx, t, "w to suspend", func() (bool, error) {
			return nodeStatusOf(t, env, node, id, "w") == types.NodeStatusSuspended, nil
		})
		if snap, _ := st.GetExecution(ctx, id); snap == nil || types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("execution must still be open while w waits for its signal, got %+v", snap)
		}
		if err := node.eng.DeliverSignal(ctx, id, "go", map[string]any{"v": 42}); err != nil {
			t.Fatalf("DeliverSignal: %v", err)
		}
		res := waitForCompletion(ctx, t, st, id, "w", "post")
		if res.Status != types.ExecutionStatusSuccess || res.Output["post"] == nil {
			t.Fatalf("status = %s post=%v, want success with post output", res.Status, res.Output["post"])
		}
		w, _ := res.Output["w"].(map[string]any)
		sd, _ := w["signal_data"].(map[string]any)
		if w["signal_name"] != "go" || e2eInt(sd["v"]) != 42 {
			t.Fatalf("w output = %v, want signal_name=go signal_data.v=42", w)
		}
	})

	t.Run("signal_timeout", func(t *testing.T) {
		id := submitWait(t, env, node, map[string]any{"mode": "signal", "signal_name": "never", "timeout": "600ms"})
		ctx := env.ctx(t, 60*time.Second)
		res := waitForCompletion(ctx, t, st, id, "w", "on_timeout")
		if res.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %s, want success via the timeout port", res.Status)
		}
		w, _ := res.Output["w"].(map[string]any)
		if w["reason"] != "timeout" || res.Output["on_timeout"] == nil {
			t.Fatalf("w=%v on_timeout=%v, want reason=timeout and on_timeout ran", w, res.Output["on_timeout"])
		}
		if s := nodeStatusOf(t, env, node, id, "post"); s != types.NodeStatusSkipped {
			t.Fatalf("post status = %q, want skipped", s)
		}
	})
}
