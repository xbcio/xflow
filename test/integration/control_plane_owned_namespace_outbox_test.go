//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	_ "github.com/xbcio/xflow/node" // registers xflow.wait / xflow.end in nodereg
	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// TestControlPlaneOwnedNamespaceTimerResume is the control-plane counterpart of
// TestKafkaQueueE2E_ProductOutboxDispatcher: the same defect, reached through
// the other entry point.
//
// Setup: a distributed backend that owns a single dedicated namespace, a real
// ControlPlane over it (Config.OwnedNamespaces left empty, so it adopts the
// backend's), started with ControlPlane.Start, and one in-process runner that
// serves that namespace. A two-node workflow (xflow.wait with a 500ms timer,
// then xflow.end) is submitted under the namespace. The runner executes the
// nodes; the timer resume does not come from the runner but from the server's
// durable outbox, which is the path under test.
//
// Expected: the resume entry is delivered within a few outbox ticks and the
// execution succeeds, as it does in the default namespace.
//
// What it guards: ControlPlane.Start binds the dispatcher through
// backend.TaskHandlerBinder.BindTaskHandler, not Backend.StartBinding, but both
// land in the same bindHandler, which starts the engine.OutboxDispatcher. That
// dispatcher runs on a context with no namespace, so discovery that returns bare
// execution IDs finds the execution and then claims its outbox from the
// "default" namespace, where it has none: the timer resume stays due in
// xflow:ns:<ns>:exec:{<id>}:outbox:ready and the execution stays "running". The
// SDK-path regression test cannot see a control plane losing the namespace
// differently, so this one drives the control plane's own binding. Discovery now
// reports each execution with its namespace (engine.NamespacedOutboxLister) and
// the dispatcher flushes it under that namespace.
//
// It runs against Redis only: no MySQL (the control plane takes no SQL store
// here) and no Kafka (the queue is the backend's default Asynq).
func TestControlPlaneOwnedNamespaceTimerResume(t *testing.T) {
	addr := requireRedis(t)
	ns := namespace.Namespace("cp-owned-" + uniqueSuffix(t))
	// Registered first so it runs last (t.Cleanup is LIFO), after the control
	// plane and the runner have stopped writing.
	t.Cleanup(func() { cleanupControlPlaneNamespaceKeys(t, addr, ns) })

	b, err := distributed.New(addr, nil,
		distributed.WithConcurrency(2),
		distributed.WithConsumer(true),
		distributed.WithOwnedNamespaces([]namespace.Namespace{ns}),
	)
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: b})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatalf("ControlPlane.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cp.Shutdown(ctx); err != nil {
			t.Errorf("ControlPlane.Shutdown: %v", err)
		}
	})

	reg := execution.NewRegistry()
	for _, nodeType := range []string{"xflow.wait", "xflow.end"} {
		h, ok := nodereg.Lookup(nodeType)
		if !ok {
			t.Fatalf("builtin %s is not registered", nodeType)
		}
		reg.RegisterGlobal(nodeType, h)
	}
	const runnerID = "cp-owned-ns-runner"
	runnerCtx, stopRunner := context.WithCancel(context.Background())
	runner := runnersvc.New(cp.RunnerServer().InProcessRunnerClient(""), reg, runnersvc.Config{
		RunnerID:    runnerID,
		Concurrency: 1,
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.wait"},
			{NodeType: "xflow.end"},
		},
		PollWait:   5 * time.Millisecond,
		Namespaces: []namespace.Namespace{ns},
	})
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(runnerCtx) }()
	t.Cleanup(func() {
		stopRunner()
		select {
		case <-runnerDone:
		case <-time.After(5 * time.Second):
			t.Error("runner did not stop in time")
		}
	})
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "control-plane-owned-ns-timer",
		Nodes: []types.NodeDef{
			{Name: "w", Type: "xflow.wait", Parameters: map[string]any{"mode": "timer", "duration": "500ms"}},
			{Name: "end", Type: "xflow.end"},
		},
		Connections: types.Connections{
			"w": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Redis keys are namespace-scoped and the control plane's engine carries no
	// namespace of its own, so every submit and read must use a context built
	// here or it addresses the default namespace.
	nsCtx := func(d time.Duration) (context.Context, context.CancelFunc) {
		return context.WithTimeout(namespace.WithNamespace(context.Background(), ns), d)
	}
	submitCtx, cancelSubmit := nsCtx(10 * time.Second)
	defer cancelSubmit()
	id, err := cp.Engine().Submit(submitCtx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	ctx, cancel := nsCtx(12 * time.Second)
	defer cancel()
	var (
		last    types.ExecutionStatus
		lastErr error
	)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for !types.IsTerminalExecutionStatus(last) {
		snap, err := b.State().GetExecution(ctx, id)
		if lastErr = err; err == nil && snap != nil {
			last = snap.Status
			continue
		}
		select {
		case <-ctx.Done():
			t.Fatalf("execution %s did not reach a terminal status in 12s (last status %q, last read error %v): "+
				"expected the 500ms timer resume to be delivered by the control plane's outbox dispatcher in namespace %q",
				id, last, lastErr, ns)
		case <-tick.C:
		}
	}
	if last != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", last)
	}
}

// cleanupControlPlaneNamespaceKeys deletes the Redis keys this test's random
// namespace created and its entry in the append-only namespace set. It never
// flushes: the namespace carries a per-run random suffix, so the patterns cannot
// match another test's keys.
func cleanupControlPlaneNamespaceKeys(t *testing.T, addr string, ns namespace.Namespace) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*"+string(ns)+"*", 500).Result()
		if err != nil {
			t.Errorf("scan redis keys for %s: %v", ns, err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("del redis keys for %s: %v", ns, err)
				return
			}
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if err := rdb.SRem(ctx, "xflow:namespaces", string(ns)).Err(); err != nil {
		t.Errorf("srem xflow:namespaces %s: %v", ns, err)
	}
	// Asynq registers every queue it has seen in the shared asynq:queues set, and
	// a dedicated deployment's queues are named after its namespace. Only the
	// members that carry this namespace are removed.
	queues, err := rdb.SMembers(ctx, "asynq:queues").Result()
	if err != nil {
		t.Errorf("smembers asynq:queues: %v", err)
		return
	}
	for _, q := range queues {
		if strings.Contains(q, string(ns)) {
			if err := rdb.SRem(ctx, "asynq:queues", q).Err(); err != nil {
				t.Errorf("srem asynq:queues %s: %v", q, err)
			}
		}
	}
}
