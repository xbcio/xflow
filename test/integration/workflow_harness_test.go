//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// This file holds the pieces the three workflow suites share: an embedded
// engine for the in-process checks, a runner bound to the server-runner harness
// for the distributed checks, and the two polls that make a suspended node
// observable before a signal is sent.

// localEngineTimeout bounds an embedded run. The embedded engine needs no broker
// round trip, so anything slower than this is a hang, not load.
const localEngineTimeout = 30 * time.Second

// workflowRunTimeout bounds a distributed run. It covers a Redis round trip per
// node plus the runner's poll interval, and is long enough that a failure names
// a stuck node rather than reporting a bare deadline.
const workflowRunTimeout = 60 * time.Second

// runnerPackageCacheEntries mirrors the SDK runner's package-cache size. The
// constant is unexported in sdk/xflow, so this suite restates the value rather
// than taking a dependency on the runner facade for a number that only bounds a
// test-local cache.
const runnerPackageCacheEntries = 64

// newLocalEngine starts an embedded engine for the in-process workflow checks
// and stops it on cleanup.
func newLocalEngine(t *testing.T) *xflow.Engine {
	t.Helper()
	eng, err := xflow.NewLocal(xflow.WithConcurrency(4))
	if err != nil {
		t.Fatalf("xflow.NewLocal: %v", err)
	}
	t.Cleanup(eng.Stop)
	return eng
}

// runLocalWorkflow registers, invokes, and waits for one workflow on an embedded
// engine, returning the terminal result.
func runLocalWorkflow(t *testing.T, wf *xflow.WorkflowBuilder, input map[string]any, opts ...xflow.InvokeOption) (*xflow.Engine, types.ExecutionID, types.Result) {
	t.Helper()
	eng := newLocalEngine(t)
	ctx := context.Background()
	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	execID, err := eng.Invoke(ctx, wfID, xflow.Start(), input, opts...)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	return eng, execID, waitLocalResult(t, ctx, eng, execID)
}

// waitLocalResult blocks until the embedded execution terminates.
func waitLocalResult(t *testing.T, ctx context.Context, eng *xflow.Engine, id types.ExecutionID) types.Result {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, localEngineTimeout)
	defer cancel()
	result, err := eng.Wait(waitCtx, id)
	if err != nil {
		t.Fatalf("Wait(%s): %v", id, err)
	}
	return result
}

// inspectNode returns the audit snapshot for one node of an execution.
func inspectNode(t *testing.T, ctx context.Context, eng *xflow.Engine, id types.ExecutionID, nodeName string) engine.NodeDetail {
	t.Helper()
	detail, err := eng.Inspect(ctx, id, nodeName)
	if err != nil {
		t.Fatalf("Inspect(%s): %v", nodeName, err)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Inspect(%s) returned %d node details, want 1", nodeName, len(detail.Nodes))
	}
	return detail.Nodes[0]
}

// workflowCaps lists the capability of every node type a definition carries,
// body members included, which is what a runner advertises so the control plane
// will assign it that work.
func workflowCaps(def *types.WorkflowDef) []protocol.Capability {
	counts := workflows.NodeTypesIn(def)
	caps := make([]protocol.Capability, 0, len(counts))
	for nodeType := range counts {
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}
	return caps
}

// startWorkflowRunner binds a runner to the harness's control plane and stops it
// on cleanup. pool and resolver are the production resource/credential wiring a
// runner receives; pass nil for a definition that needs neither.
func startWorkflowRunner(t *testing.T, h *serverRunnerHarness, runnerID string, def *types.WorkflowDef, pool types.ResourcePool, resolver func(namespace.Namespace, string) map[string]any) {
	t.Helper()
	reg := execution.NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		reg,
		runnersvc.Config{
			RunnerID:           runnerID,
			Concurrency:        1,
			Capabilities:       workflowCaps(def),
			PollWait:           5 * time.Millisecond,
			ResourcePool:       pool,
			CredentialResolver: resolver,
			// A map node dispatches one synthetic batch lease per item and the
			// lease names a node with no handler, so the runner needs the
			// subgraph runtime to resolve the body's members. Production
			// configures one unconditionally; a runner without it fails every
			// batch task, which is why the tier suite must install it too.
			SubgraphRuntime: runnersvc.NewSubgraphRuntime(
				reg,
				runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: runnerPackageCacheEntries}),
			),
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("runner %s: %v", runnerID, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("runner %s did not stop in time", runnerID)
		}
	})
	waitForE2ERunner(t, h.runners, runnerID)
}

// waitNodeSuspended polls until the named node is parked awaiting a signal, so a
// test can deliver one without racing the runner to the suspension.
//
// It polls the node snapshot, not the execution: an execution can be running for
// minutes while the node of interest flips between running and suspended, and
// only the node's own status says it is safe to signal.
func waitNodeSuspended(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, nodeName string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(workflowRunTimeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last types.NodeStatus
	for {
		if snap, err := h.state.GetNode(ctx, id, nodeName); err == nil && snap != nil {
			last = snap.Status
			if snap.Status == types.NodeStatusSuspended {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for node %q to suspend (last status %s)", nodeName, last)
		}
		<-ticker.C
	}
}

// postSignal delivers a signal over the production HTTP route, which is the same
// path an operator or an approval UI uses.
func postSignal(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, name string, data map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "data": data})
	if err != nil {
		t.Fatalf("encode signal: %v", err)
	}
	url := h.httpSrv.URL + "/v1/executions/" + string(id) + "/signals"
	resp, err := h.httpSrv.Client().Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post signal: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	var envelope struct {
		Data struct {
			Accepted bool `json:"accepted"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode signal response: %v", err)
	}
	if !envelope.Data.Accepted {
		t.Fatal("signal was not accepted")
	}
}

// waitDistributedCompletion polls the control plane's state store until the
// execution reaches a terminal status, then returns the result.
func waitDistributedCompletion(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, outputNodes ...string) types.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), workflowRunTimeout)
	defer cancel()
	return waitForCompletion(ctx, t, h.state, id, outputNodes...)
}

// waitDistributedCompletionDumping is waitDistributedCompletion with the node
// snapshots logged before it gives up, so a stuck run names the node it stuck on
// instead of reporting only a deadline.
func waitDistributedCompletionDumping(t *testing.T, h *serverRunnerHarness, id types.ExecutionID, names []string, outputNodes ...string) types.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), workflowRunTimeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if snap, err := h.state.GetExecution(ctx, id); err == nil && types.IsTerminalExecutionStatus(snap.Status) {
			out := map[string]any{}
			for _, n := range outputNodes {
				if v, e := h.state.GetOutput(ctx, id, n); e == nil && v != nil {
					out[n] = v
				}
			}
			return types.Result{ExecutionID: id, Status: snap.Status, Output: out}
		}
		select {
		case <-ctx.Done():
			dumpNodes(t, h, id, names...)
			t.Fatalf("timeout waiting for execution %s", id)
		case <-ticker.C:
		}
	}
}
