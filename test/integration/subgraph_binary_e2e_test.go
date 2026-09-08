//go:build integration

// TestSubgraphBinaryE2E is the map-body release-gate evidence: it builds the
// production bin/server and bin/runner, starts them as real OS processes
// against real Redis + MySQL, and proves a map node's body sub-graph actually
// executes — once per item, on the runner.
//
// It must be real processes. The control plane sets WithRemoteBatchExecution,
// so its own engine has no BatchBodyExecutor at all: a batch it cannot route
// stays undispatched forever. That asymmetry is what makes "the execution
// reached Success" real evidence of remote execution here, and it is exactly
// what an in-process harness that wires both halves itself would erase.
package integration

import (
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// subgraphBody wraps body member nodes in the shape the compiler requires:
// a NodeDef of the synthetic type xflow.subgraph whose parameters carry the
// members. Anything else is rejected at compile time.
func subgraphBody(nodes ...map[string]any) map[string]any {
	members := make([]any, 0, len(nodes))
	for _, n := range nodes {
		members = append(members, n)
	}
	return map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": members,
		},
	}
}

// mapBodyWorkflow is one xflow.map node whose body is a single xflow.function
// evaluating code against the injected $item/$index roots.
//
// on_error is main_output rather than the default stop for one reason: a failing
// map node under "stop" commits a nil output, so the results array — the only
// per-item evidence a real process exposes — would be unobservable in exactly
// the case the continue_on_error tests need to inspect.
func mapBodyWorkflow(name, code string, batchSize int, continueOnError bool) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{{
			Name:    "m",
			Type:    "xflow.map",
			OnError: "main_output",
			Parameters: map[string]any{
				"items":             "$input.rows",
				"batch_size":        batchSize,
				"continue_on_error": continueOnError,
				"body": subgraphBody(map[string]any{
					"name":       "echo",
					"type":       "xflow.function",
					"parameters": map[string]any{"code": code},
				}),
			},
		}},
	}
}

// bodyEchoCode returns each item's id alongside its GLOBAL index. $index is the
// position in the map node's whole items array, not the position within the
// batch, so a workflow's results must not change when batch_size does.
const bodyEchoCode = `{"id": $item.id, "idx": $index, "total": len($items)}`

// bodyFailAtCode fails the item at global index 1 and echoes every other item.
// The failure is an expression fault (fetch from nil), which is a permanent
// per-item error — not a transient the engine would retry.
const bodyFailAtCode = `$index == 1 ? $item.nope.deep : {"id": $item.id, "idx": $index}`

// subgraphRows builds n items as [{id:0},{id:1},...].
func subgraphRows(n int) []any {
	rows := make([]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"id": i})
	}
	return rows
}

// subgraphSubmit submits a workflow with params (the map node is the root, so
// params become its input.Data and "$input.rows" resolves against them).
func subgraphSubmit(t *testing.T, baseURL string, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := map[string]any{"workflow": wf, "params": params}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/workflows/execute", r8Token, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit map workflow: status=%d, want 200 (body=%s)", resp.StatusCode, string(raw))
	}
	out := decodeSubmitEnvelope(t, raw)
	return out.ExecutionID
}

// startSubgraphRunner starts the production runner binary with the given
// capabilities. Batch routing advertises the map node's own type
// (engine.TaskRouting returns meta.Type), so "xflow.map" is what lets a runner
// claim a batch at all — which is why the caps are a parameter here.
func startSubgraphRunner(t *testing.T, runnerBin, httpURL, id, caps string) *r8Process {
	t.Helper()
	out := &safeBuffer{}
	cmd := exec.Command(runnerBin, "run",
		"--server", httpURL,
		"--transport", "http",
		"--id", id,
		"--token", r8RunnerToken,
		"--cap", caps,
		"--poll-wait", "50ms",
		"--concurrency", "2",
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start runner %s: %v", id, err)
	}
	p := &r8Process{cmd: cmd, out: out}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("runner %s logs:\n%s", id, out.String())
		}
	})
	return p
}

// mapNodeDetail finds the map node's audit snapshot in an execution detail.
func mapNodeDetail(t *testing.T, detail engine.ExecutionDetail, name string) engine.NodeDetail {
	t.Helper()
	for _, n := range detail.Nodes {
		if n.Name == name {
			return n
		}
	}
	t.Fatalf("execution detail carries no node %q (nodes=%+v)", name, detail.Nodes)
	return engine.NodeDetail{}
}

// mapResults reads the flattened per-item results out of the map node's output.
// The map node's output is the only place a real-process test can observe what
// the body produced: the body ran in another process, so there is no in-test
// recorder to consult.
func mapResults(t *testing.T, detail engine.ExecutionDetail) []any {
	t.Helper()
	node := mapNodeDetail(t, detail, "m")
	results, ok := node.Output["results"].([]any)
	if !ok {
		t.Fatalf("map node output has no results array: %+v", node.Output)
	}
	return results
}

// itemRow asserts results[i] is a map and returns it.
func itemRow(t *testing.T, results []any, i int) map[string]any {
	t.Helper()
	if i >= len(results) {
		t.Fatalf("results has %d entries, wanted index %d: %+v", len(results), i, results)
	}
	row, ok := results[i].(map[string]any)
	if !ok {
		t.Fatalf("results[%d] = %#v, want a map", i, results[i])
	}
	return row
}

func TestSubgraphBinaryE2E(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)

	t.Run("BodyRunsOncePerItemOnTheRunner", func(t *testing.T) {
		subgraphBodyRunsPerItem(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("RunnerWithoutMapCapabilityNeverRunsTheBody", func(t *testing.T) {
		subgraphNoMapCapability(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("ContinueOnErrorTrueKeepsGoing", func(t *testing.T) {
		subgraphContinueOnErrorTrue(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("ContinueOnErrorFalseStopsRemainingItems", func(t *testing.T) {
		subgraphContinueOnErrorFalse(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("KillRunnerMidExpansionThenRestart", func(t *testing.T) {
		subgraphKillMidExpansion(t, serverBin, runnerBin, addr, dsn)
	})
}

// subgraphBodyRunsPerItem is probe (1) and probe (4) of §11.1 in one execution:
// the body runs once per item, in another process, and $index is the item's
// GLOBAL position rather than its position within its batch.
//
// The global-index assertion is what makes batch_size a pure durability knob:
// 5 items at batch_size=2 arrive as batches [0,1], [2,3], [4]. A runtime that
// used the in-batch position would report idx 0,1,0,1,0 — five results either
// way, and every "the body ran" assertion would still pass.
func subgraphBodyRunsPerItem(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-happy", "xflow.map,xflow.function")
	t.Cleanup(func() { runner.stop(t) })

	wf := mapBodyWorkflow("sg-per-item", bodyEchoCode, 2, false)
	id := subgraphSubmit(t, srv.httpURL, wf, map[string]any{"rows": subgraphRows(5)})
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 60*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("map execution status=%s, want %s (error=%q, runner out=%s)",
			detail.Status, types.ExecutionStatusSuccess, detail.Error, runner.out.String())
	}

	results := mapResults(t, detail)
	if len(results) != 5 {
		t.Fatalf("map produced %d results, want 5 — one per item, independent of batch_size: %+v",
			len(results), results)
	}
	for i := range results {
		row := itemRow(t, results, i)
		if got := toInt(t, row["id"]); got != i {
			t.Errorf("results[%d].id = %d, want %d — the body saw the wrong $item", i, got, i)
		}
		if got := toInt(t, row["idx"]); got != i {
			t.Errorf("results[%d].idx = %d, want %d — $index must be the item's global "+
				"position, not its position within its batch (batch_size=2 here)", i, got, i)
		}
		if got := toInt(t, row["total"]); got != 5 {
			t.Errorf("results[%d].total = %d, want 5 — $items must be the whole array, "+
				"not just this batch's slice", i, got)
		}
	}
	t.Logf("subgraph e2e: real bin/server+bin/runner ran the body 5×, global indices intact")
}

// subgraphNoMapCapability is probe (1)'s reverse: a runner that does not
// advertise xflow.map must never claim a batch, and — critically — the control
// plane must not quietly run the body itself.
//
// The control plane sets WithRemoteBatchExecution, so it has no
// BatchBodyExecutor: an unroutable batch stays queued and the execution never
// terminalizes. That is the assertion. Per negative-assertion-needs-time, it is
// made by waiting rather than by one immediate read.
func subgraphNoMapCapability(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	// The runner can run the BODY member (xflow.function) but not the map node,
	// so a body executing anywhere would have to mean the control plane did it.
	runner := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-nomap", "xflow.function")
	t.Cleanup(func() { runner.stop(t) })

	wf := mapBodyWorkflow("sg-nomap", bodyEchoCode, 2, false)
	id := subgraphSubmit(t, srv.httpURL, wf, map[string]any{"rows": subgraphRows(4)})
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		_, detail := g1InspectAuth(t, srv.httpURL, r8Token, id)
		if types.IsTerminalExecutionStatus(detail.Status) {
			t.Fatalf("execution %s reached %s with no runner advertising xflow.map — "+
				"the batch was executed somewhere it should not have been "+
				"(map node=%+v)", id, detail.Status, mapNodeDetail(t, detail, "m"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("subgraph e2e: batch stayed undispatched for 8s with no xflow.map capability")
}

// subgraphContinueOnErrorTrue is probe (2)'s true arm plus probe (7): one item
// fails, every other item still runs, the map node still succeeds, and the
// failed item occupies its slot as an {_error,_index} placeholder rather than
// vanishing.
//
// batch_size=1 puts the failing item in a batch of its own, so "the others ran"
// cannot be explained by them sharing a batch with it.
func subgraphContinueOnErrorTrue(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-coe-true", "xflow.map,xflow.function")
	t.Cleanup(func() { runner.stop(t) })

	wf := mapBodyWorkflow("sg-coe-true", bodyFailAtCode, 1, true)
	id := subgraphSubmit(t, srv.httpURL, wf, map[string]any{"rows": subgraphRows(4)})
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 60*time.Second)
	results := mapResults(t, detail)
	if len(results) != 4 {
		t.Fatalf("continue_on_error=true produced %d results, want 4 — a failed item must "+
			"hold its slot, not be dropped: %+v", len(results), results)
	}
	// The failing item is a placeholder; every other item carries real data.
	failed := itemRow(t, results, 1)
	if _, ok := failed["_error"]; !ok {
		t.Errorf("results[1] = %+v, want an _error placeholder for the failed item", failed)
	}
	for _, i := range []int{0, 2, 3} {
		row := itemRow(t, results, i)
		if _, isErr := row["_error"]; isErr {
			t.Errorf("results[%d] = %+v, want real data — only item 1 was made to fail", i, row)
		}
		if got := toInt(t, row["idx"]); got != i {
			t.Errorf("results[%d].idx = %d, want %d — items after the failure must keep "+
				"their own global index", i, got, i)
		}
	}
	t.Logf("subgraph e2e: continue_on_error=true, 1 of 4 items failed, other 3 ran, status=%s", detail.Status)
}

// subgraphContinueOnErrorFalse is probe (2)'s false arm, the one the spec calls
// out as easiest to fake: asserting only "the map node failed" passes both when
// the FIRST item failed and when a later item failed after earlier ones already
// ran. So this asserts both halves.
//
// batch_size=4 puts all four items in ONE batch, which is the only arrangement
// where "stop the rest" is even observable — items in different batches are
// different tasks and were never sequenced against each other.
func subgraphContinueOnErrorFalse(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-coe-false", "xflow.map,xflow.function")
	t.Cleanup(func() { runner.stop(t) })

	wf := mapBodyWorkflow("sg-coe-false", bodyFailAtCode, 4, false)
	id := subgraphSubmit(t, srv.httpURL, wf, map[string]any{"rows": subgraphRows(4)})
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 60*time.Second)
	results := mapResults(t, detail)

	// Positive evidence: item 0 ran before the failure. Without this the test
	// would pass on an implementation that failed the whole batch up front.
	if len(results) < 1 {
		t.Fatalf("continue_on_error=false produced no results — item 0 runs BEFORE item 1 "+
			"fails, so its result must be present: %+v", results)
	}
	first := itemRow(t, results, 0)
	if _, isErr := first["_error"]; isErr {
		t.Errorf("results[0] = %+v, want real data — item 0 ran before item 1 failed", first)
	}

	// Negative evidence: items 2 and 3 must NOT have run. They are in the same
	// batch, after the failure, so the batch must have stopped at item 1.
	if len(results) != 2 {
		t.Fatalf("continue_on_error=false produced %d results, want 2 (item 0 succeeded, "+
			"item 1 failed, items 2-3 never ran): %+v", len(results), results)
	}
	failed := itemRow(t, results, 1)
	if _, ok := failed["_error"]; !ok {
		t.Errorf("results[1] = %+v, want the _error that stopped the batch", failed)
	}

	// The map node must carry the failure, not report a clean success.
	node := mapNodeDetail(t, detail, "m")
	if node.Error == "" {
		t.Errorf("map node reported no error after a batch failed under "+
			"continue_on_error=false: %+v", node)
	}
	t.Logf("subgraph e2e: continue_on_error=false stopped at item 1; item 0 ran, items 2-3 did not")
}

// subgraphKillMidExpansion is probe (3): SIGKILL the runner mid-expansion and
// restart it. The parent map node's lease is the expansion's fence, so batches
// from the dead generation are rejected on commit rather than corrupting the new
// one — and the execution still converges to exactly one result per item.
//
// The count is the assertion that matters. A fence that failed to reject stale
// batches would show up as duplicate or missing entries in the results array,
// not as a wrong status.
func subgraphKillMidExpansion(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-kill-1", "xflow.map,xflow.function")

	wf := mapBodyWorkflow("sg-kill", bodyEchoCode, 1, false)
	id := subgraphSubmit(t, srv.httpURL, wf, map[string]any{"rows": subgraphRows(6)})
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	// Kill as soon as the map node is expanding (Waiting is the state it holds
	// for the whole expansion — it terminalizes only at the all-done barrier).
	g1WaitForNodeStatus(t, srv.httpURL, r8Token, id, "m",
		[]types.NodeStatus{types.NodeStatusWaiting, types.NodeStatusRunning}, 30*time.Second)
	runner.kill(t)
	t.Logf("subgraph e2e: runner SIGKILLed while map node m was expanding")

	runner2 := startSubgraphRunner(t, runnerBin, srv.httpURL, "sg-runner-kill-2", "xflow.map,xflow.function")
	t.Cleanup(func() { runner2.stop(t) })

	deadline := time.Now().Add(240 * time.Second)
	started := time.Now()
	for time.Now().Before(deadline) {
		_, detail := g1InspectAuth(t, srv.httpURL, r8Token, id)
		if !types.IsTerminalExecutionStatus(detail.Status) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if detail.Status != types.ExecutionStatusSuccess {
			t.Fatalf("after runner kill+restart, execution %s = %s, want Success (error=%q, runner out=%s)",
				id, detail.Status, detail.Error, runner2.out.String())
		}
		results := mapResults(t, detail)
		if len(results) != 6 {
			t.Fatalf("after runner kill+restart the map node reported %d results, want exactly 6 — "+
				"a stale batch from the dead generation either duplicated or lost a slot: %+v",
				len(results), results)
		}
		seen := map[int]bool{}
		for i := range results {
			row := itemRow(t, results, i)
			idx := toInt(t, row["idx"])
			if seen[idx] {
				t.Errorf("global index %d appears twice — a stale batch's result was accepted", idx)
			}
			seen[idx] = true
		}
		t.Logf("subgraph e2e: runner killed mid-expansion, restarted, execution %s -> Success "+
			"with 6 distinct items after %s", id, time.Since(started).Round(time.Second))
		return
	}
	// Recovery here depends on the production LeaseSweeper reclaiming the dead
	// runner's leases (DefaultSweepPeriod=10s, DefaultLeaseTTL=60s), so the
	// window is generous. It is NOT a degradation branch: a batch that never
	// converges means a stale generation blocked the new one, which is the
	// defect this probe exists to catch.
	_, last := g1InspectAuth(t, srv.httpURL, r8Token, id)
	t.Fatalf("execution %s did not converge within 240s after the runner was SIGKILLed "+
		"mid-expansion (status=%s, map node=%+v); a batch from the dead generation is "+
		"blocking the new one", id, last.Status, mapNodeDetail(t, last, "m"))
}

// toInt coerces a JSON-decoded number to int. Every number that crosses the HTTP
// boundary arrives as float64, so a direct int assertion would fail on correct
// data.
func toInt(t *testing.T, v any) int {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		t.Fatalf("value %#v is not a number", v)
		return 0
	}
}
