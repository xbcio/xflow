package workflows_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// TestFullWorkflowCoversEveryNodeType asserts FullWorkflow ALONE carries every
// registered action type the compiler accepts, every registered trigger kind,
// and both declaration-only supply kinds. The integration coverage suite
// measures the union of all tiers; this pins the single-definition claim, and
// runs without a build tag so it gates ordinary `go test`.
//
// The expected set is read from the registry, not written down, so a newly
// registered node type fails this test until FullWorkflow uses it.
func TestFullWorkflowCoversEveryNodeType(t *testing.T) {
	def, err := workflows.FullWorkflow(workflows.DefaultFullTriggerConfig()).Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if _, err := graph.Compile(def); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	seen := workflows.NodeTypesIn(def)

	want := append(append(append([]string{}, registry.Types()...), registry.TriggerTypes()...),
		workflows.DeclarationOnlyNodeTypes...)

	var missing []string
	for _, nodeType := range want {
		if seen[nodeType] == 0 {
			missing = append(missing, nodeType)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("FullWorkflow does not use %d registered type(s):\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// The reverse: every type named must be resolvable by a real runner. The
	// body-only subgraph wrapper is what the map body compiles into.
	known := setOf(want)
	known[workflows.SubgraphNodeType] = true
	var unknown []string
	for nodeType := range seen {
		if !known[nodeType] {
			unknown = append(unknown, nodeType)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		t.Errorf("FullWorkflow uses unregistered or deprecated types: %v", unknown)
	}

	total := 0
	for _, n := range seen {
		total += n
	}
	t.Logf("FullWorkflow: %d nodes, %d distinct types", total, len(seen))
}

// TestFullWorkflowEntries asserts the compiled graph exposes all six entries:
// start and one per trigger kind.
func TestFullWorkflowEntries(t *testing.T) {
	def, err := workflows.FullWorkflow(workflows.DefaultFullTriggerConfig()).Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	for _, name := range []string{
		workflows.FullEntryStart, workflows.FullEntryTimer, workflows.FullEntryCron,
		workflows.FullEntryWebhook, workflows.FullEntryRedis, workflows.FullEntryKafka,
	} {
		if _, ok := g.EntryIndex(name); !ok {
			t.Errorf("entry %q is not an entry of the compiled graph", name)
		}
	}
}

// completionRecorder records the terminal status of every execution.
type completionRecorder struct {
	engine.BaseHooks
	mu   sync.Mutex
	done map[types.ExecutionID]types.ExecutionStatus
	ids  []types.ExecutionID
}

func (r *completionRecorder) OnExecutionComplete(_ context.Context, id types.ExecutionID, s types.ExecutionStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done == nil {
		r.done = map[types.ExecutionID]types.ExecutionStatus{}
	}
	r.done[id] = s
	r.ids = append(r.ids, id)
}

func (r *completionRecorder) completed() []types.ExecutionID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]types.ExecutionID(nil), r.ids...)
}

// TestFullWorkflowRunsLocally hosts FullWorkflow on an embedded engine, with
// every trigger armed, and drives both kinds of entry:
//
//   - start: no HTTP, database, or gRPC endpoint is supplied, so enrich fails
//     and its unset error strategy fails the run. What this proves without IO
//     is that the run TERMINATES — the five unselected trigger entries resolve
//     as skipped instead of holding it open — and that the nodes ahead of
//     enrich succeed. The full main lane runs against real dependencies in
//     test/integration (TestWorkflowFullDistributed).
//   - webhook: a real HTTP request on the engine's webhook handler fires the
//     trigger lane to success, and the whole main lane plus the four sibling
//     triggers resolve as skipped.
//
// The redis and kafka entries point at local defaults that need not exist; an
// unreachable broker is the trigger's own retry loop, not a registration
// failure, so the other entries are unaffected.
func TestFullWorkflowRunsLocally(t *testing.T) {
	rec := &completionRecorder{}
	eng, err := xflow.NewLocal(xflow.WithHooks(rec))
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	t.Cleanup(eng.Stop)
	ctx := context.Background()
	id, err := eng.AddWorkflow(ctx, workflows.FullWorkflow(workflows.DefaultFullTriggerConfig()))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	t.Run("start", func(t *testing.T) {
		execID, err := eng.Invoke(ctx, id, xflow.Start(), workflows.AboveThresholdInput())
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		res, err := eng.Wait(waitCtx, execID)
		if err != nil {
			t.Fatalf("Wait: %v (did an unselected entry hold the run open?)", err)
		}
		if res.Status != types.ExecutionStatusFailed {
			t.Fatalf("status = %s, want failed at enrich (no endpoint supplied)", res.Status)
		}
		want := map[string]types.NodeStatus{
			"start": types.NodeStatusSuccess, "seed": types.NodeStatusSuccess,
			"tag": types.NodeStatusSuccess, "enrich": types.NodeStatusFailed,
		}
		for _, name := range workflows.FullTriggerNodeNames() {
			want[name] = types.NodeStatusSkipped
		}
		assertStatuses(t, eng, execID, want)
	})

	t.Run("webhook", func(t *testing.T) {
		handler := eng.WebhookHandler()
		if handler == nil {
			t.Fatal("WebhookHandler() is nil; FullWorkflow registers a webhook route")
		}
		srv := httptest.NewServer(handler)
		defer srv.Close()

		before := len(rec.completed())
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+workflows.FullWebhookPath,
			strings.NewReader(`{"source":"full-local"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("webhook status = %d, want 202", resp.StatusCode)
		}

		var execID types.ExecutionID
		deadline := time.Now().Add(10 * time.Second)
		for execID == "" {
			if ids := rec.completed(); len(ids) > before {
				execID = ids[before]
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("webhook firing produced no completed execution")
			}
			time.Sleep(10 * time.Millisecond)
		}
		res, err := eng.Wait(ctx, execID)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if res.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %s (%s), want success", res.Status, res.Error)
		}
		want := map[string]types.NodeStatus{
			workflows.FullEntryWebhook: types.NodeStatusSuccess,
			"trigger_join":             types.NodeStatusSuccess,
			"trigger_record":           types.NodeStatusSuccess,
			"trigger_done":             types.NodeStatusSuccess,
		}
		for _, name := range []string{workflows.FullEntryTimer, workflows.FullEntryCron, workflows.FullEntryRedis, workflows.FullEntryKafka} {
			want[name] = types.NodeStatusSkipped
		}
		for _, name := range workflows.FullMainNodeNames() {
			if name == "dq" { // map body member: never a node of the outer execution
				continue
			}
			want[name] = types.NodeStatusSkipped
		}
		assertStatuses(t, eng, execID, want)

		// The recording node must see the firing entry's event through the
		// wait-any merge, not merely run.
		d, err := eng.Inspect(ctx, execID, "trigger_record")
		if err != nil || len(d.Nodes) != 1 {
			t.Fatalf("Inspect(trigger_record): %v", err)
		}
		ev, ok := d.Nodes[0].Output["event"].(*types.TriggerEvent)
		if !ok || ev.Kind != "webhook" {
			t.Fatalf("trigger_record event = %#v, want the webhook TriggerEvent", d.Nodes[0].Output["event"])
		}
	})
}

func assertStatuses(t *testing.T, eng *xflow.Engine, id types.ExecutionID, want map[string]types.NodeStatus) {
	t.Helper()
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		d, err := eng.Inspect(context.Background(), id, name)
		if err != nil || len(d.Nodes) != 1 {
			t.Errorf("Inspect(%s): %v", name, err)
			continue
		}
		if got := d.Nodes[0].Status; got != want[name] {
			t.Errorf("node %s = %s, want %s", name, got, want[name])
		}
	}
}

func setOf(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, s := range items {
		out[s] = true
	}
	return out
}
