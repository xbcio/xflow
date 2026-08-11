//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// wasmBindTriggerType is a fake trigger node type used only by this test. It is
// registered in the global node registry so both the control plane's capability
// derivation and the runner's TriggerHandlerLookup resolve it, exactly as they
// would for a real Kafka trigger — without this test needing a broker. What is
// under test is the binding's derivation, transport, and registration; message
// delivery is covered elsewhere.
const wasmBindTriggerType = "test.wasmbind.trigger"

// wasmBindSupplyNode is this test's supply NODE name. supply.Default is
// process-wide and UnregisterConsumer never drops the applied Snapshot, so a
// name shared with another test would leave content resident across tests.
const wasmBindSupplyNode = "rules_wasmbind"

// fakeWasmBindTrigger activates into a no-op subscription. It records that
// Activate ran so the test can tell "the binding was registered as part of a
// real activation" from "the activation never happened at all".
type fakeWasmBindTrigger struct {
	mu        sync.Mutex
	activated bool
}

func (h *fakeWasmBindTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: wasmBindTriggerType, Kind: types.NodeKindTrigger}
}

func (h *fakeWasmBindTrigger) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

func (h *fakeWasmBindTrigger) Activate(_ context.Context, _ *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	h.mu.Lock()
	h.activated = true
	h.mu.Unlock()
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

func (h *fakeWasmBindTrigger) wasActivated() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activated
}

// bindCountingObserver records the consumer count the supply registry reports
// per supply name. It observes through Registry.SetObserver — the registry's own
// production seam, also used by the metrics adapter — rather than a hook added
// for this test.
type bindCountingObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func (o *bindCountingObserver) OnConsumerCount(_ context.Context, name string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts = map[string]int{}
	}
	o.counts[name] = n
}

func (o *bindCountingObserver) get(name string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counts[name]
}

// buildReactorSeamWasm compiles the reactorseam guest and returns its bytes. The
// guest reports back which rule names configured it plus the pool revision the
// host stamps, which is what makes "the supply content actually reached the
// module" observable from outside.
func buildReactorSeamWasm(t *testing.T) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "reactorseam.wasm")
	src := filepath.Join("..", "..", "node", "internal", "code", "script", "wasm", "testdata", "reactorseam", "main.go")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, src)
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build reactorseam guest: %v\n%s", err, b)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read reactorseam guest: %v", err)
	}
	return raw
}

// newWasmBindingRunner mirrors newSupplyGatingRunner but adds the piece this
// test is about: WithArtifactCodeResolver. Without it a directive carrying
// SupplyConsumers is rejected (the handler cannot compile the module, so it
// refuses to register a consumer against it), and the activation would fail
// closed rather than register anything.
//
// The gate publishes into supply.Default — NOT an isolated registry — because
// the consumer registration path hardcodes supply.Default (the forwarding layer
// in node/internal/code/script/warmup.go). A gate writing anywhere else would
// leave the registered consumer watching a registry that never receives content,
// and this test would pass for the wrong reason. Isolation comes from the unique
// supply node name instead.
func newWasmBindingRunner(baseURL, token string, resolve func(context.Context, string) ([]byte, error)) *runnersvc.ActivationTracker {
	fetcher := &runnersvc.HTTPSupplyFetcher{BaseURL: baseURL, Token: token}
	gate := runnersvc.NewSupplyGate(fetcher, supply.Default, slog.Default())
	handler := runnersvc.NewTriggerActivationHandler(baseURL, token, registryTriggerLookupGating{},
		runnersvc.WithSupplyGate(gate),
		runnersvc.WithArtifactCodeResolver(resolve))
	return runnersvc.NewActivationTracker(handler, slog.Default())
}

// TestSupplyConsumerBindingReachesRunner is the end-to-end proof for the wasm
// supply-consumer wiring: the pairing "this module consumes that supply" is
// derived on the SERVER from the graph's dependency edges, travels on the
// heartbeat's activate directive, and is registered by the receiving runner.
//
// It has to be end to end because the pairing exists nowhere else. The directive
// carries a flat supply name list, and a projected group package flattens every
// member's supply refs into one deduplicated name set — by the time content
// reaches a runner, "who consumes it" is gone. Before this wiring the runner
// fetched the content and handed it to nobody: the module evaluated every record
// against no rules, passing all traffic through uncleansed and untagged with no
// error and no diagnostic.
//
// The assertions, in order:
//
//	(a) the activation is hosted — so what follows is the real activation path,
//	    not a registration that happened for some other reason;
//	(b) exactly one consumer is registered for the supply, observed through the
//	    registry's own consumer-count seam;
//	(c) the module RUNS and reports the supply's revision as the content that
//	    configured it. This is the assertion that cannot be faked: revision 0
//	    would mean the legacy globals path (no supply content at all), and a
//	    refusal would mean the consumer was registered against a module that was
//	    never compiled — the deadlock this wiring had to avoid introducing.
func TestSupplyConsumerBindingReachesRunner(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	guest := buildReactorSeamWasm(t)
	// store.ContentHash is what the control plane records as artifact_digest, and
	// the wasm host derives its module key from the same sha256. The two must
	// agree or the registration targets a module that does not exist.
	digest := store.ContentHash(guest)

	trigger := &fakeWasmBindTrigger{}
	registry.Register(trigger)

	obs := &bindCountingObserver{}
	supply.Default.SetObserver(obs)
	t.Cleanup(func() { supply.Default.SetObserver(nil) })
	// supply.Default outlives this test; a registration left behind would keep
	// rebuilding a dead module's pool on every later Apply in this binary.
	t.Cleanup(func() { node.UnregisterWasmSupplyConsumerByDigest(digest, wasmBindSupplyNode) })

	httpSrv, cp, token := newSupplyGatingControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_wasmbind"
		taggerNode  = "tag_wasmbind"
		supplyRes   = "wasmbind-rules"
		zoneLabel   = "wasmbind"
		runnerID    = "runner-wasmbind"
	)

	def := &types.WorkflowDef{
		// Unique per run: AddWorkflow keys on (namespace, Name, Version) and
		// treats a differing DefinitionHash under the same key as a conflict.
		// The def embeds a freshly built module's digest, so its hash differs
		// whenever the guest is rebuilt.
		Name:           uniqueTopic("xflow-wasm-binding"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: wasmBindTriggerType},
			// The definition carries the module's DIGEST and a declaration that
			// it consumes the supply — never the rules themselves. That absence
			// is the point: it is what proves the rules travelled over the supply
			// channel and were routed by the derived binding.
			{Name: taggerNode, Kind: types.NodeKindAction, Type: "xflow.script", Parameters: map[string]any{
				"language":        "wasm",
				"runtime":         "wazero-reactor",
				"artifact_digest": digest,
			}},
			{Name: wasmBindSupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: taggerNode, Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: taggerNode, Supply: wasmBindSupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	// Two PUTs so the revision the module reports is 2. Revision 1 would still
	// be distinguishable from the legacy path's 0, but 2 also rules out the
	// module having been configured from a stale first fetch.
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[{"name":"v1"}]}`))
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[{"name":"from-supply"}]}`))
	const wantRevision = uint64(2)

	// The resolver stands in for the runner's artifact fetch. cmd/runner builds
	// this closure over a read-through artifact store; serving the bytes directly
	// keeps this test on its own subject (derivation -> transport -> registration)
	// instead of also exercising artifact HTTP transport, which has its own
	// coverage and would need an ArtifactIndex the memstore harness has no
	// equivalent for.
	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		if gotDigest != digest {
			t.Errorf("artifact resolver called with digest %q, want %q", gotDigest, digest)
		}
		return guest, nil
	}
	tracker := newWasmBindingRunner(httpSrv.URL, token, resolve)

	labels := map[string]string{"zone": zoneLabel}
	caps := []protocol.Capability{{NodeType: wasmBindTriggerType}, {NodeType: "xflow.script"}}

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:          runnerID,
			Concurrency:       1,
			Labels:            labels,
			Capabilities:      caps,
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runnerCtx) }()
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)

	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: wfID, WorkflowVersion: wfVersion, EntryUnitID: entryUnitID,
	}
	act, _, err := cp.EntryActivationStore().Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after reconcile: %v", err)
	}
	if act.RunnerID != runnerID {
		t.Fatalf("reconcile must assign %s, got %q", runnerID, act.RunnerID)
	}
	// The durable record is where the derivation lands; if it is empty the
	// directive can carry nothing and the runner-side assertions below would be
	// testing an empty list.
	wantBinding := []engine.SupplyConsumerBinding{{ModuleDigest: digest, SupplyNode: wasmBindSupplyNode}}
	if len(act.SupplyConsumers) != 1 || act.SupplyConsumers[0] != wantBinding[0] {
		t.Fatalf("derived SupplyConsumers = %+v, want %+v", act.SupplyConsumers, wantBinding)
	}

	// --- (a): the activation is hosted, so the registration below happened on
	// the real activation path. ---
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) == 0 {
		t.Fatal("(a) the runner never hosted the activation; nothing downstream can be attributed to the binding")
	}
	if !trigger.wasActivated() {
		t.Fatal("(a) the trigger handler was never activated")
	}

	// --- (b): exactly one consumer registered for this supply. ---
	if got := obs.get(wasmBindSupplyNode); got != 1 {
		t.Fatalf("(b) consumers for %q = %d, want 1 — without a registration the "+
			"content reaches nobody and the module evaluates against no rules",
			wasmBindSupplyNode, got)
	}

	// --- (c): the module runs and reports the supply's revision. Executed with
	// the inline code string rather than the digest because the wasm host derives
	// the same module key from the same bytes either way, so this lands on the
	// very engine the digest registration seeded. ---
	out, err := (&node.ScriptNode{}).Execute(ctx, &types.Input{
		Params: map[string]any{
			"code":     base64.StdEncoding.EncodeToString(guest),
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		Data: map[string]any{"payload": "x"},
	})
	if err != nil {
		t.Fatalf("(c) module execution failed: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("(c) the module refuses traffic (%#v) — a source-driven module with "+
			"no configured pool, i.e. the consumer was registered against a module "+
			"that was never compiled", out.Data)
	}
	gen, _ := out.Data["config_generation"].(uint64)
	if gen != wantRevision {
		t.Fatalf("(c) config_generation = %d, want %d (the supply revision); 0 means the "+
			"module fell back to the legacy globals path and evaluated against no rules",
			gen, wantRevision)
	}
	matched, _ := out.Data["matched"].([]any)
	if len(matched) != 1 || matched[0] != "from-supply" {
		t.Fatalf("(c) matched = %#v, want [from-supply] — the module was configured "+
			"from content other than the latest supply revision", out.Data["matched"])
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}
