//go:build integration

package integration

import (
	"context"
	"encoding/base64"
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

// mapBodyTriggerType is this test's own fake trigger node type. Registered in
// the global node registry so both the control plane's capability derivation and
// the runner's TriggerHandlerLookup resolve it, without needing a broker.
const mapBodyTriggerType = "test.mapbody.trigger"

// mapBodySupplyNode is this test's supply NODE name, distinct from every other
// test's: supply.Default is process-wide and UnregisterConsumer never drops the
// applied Snapshot, so a shared name would leave content resident across tests.
const mapBodySupplyNode = "rules_mapbody"

// fakeMapBodyTrigger activates into a no-op subscription and records that
// Activate ran, so the test can tell "the consumer was registered as part of a
// real activation" from "the activation never happened".
type fakeMapBodyTrigger struct {
	mu        sync.Mutex
	activated bool
}

func (h *fakeMapBodyTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: mapBodyTriggerType, Kind: types.NodeKindTrigger}
}

func (h *fakeMapBodyTrigger) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

func (h *fakeMapBodyTrigger) Activate(_ context.Context, _ *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	h.mu.Lock()
	h.activated = true
	h.mu.Unlock()
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

func (h *fakeMapBodyTrigger) wasActivated() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activated
}

// appendWasmCustomSection returns the module with one extra custom section
// carrying the given name as its payload, which changes its bytes — and
// therefore its digest and the wasm host's module key — without changing what it
// does.
//
// This test and TestSupplyConsumerBindingReachesRunner both drive the
// reactorseam guest, and the host keys its instance pool on the sha256 of the
// bytes. Identical bytes would put both tests' consumers on ONE pool: each
// registration rebuilds it from its own supply, so whichever ran last would
// decide the config_generation both assert on. The failure would be a
// cross-test ordering artifact, not a defect — the class of pollution
// supply.Default is already prone to.
//
// A custom section is the right lever: the spec requires implementations to
// ignore sections they do not recognise, so the module still validates and runs
// unchanged. Encoding is section id 0x00, then a uvarint byte count covering
// everything that follows: the name's own uvarint length, the name, and the
// payload. The payload must be non-empty — wazero decodes a custom section by
// reading exactly (section size - name size) bytes, and bytes.Reader answers a
// zero-length read at the end of the module with io.EOF, which surfaces as
// "failed to read custom section name[...]: EOF" and fails the compile.
func appendWasmCustomSection(module []byte, name string) []byte {
	var body []byte
	body = appendULEB128(body, uint32(len(name)))
	body = append(body, name...)
	body = append(body, name...) // payload; see the note on the zero-length read

	out := make([]byte, 0, len(module)+len(body)+8)
	out = append(out, module...)
	out = append(out, 0x00)
	out = appendULEB128(out, uint32(len(body)))
	return append(out, body...)
}

// appendULEB128 appends v in the unsigned LEB128 encoding the wasm binary
// format uses for every length and index.
func appendULEB128(dst []byte, v uint32) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}

// TestSupplyConsumerBindingReachesMapBodyModule is the end-to-end proof for the
// topology SAS actually deploys: the wasm module does NOT sit on the graph as a
// node — it lives inside an xflow.map node's BODY, and the supply it consumes
// hangs off the map node, because a body-local dependency edge is dropped twice
// over on the way through the compiler.
//
// That difference is the whole subject. SupplyConsumerBindingsForEntryUnit walks
// the compiled graph's flow edges, and a body member is not a graph node: before
// this wiring the walk saw the map node, saw its supply refs, found no wasm
// script AT that node, and derived nothing. The directive then carried the
// supply's CONTENT but no binding, so the runner fetched the rules and handed
// them to nobody. Both guests stayed on the legacy globals path, compiled an
// EMPTY rule set, reported configure success, and discarded nothing — every
// record of production traffic through unfiltered, with no error anywhere.
//
// It is the same three-part assertion as
// TestSupplyConsumerBindingReachesRunner, re-asked of a body member:
//
//	(a) the activation is hosted, so what follows is the real activation path;
//	(b) exactly one consumer is registered for the supply;
//	(c) the module RUNS and reports the supply's revision as the content that
//	    configured it — 0 would mean the legacy globals path (no rules at all).
//
// Nothing here stuffs a binding or calls configure by hand: the binding is
// derived by the server from the definition, travels on the heartbeat directive,
// and is registered by the receiving runner.
func TestSupplyConsumerBindingReachesMapBodyModule(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Tagged with a custom section so these bytes — and thus the module key the
	// wasm host pools on — are this test's alone, even though the guest source is
	// shared with TestSupplyConsumerBindingReachesRunner.
	guest := appendWasmCustomSection(buildReactorSeamWasm(t), "xflow-mapbody-binding-e2e")
	// store.ContentHash is what the control plane records as artifact_digest and
	// what the wasm host derives its module key from. The two must agree or the
	// registration targets a module that does not exist.
	digest := store.ContentHash(guest)

	trigger := &fakeMapBodyTrigger{}
	registry.Register(trigger)

	obs := &bindCountingObserver{}
	supply.Default.SetObserver(obs)
	t.Cleanup(func() { supply.Default.SetObserver(nil) })
	// supply.Default outlives this test; a registration left behind would keep
	// rebuilding this module's pool on every later Apply in this binary.
	t.Cleanup(func() { node.UnregisterWasmSupplyConsumerByDigest(digest, mapBodySupplyNode) })

	httpSrv, cp, token := newSupplyGatingControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_mapbody"
		mapNode     = "collect_mapbody"
		bodyMember  = "decode_mapbody"
		supplyRes   = "mapbody-rules"
		zoneLabel   = "mapbody"
		runnerID    = "runner-mapbody"
	)

	def := &types.WorkflowDef{
		// Unique per run: AddWorkflow keys on (namespace, Name, Version) and
		// treats a differing DefinitionHash under the same key as a conflict.
		Name:           uniqueTopic("xflow-mapbody-binding"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: mapBodyTriggerType},
			// The wasm module is a BODY member — not a node of this graph. The
			// "type": "xflow.subgraph" wrapper is mandatory: ParseTransformSpec
			// rejects a bare {nodes, connections} body outright.
			{Name: mapNode, Kind: types.NodeKindAction, Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.messages",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name": bodyMember,
								"type": "xflow.script",
								"parameters": map[string]any{
									"language":        "wasm",
									"runtime":         "wazero-reactor",
									"artifact_digest": digest,
								},
							},
						},
					},
				},
			}},
			{Name: mapBodySupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: mapNode, Input: "main"}}}},
		},
		// The edge hangs on the MAP node, which is the only place it can: a body
		// carries just its nodes and connections, so a body-local DependsOn is
		// dropped by both subgraphBodyParam and decodeSubgraphMembers.
		DependencyEdges: []types.DependencyEdge{
			{Node: mapNode, Supply: mapBodySupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	// Three PUTs, so the revision the module reports is 3. Distinguishable from
	// the legacy path's 0, and from a stale configure off an earlier fetch.
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[{"name":"v1"}]}`))
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[{"name":"v2"}]}`))
	putSupplyContent(t, httpSrv.URL, client, supplyRes, []byte(`{"rules":[{"name":"from-map-body"}]}`))
	const wantRevision = uint64(3)

	// Stands in for the runner's artifact fetch. Serving the bytes directly keeps
	// this test on its own subject (body-member derivation → transport →
	// registration); artifact HTTP transport has its own coverage.
	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		if gotDigest != digest {
			t.Errorf("artifact resolver called with digest %q, want %q", gotDigest, digest)
		}
		return guest, nil
	}
	tracker := newWasmBindingRunner(httpSrv.URL, token, resolve)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	// The entry unit's derived SupplyConsumers are declaration-shaped (DigestExpr,
	// not ModuleDigest), so DeriveEntryActivations appends a
	// FeatureWasmSupplyDeclarationV1 requirement on the trigger's NodeType
	// (service/control/entry_activation_manager.go's requireDeclarationCapability).
	// Without it here, MatchCapabilities never matches this runner and Reconcile
	// leaves the activation's RunnerID empty — a runner-selection miss, not a
	// binding-shape defect, but one that fails before the shape is ever checked.
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:    runnerID,
			Concurrency: 1,
			Labels:      map[string]string{"zone": zoneLabel},
			Capabilities: []protocol.Capability{
				{NodeType: mapBodyTriggerType, Features: []string{engine.FeatureWasmSupplyDeclarationV1}},
				{NodeType: "xflow.map"},
				{NodeType: "xflow.script"},
			},
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
	// The durable record is where the derivation lands. An empty list here is the
	// exact pre-fix state: the supply is delivered, nobody consumes it.
	//
	// This is a DECLARATION, not the legacy shape: the control plane never sets
	// ModuleDigest (collectWasmBindings carries artifact_digest verbatim as
	// DigestExpr, because it may be an expression -- spec §4.2). A body member's
	// runtime WorkflowName is the body-bearing (map) node's name, per
	// types.Input.WorkflowName's contract -- not the workflow's own name. See
	// task-10-corrections.md C-0.
	wantBinding := engine.SupplyConsumerBinding{
		SupplyNode:   mapBodySupplyNode,
		WorkflowName: mapNode,
		NodeName:     bodyMember,
		DigestExpr:   digest,
	}
	if len(act.SupplyConsumers) != 1 || act.SupplyConsumers[0] != wantBinding {
		t.Fatalf("derived SupplyConsumers = %+v, want [%+v] — the map BODY's wasm module "+
			"was not paired with the supply its parent map node depends on, so it would "+
			"evaluate every record against an empty rule set. Supplies = %+v",
			act.SupplyConsumers, wantBinding, act.Supplies)
	}
	if act.SupplyConsumers[0].ModuleDigest != "" {
		t.Fatalf("derived SupplyConsumers[0].ModuleDigest = %q, want empty -- a declaration "+
			"never carries the module digest; a literal artifact_digest travels verbatim as "+
			"DigestExpr instead", act.SupplyConsumers[0].ModuleDigest)
	}

	// --- (a): the activation is hosted. ---
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

	// --- (b): two consumers registered for this supply. Task 8/9's warm-up
	// consumer (service/runner/wasm_supply_declaration.go's wasmWarmupConsumer)
	// registers under its own "warmup/workflow/node@supply" key at activation
	// time and, once it resolves and compiles the digest, ALSO registers the
	// module's own "supply@moduleKey" key (node.RegisterWasmSupplyConsumerByDigest,
	// :94) so the module keeps receiving content directly. Both keys are real,
	// distinct, intentional registrations against the SAME supply name -- this
	// assertion predates the warm-up consumer and asserted 1 before it existed.
	if got := obs.get(mapBodySupplyNode); got != 2 {
		t.Fatalf("(b) consumers for %q = %d, want 2 (warm-up consumer + module "+
			"consumer) — without a registration the content reaches nobody and the "+
			"body's module evaluates against no rules",
			mapBodySupplyNode, got)
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
		t.Fatalf("(c) the module refuses traffic (%#v) — a source-driven module with no "+
			"configured pool, i.e. the consumer was registered against a module that was "+
			"never compiled", out.Data)
	}
	gen, _ := out.Data["config_generation"].(uint64)
	if gen != wantRevision {
		t.Fatalf("(c) config_generation = %d, want %d (the supply revision); 0 means the "+
			"body's module fell back to the legacy globals path and evaluated against no rules",
			gen, wantRevision)
	}
	matched, _ := out.Data["matched"].([]any)
	if len(matched) != 1 || matched[0] != "from-map-body" {
		t.Fatalf("(c) matched = %#v, want [from-map-body] — the module was configured from "+
			"content other than the latest supply revision", out.Data["matched"])
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}
