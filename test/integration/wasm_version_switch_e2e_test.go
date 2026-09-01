//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// This file is task-10-brief.md's four §9.3 acceptance probes for the
// expression-artifact-digest feature: artifact_digest may now be an EXPRESSION
// (a "declaration") resolved per execution against a supply's CURRENT content,
// rather than a literal digest fixed at compile time. Probes ③ and ④ are
// RELEASE GATES per the brief.
//
// The harness SHAPE is borrowed from wasm_supply_binding_e2e_test.go per
// task-10-corrections.md C-2: buildReactorSeamWasm, newWasmBindingRunner,
// putSupplyContent, and distributed-mode control-plane wiring reused directly
// (same package). newVersionSwitchControlPlane below is the one NEW piece of
// wiring this file needs: it additionally sets control.Config.Supplies so the
// SupplyHinter is constructed and heartbeat-piggybacked supply hints actually
// flow (service/control/controlplane.go:406) -- required by probes ② and ④,
// which depend on a POST-activation content change reaching an already-hosted
// runner, unlike wasm_supply_binding_e2e_test.go's single activation-time fetch.

const (
	vswitchDeclareTriggerType      = "test.vswitch.declare.trigger"
	vswitchFlipTriggerType         = "test.vswitch.flip.trigger"
	vswitchUnresolvableTriggerType = "test.vswitch.unresolvable.trigger"
	vswitchRollbackTriggerType     = "test.vswitch.rollback.trigger"

	vswitchDeclareSupplyNode      = "rules_vswitch_declare"
	vswitchFlipSupplyNode         = "rules_vswitch_flip"
	vswitchUnresolvableSupplyNode = "rules_vswitch_unresolvable"
	vswitchRollbackSupplyNode     = "rules_vswitch_rollback"

	vswitchUnresolvableSinkType = "test.vswitch.unresolvable.sink"
)

// fakeVersionSwitchTrigger activates into a no-op subscription. It records
// that Activate ran, and -- unlike wasm_supply_binding_e2e_test.go's fake
// trigger -- it also captures the entry-seed runtime the directive carries, so
// a test can seed executions ON DEMAND (any number of times, with distinct
// admission keys) instead of only once, synchronously, inside Activate.
type fakeVersionSwitchTrigger struct {
	nodeType  string
	mu        sync.Mutex
	activated bool
	seedRT    types.EntrySeedRuntime
}

func newFakeVersionSwitchTrigger(nodeType string) *fakeVersionSwitchTrigger {
	return &fakeVersionSwitchTrigger{nodeType: nodeType}
}

func (h *fakeVersionSwitchTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindTrigger}
}

func (h *fakeVersionSwitchTrigger) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

func (h *fakeVersionSwitchTrigger) Activate(_ context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	h.mu.Lock()
	h.activated = true
	if rt, ok := input.Runtime.(types.EntrySeedRuntime); ok {
		h.seedRT = rt
	}
	h.mu.Unlock()
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

func (h *fakeVersionSwitchTrigger) wasActivated() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activated
}

func (h *fakeVersionSwitchTrigger) seedRuntime() types.EntrySeedRuntime {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seedRT
}

// vswitchSinkHandler counts successful executions so probe ③ can assert zero
// records ever reached downstream of a node whose artifact_digest can never
// resolve.
type vswitchSinkHandler struct{ counter *gatingCounter }

func (h vswitchSinkHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: vswitchUnresolvableSinkType, Kind: types.NodeKindAction}
}

func (h vswitchSinkHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	h.counter.add(1)
	return &types.Output{Data: map[string]any{"handled": true}}, nil
}

// digestCallLog records, in order, every digest the artifact resolver was
// asked to fetch. Per task-10-corrections.md C-5, this is probe ④'s real
// observable: node.WasmSupplyConfigured is insert-only and cannot show that a
// digest STOPPED being used, but the resolver is only ever called once per
// newly-seen digest (script.go's sharedArtifactCode cache, and the wasm
// warm-up consumer's WasmSupplyConfigured short-circuit) -- so its call
// sequence is exactly "which module did the system actually resolve, and
// when".
type digestCallLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *digestCallLog) record(d string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, d)
}

func (l *digestCallLog) count(d string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		if c == d {
			n++
		}
	}
	return n
}

// newVersionSwitchControlPlane mirrors newSupplyGatingControlPlane
// (supply_gating_test.go), but additionally wires control.Config.Supplies so
// the SupplyHinter is constructed. Without it the server never piggybacks a
// SupplyHints field onto HeartbeatResponse (service/control/controlplane.go:406),
// and a POST-activation content change would never reach an already-hosted
// runner within this test's lifetime -- probes ② and ④ depend on exactly that.
// It also returns the backend's StateStore so a test can poll seeded
// executions to a terminal status directly (probe ③), and the *memstore.Store
// itself so callers can seed supply content directly (HTTP PUT
// /v1/supplies/{name} is sealed, spec appendix Z.5).
func newVersionSwitchControlPlane(t *testing.T, redisAddr string) (*httptest.Server, *control.ControlPlane, string, engine.StateStore, *memstore.Store) {
	t.Helper()
	const token = "vswitch-test-token-0123456789ab"

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	flushAsynqKeys(context.Background(), t, rdb)
	flushXflowKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	be, err := distributed.New(redisAddr, nil, distributed.WithConsumer(true), distributed.WithConcurrency(1))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	entryStore := be.NewEntryActivationStore(time.Minute)

	supplies := memstore.New()
	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: entryStore,
		Supplies:             supplies,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	srv, err := apiserver.New(apiserver.Config{
		Supplies:      supplies,
		PrincipalAuth: apiserver.NewBearerPrincipalAuth(token, "vswitch-test", []string{"workflow", "execution", "supply.write", "supply.read"}),
		Authorizer:    apiserver.ScopeAuthorizer{},
		AuditSink:     apiserver.NewInMemoryAuditSink(),
	}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("apiserver.Start: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	return httpSrv, cp, token, be.State(), supplies
}

// newWasmBindingRunnerWithGate mirrors newWasmBindingRunner (same package,
// wasm_supply_binding_e2e_test.go), but also returns the SupplyGate so a test
// can additionally wire it into runnersvc.Config.SupplyGate.
//
// That second wiring is required for any POST-activation content change to
// reach an already-hosted runner: TriggerActivationHandler's WithSupplyGate
// (used here) only fetches once, synchronously, inside Activate. The runner's
// own processSupplyHints -- driven by HeartbeatResponse.SupplyHints, which
// only exists once control.Config.Supplies is set -- calls ApplyHints on
// whatever gate sits in runnersvc.Config.SupplyGate (service/runner/runner.go).
// A gate wired only into the activation handler, as wasm_supply_binding_e2e_test.go
// does, never observes a hint: that test never changes supply content after
// activation, so it never needed this.
func newWasmBindingRunnerWithGate(baseURL, token string, resolve func(context.Context, string) ([]byte, error)) (*runnersvc.ActivationTracker, *runnersvc.SupplyGate) {
	fetcher := &runnersvc.HTTPSupplyFetcher{BaseURL: baseURL, Token: token}
	gate := runnersvc.NewSupplyGate(fetcher, supply.Default, slog.Default())
	handler := runnersvc.NewTriggerActivationHandler(baseURL, token, registryTriggerLookupGating{},
		runnersvc.WithSupplyGate(gate),
		runnersvc.WithArtifactCodeResolver(resolve))
	return runnersvc.NewActivationTracker(handler, slog.Default()), gate
}

// TestExpressionDigestActivatesEndToEnd is probe ①: an artifact_digest
// EXPRESSION -- not a literal -- must produce a declaration binding
// (DigestExpr set, ModuleDigest empty), reconcile onto a runner whose
// capability advertises FeatureWasmSupplyDeclarationV1, activate the trigger,
// and have the runner's supply-declaration forwarding layer record that this
// node declares the supply -- all without a real dispatch cycle.
func TestExpressionDigestActivatesEndToEnd(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Tagged so these bytes -- and thus the module key the wasm host pools on
	// -- are unique to this test: buildReactorSeamWasm always compiles the same
	// guest source, and an untagged digest here would collide with every other
	// probe in this file that also builds it, sharing ONE pool object across
	// tests via the process-global supply.Default (see the note on guestA in
	// TestPointerFlipIsObservedAsModuleReady and TestRollbackKeepsReceivingContentUpdates).
	guest := appendWasmCustomSection(buildReactorSeamWasm(t), "xflow-vswitch-declare")
	digest := store.ContentHash(guest)

	trigger := newFakeVersionSwitchTrigger(vswitchDeclareTriggerType)
	registry.Register(trigger)

	httpSrv, cp, token, _, supplies := newVersionSwitchControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_vswitch_declare"
		taggerNode  = "tag_vswitch_declare"
		supplyRes   = "vswitch-declare-rules"
		zoneLabel   = "vswitch-declare"
		runnerID    = "runner-vswitch-declare"
	)
	wantExpr := "${{ $supplies." + vswitchDeclareSupplyNode + ".digest }}"

	def := &types.WorkflowDef{
		Name:           uniqueTopic("xflow-vswitch-declare"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: vswitchDeclareTriggerType},
			{Name: taggerNode, Kind: types.NodeKindAction, Type: "xflow.script", Parameters: map[string]any{
				"language":        "wasm",
				"runtime":         "wazero-reactor",
				"artifact_digest": wantExpr,
			}},
			{Name: vswitchDeclareSupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: taggerNode, Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: taggerNode, Supply: vswitchDeclareSupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	// supply.Default outlives this test; a registration left behind would keep
	// rebuilding a dead module's pool on every later Apply in this binary. The
	// owner must match what the declaration path uses -- the warm-up consumer
	// and the execution-time guard both register/release under
	// "node:" + WorkflowName + "/" + NodeName (spec Z.4/Z.8).
	t.Cleanup(func() {
		node.UnregisterWasmSupplyConsumerByDigest(digest, vswitchDeclareSupplyNode, "node:"+def.Name+"/"+taggerNode)
	})

	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digest+`","rules":[{"name":"from-declare"}]}`))

	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		if gotDigest != digest {
			t.Errorf("artifact resolver called with digest %q, want %q", gotDigest, digest)
		}
		return guest, nil
	}
	tracker := newWasmBindingRunner(httpSrv.URL, token, resolve)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:    runnerID,
			Concurrency: 1,
			Labels:      map[string]string{"zone": zoneLabel},
			Capabilities: []protocol.Capability{
				{NodeType: vswitchDeclareTriggerType, Features: []string{engine.FeatureWasmSupplyDeclarationV1}},
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

	wantBinding := engine.SupplyConsumerBinding{
		SupplyNode:   vswitchDeclareSupplyNode,
		WorkflowName: def.Name,
		NodeName:     taggerNode,
		DigestExpr:   wantExpr,
	}
	if len(act.SupplyConsumers) != 1 || act.SupplyConsumers[0] != wantBinding {
		t.Fatalf("derived SupplyConsumers = %+v, want [%+v] -- an artifact_digest EXPRESSION must "+
			"travel verbatim as DigestExpr so it can be re-evaluated per execution against the "+
			"supply's current content", act.SupplyConsumers, wantBinding)
	}
	if act.SupplyConsumers[0].ModuleDigest != "" {
		t.Fatalf("derived SupplyConsumers[0].ModuleDigest = %q, want empty -- a declaration never "+
			"carries the module digest", act.SupplyConsumers[0].ModuleDigest)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) == 0 {
		t.Fatal("the runner never hosted the activation; nothing downstream can be attributed to the declaration")
	}
	if !trigger.wasActivated() {
		t.Fatal("the trigger handler was never activated")
	}

	deadline = time.Now().Add(20 * time.Second)
	var declared []string
	for time.Now().Before(deadline) {
		declared = node.WasmSupplyDeclarations(def.Name, taggerNode)
		if len(declared) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(declared) != 1 || declared[0] != vswitchDeclareSupplyNode {
		t.Fatalf("WasmSupplyDeclarations(%q, %q) = %v, want [%q] -- the runner's forwarding layer "+
			"never recorded the declaration, so a wasm node driven by an expression would be "+
			"evaluated with an empty rule set at execution time (spec §4.3.1's guard)",
			def.Name, taggerNode, declared, vswitchDeclareSupplyNode)
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}

// TestPointerFlipIsObservedAsModuleReady is probe ②: once a pointer supply
// resolves to digest A and the module is compiled and registered, flipping
// the pointer to a different digest B must be observed, end to end, as B
// becoming ready+configured too.
//
// Per task-10-corrections.md C-6, the criterion is L1' (IsReady AND
// WasmSupplyConfigured), not bare L1: Task 9's warm-up consumer is
// SYNCHRONOUS (Registry.Apply holds inFlight across the notify loop), so a
// supply with an outstanding verdict cannot report ready -- but a supply with
// NO registered consumers is ALWAYS ready, so IsReady alone would go green
// with this whole feature unimplemented.
func TestPointerFlipIsObservedAsModuleReady(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// guestA is tagged too, not just guestB: buildReactorSeamWasm always
	// compiles the same guest source, and an untagged digest here would
	// collide with every OTHER test in this file that also builds an
	// untagged guest (they share ONE pool object, keyed by content hash, via
	// the process-global supply.Default) -- observed directly: without this
	// tag, TestRollbackKeepsReceivingContentUpdates's "digest A" is the exact
	// same bytes as this test's, and whichever test runs first leaves
	// config_generation state the other inherits.
	guestA := appendWasmCustomSection(buildReactorSeamWasm(t), "xflow-vswitch-flip-a")
	guestB := appendWasmCustomSection(guestA, "xflow-vswitch-flip-b")
	digestA := store.ContentHash(guestA)
	digestB := store.ContentHash(guestB)
	modules := map[string][]byte{digestA: guestA, digestB: guestB}

	trigger := newFakeVersionSwitchTrigger(vswitchFlipTriggerType)
	registry.Register(trigger)

	httpSrv, cp, token, _, supplies := newVersionSwitchControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_vswitch_flip"
		taggerNode  = "tag_vswitch_flip"
		supplyRes   = "vswitch-flip-rules"
		zoneLabel   = "vswitch-flip"
		runnerID    = "runner-vswitch-flip"
	)
	digestExpr := "${{ $supplies." + vswitchFlipSupplyNode + ".digest }}"

	def := &types.WorkflowDef{
		Name:           uniqueTopic("xflow-vswitch-flip"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: vswitchFlipTriggerType},
			{Name: taggerNode, Kind: types.NodeKindAction, Type: "xflow.script", Parameters: map[string]any{
				"language":        "wasm",
				"runtime":         "wazero-reactor",
				"artifact_digest": digestExpr,
			}},
			{Name: vswitchFlipSupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: taggerNode, Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: taggerNode, Supply: vswitchFlipSupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	// supply.Default outlives this test; a registration left behind would keep
	// rebuilding a dead module's pool on every later Apply in this binary. The
	// owner must match what the declaration path uses -- the warm-up consumer
	// and the execution-time guard both register/release under
	// "node:" + WorkflowName + "/" + NodeName (spec Z.4/Z.8).
	t.Cleanup(func() {
		node.UnregisterWasmSupplyConsumerByDigest(digestA, vswitchFlipSupplyNode, "node:"+def.Name+"/"+taggerNode)
	})
	t.Cleanup(func() {
		node.UnregisterWasmSupplyConsumerByDigest(digestB, vswitchFlipSupplyNode, "node:"+def.Name+"/"+taggerNode)
	})

	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestA+`","rules":[{"name":"from-a"}]}`))

	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		mod, ok := modules[gotDigest]
		if !ok {
			t.Errorf("artifact resolver called with unknown digest %q, want %q or %q", gotDigest, digestA, digestB)
			return nil, fmt.Errorf("unknown digest %q", gotDigest)
		}
		return mod, nil
	}
	tracker, gate := newWasmBindingRunnerWithGate(httpSrv.URL, token, resolve)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:    runnerID,
			Concurrency: 1,
			Labels:      map[string]string{"zone": zoneLabel},
			Capabilities: []protocol.Capability{
				{NodeType: vswitchFlipTriggerType, Features: []string{engine.FeatureWasmSupplyDeclarationV1}},
				{NodeType: "xflow.script"},
			},
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
			SupplyGate:        gate,
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

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) == 0 {
		t.Fatal("the runner never hosted the activation")
	}
	if !trigger.wasActivated() {
		t.Fatal("the trigger handler was never activated")
	}

	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if supply.Default.IsReady(vswitchFlipSupplyNode) && node.WasmSupplyConfigured(digestA) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !(supply.Default.IsReady(vswitchFlipSupplyNode) && node.WasmSupplyConfigured(digestA)) {
		t.Fatalf("pointer A (%s) was never observed ready+configured within the deadline", digestA)
	}

	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestB+`","rules":[{"name":"from-b"}]}`))

	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if supply.Default.IsReady(vswitchFlipSupplyNode) && node.WasmSupplyConfigured(digestB) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !(supply.Default.IsReady(vswitchFlipSupplyNode) && node.WasmSupplyConfigured(digestB)) {
		t.Fatalf("pointer flip from A (%s) to B (%s) was never observed ready+configured within "+
			"the deadline -- a post-activation content change never reached the already-hosted "+
			"runner", digestA, digestB)
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}

// TestUnresolvableDigestDropsNoRecordsToSink is probe ③ [RELEASE GATE]: an
// artifact_digest expression that resolves to a well-formed digest string
// naming an artifact the resolver can never actually fetch must fail every
// execution -- fail closed -- rather than silently falling back to the
// node's legacy no-rules path and passing every record through unfiltered
// (the exact defect class TestSupplyConsumerBindingReachesRunner and
// TestSupplyConsumerBindingReachesMapBodyModule exist to catch for the
// binding-derivation side; this probe is the execution-time counterpart for
// the case where the digest never resolves to real content at all).
//
// Activation must still succeed: trigger_activation_handler.go's
// registerSupplyConsumers calls node.DeclareWasmSupplyConsumers
// unconditionally for a declaration binding (it never fails), and
// supply.Registry.RegisterConsumer has no error return -- so a warm-up
// consumer that can never compile its digest does not fail Activate. The
// node's own NodeDef carries no Retry policy, so engine/atomic_commit.go's
// tryRetryWithAttempt short-circuits to a terminal failure on the first
// attempt (settings == nil), giving each seeded execution a prompt terminal
// ExecutionStatusFailed instead of hanging in retry.
func TestUnresolvableDigestDropsNoRecordsToSink(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A well-formed digest (passes store.ValidateDigest) naming content that
	// was never actually stored anywhere -- the resolver below always errors
	// for it, standing in for a supply that names a module the artifact store
	// never received.
	unresolvableDigest := "sha256:" + strings.Repeat("ab", 32)
	if err := store.ValidateDigest(unresolvableDigest); err != nil {
		t.Fatalf("test fixture digest %q must be well-formed: %v", unresolvableDigest, err)
	}

	trigger := newFakeVersionSwitchTrigger(vswitchUnresolvableTriggerType)
	registry.Register(trigger)

	counter := newGatingCounter()
	registry.Register(vswitchSinkHandler{counter: counter})

	httpSrv, cp, token, state, supplies := newVersionSwitchControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_vswitch_unresolvable"
		taggerNode  = "tag_vswitch_unresolvable"
		sinkNode    = "sink_vswitch_unresolvable"
		supplyRes   = "vswitch-unresolvable-rules"
		zoneLabel   = "vswitch-unresolvable"
		runnerID    = "runner-vswitch-unresolvable"
	)
	digestExpr := "${{ $supplies." + vswitchUnresolvableSupplyNode + ".digest }}"

	def := &types.WorkflowDef{
		Name:           uniqueTopic("xflow-vswitch-unresolvable"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: vswitchUnresolvableTriggerType},
			{Name: taggerNode, Kind: types.NodeKindAction, Type: "xflow.script", Parameters: map[string]any{
				"language":        "wasm",
				"runtime":         "wazero-reactor",
				"artifact_digest": digestExpr,
			}},
			{Name: sinkNode, Kind: types.NodeKindAction, Type: vswitchUnresolvableSinkType},
			{Name: vswitchUnresolvableSupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: taggerNode, Input: "main"}}}},
			taggerNode:  {"main": {Targets: []types.Connection{{Node: sinkNode, Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: taggerNode, Supply: vswitchUnresolvableSupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+unresolvableDigest+`","rules":[{"name":"unreachable"}]}`))

	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		return nil, fmt.Errorf("simulated artifact store miss for %s", gotDigest)
	}
	tracker := newWasmBindingRunner(httpSrv.URL, token, resolve)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:    runnerID,
			Concurrency: 1,
			Labels:      map[string]string{"zone": zoneLabel},
			Capabilities: []protocol.Capability{
				{NodeType: vswitchUnresolvableTriggerType, Features: []string{engine.FeatureWasmSupplyDeclarationV1}},
				{NodeType: "xflow.script"},
				{NodeType: vswitchUnresolvableSinkType},
			},
			HeartbeatInterval:    100 * time.Millisecond,
			PollWait:             10 * time.Millisecond,
			ActivationTracker:    tracker,
			ArtifactCodeResolver: resolve,
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
		t.Fatalf("reconcile must assign %s, got %q -- activation must succeed even though the "+
			"digest can never resolve; a declaration binding never fails Activate", runnerID, act.RunnerID)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) == 0 {
		t.Fatal("the runner never hosted the activation")
	}
	if !trigger.wasActivated() {
		t.Fatal("the trigger handler was never activated")
	}

	seedRT := trigger.seedRuntime()
	if seedRT == nil {
		t.Fatal("the trigger's Activate never received an EntrySeedRuntime")
	}

	const numExecutions = 3
	executionIDs := make([]types.ExecutionID, 0, numExecutions)
	for i := 0; i < numExecutions; i++ {
		admissionKey := string(engine.BuildAdmissionKeySingle(
			namespace.Default, wfID, wfVersion, entryUnitID, "events", 0, int64(i),
		))
		resp, err := seedRT.SeedExecutionFromEntry(ctx, types.EntrySeedRequest{
			AdmissionKey:    admissionKey,
			WorkflowID:      wfID,
			WorkflowVersion: wfVersion,
			EntryUnitID:     entryUnitID,
			Outcome:         string(engine.GroupOutcomeSuccess),
			Exits: []types.BoundaryExit{{
				NodeName: entryUnitID,
				Port:     "main",
				Data:     map[string]any{"value": i},
			}},
		})
		if err != nil {
			t.Fatalf("SeedExecutionFromEntry #%d: %v", i, err)
		}
		if !resp.Accepted || resp.ExecutionID == "" {
			t.Fatalf("SeedExecutionFromEntry #%d: resp = %+v, want Accepted with a non-empty ExecutionID", i, resp)
		}
		executionIDs = append(executionIDs, resp.ExecutionID)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	for i, execID := range executionIDs {
		result := waitForCompletion(waitCtx, t, state, execID)
		if result.Status != types.ExecutionStatusFailed {
			t.Fatalf("execution #%d (%s) status = %q, want %q -- an artifact_digest that resolves "+
				"to a well-formed but permanently unfetchable digest must fail the node's "+
				"execution, not silently succeed against an empty/legacy rule set",
				i, execID, result.Status, types.ExecutionStatusFailed)
		}
	}

	if got := counter.load(); got != 0 {
		t.Fatalf("sink handler was invoked %d time(s), want 0 -- every one of %d records with an "+
			"unresolvable artifact_digest must be dropped before reaching downstream, not passed "+
			"through uncleansed", got, numExecutions)
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}

// TestRollbackKeepsReceivingContentUpdates is probe ④ [RELEASE GATE]: after a
// pointer supply flips from digest A to digest B and back to A, the node must
// still receive LIVE content updates while pinned on A -- not get stuck on
// whatever A's pool last held before the flip.
//
// Per task-10-corrections.md C-5, digest A and B are byte-distinct but
// BEHAVIOURALLY IDENTICAL (appendWasmCustomSection), so "tagged per A's vs
// B's ruleset" is not an observable: matched/config_generation come from
// supply CONTENT, not from which module ran. The real observable is the
// artifact resolver's call sequence (steps 1-3, "which digest did the system
// actually resolve"), and — because that alone cannot distinguish "rules
// content changed under an unchanged digest" — step 4 additionally executes
// the module directly and inspects matched/config_generation, exactly as
// wasm_supply_binding_e2e_test.go's step (c) does.
//
// Step 4 is the assertion with teeth (per the brief): it is expected to be the
// one that goes red under the C-9 mutation (node/internal/code/script/wasm/supply_consumer.go's
// consumerKeyFor collapsing to a bare supplyNode), because that mutation makes
// digest B's module-level consumer registration overwrite digest A's, so A's
// pool stops receiving content updates once B has ever been registered -- even
// after the pointer rolls back to A.
func TestRollbackKeepsReceivingContentUpdates(t *testing.T) {
	redisAddr := requireRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// guestA is tagged too, not just guestB -- see the identical note in
	// TestPointerFlipIsObservedAsModuleReady: an untagged buildReactorSeamWasm(t)
	// result is byte-identical across every test in this file, and they share
	// ONE pool object (keyed by content hash) via the process-global
	// supply.Default.
	guestA := appendWasmCustomSection(buildReactorSeamWasm(t), "xflow-vswitch-rollback-a")
	guestB := appendWasmCustomSection(guestA, "xflow-vswitch-rollback-b")
	digestA := store.ContentHash(guestA)
	digestB := store.ContentHash(guestB)
	modules := map[string][]byte{digestA: guestA, digestB: guestB}

	log := &digestCallLog{}

	trigger := newFakeVersionSwitchTrigger(vswitchRollbackTriggerType)
	registry.Register(trigger)

	httpSrv, cp, token, _, supplies := newVersionSwitchControlPlane(t, redisAddr)
	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kin_vswitch_rollback"
		taggerNode  = "tag_vswitch_rollback"
		supplyRes   = "vswitch-rollback-rules"
		zoneLabel   = "vswitch-rollback"
		runnerID    = "runner-vswitch-rollback"
	)
	digestExpr := "${{ $supplies." + vswitchRollbackSupplyNode + ".digest }}"

	def := &types.WorkflowDef{
		Name:           uniqueTopic("xflow-vswitch-rollback"),
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: vswitchRollbackTriggerType},
			{Name: taggerNode, Kind: types.NodeKindAction, Type: "xflow.script", Parameters: map[string]any{
				"language":        "wasm",
				"runtime":         "wazero-reactor",
				"artifact_digest": digestExpr,
			}},
			{Name: vswitchRollbackSupplyNode, Kind: types.NodeKindSupply, Type: "xflow.supply.external", Parameters: map[string]any{
				"resource": supplyRes, "require_ready": true,
			}},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: taggerNode, Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{
			{Node: taggerNode, Supply: vswitchRollbackSupplyNode},
		},
	}
	client := authedClient(token)
	wfID := registerWorkflowHTTP(t, httpSrv.URL, client, def)

	// supply.Default outlives this test; a registration left behind would keep
	// rebuilding a dead module's pool on every later Apply in this binary. The
	// owner must match what the declaration path uses -- the warm-up consumer
	// and the execution-time guard both register/release under
	// "node:" + WorkflowName + "/" + NodeName (spec Z.4/Z.8).
	t.Cleanup(func() {
		node.UnregisterWasmSupplyConsumerByDigest(digestA, vswitchRollbackSupplyNode, "node:"+def.Name+"/"+taggerNode)
	})
	t.Cleanup(func() {
		node.UnregisterWasmSupplyConsumerByDigest(digestB, vswitchRollbackSupplyNode, "node:"+def.Name+"/"+taggerNode)
	})

	// --- Step 1: pointer -> A, revision 1. ---
	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestA+`","rules":[{"name":"from-a"}]}`))
	const wantRevision1 = uint64(1)

	resolve := func(_ context.Context, gotDigest string) ([]byte, error) {
		log.record(gotDigest)
		mod, ok := modules[gotDigest]
		if !ok {
			return nil, fmt.Errorf("unknown digest %q (want %q or %q)", gotDigest, digestA, digestB)
		}
		return mod, nil
	}
	tracker, gate := newWasmBindingRunnerWithGate(httpSrv.URL, token, resolve)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, client),
		execution.NewRegistry(),
		runnersvc.Config{
			RunnerID:    runnerID,
			Concurrency: 1,
			Labels:      map[string]string{"zone": zoneLabel},
			Capabilities: []protocol.Capability{
				{NodeType: vswitchRollbackTriggerType, Features: []string{engine.FeatureWasmSupplyDeclarationV1}},
				{NodeType: "xflow.script"},
			},
			HeartbeatInterval: 100 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
			SupplyGate:        gate,
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

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(tracker.Inventory()) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	if got := tracker.Inventory(); len(got) == 0 {
		t.Fatal("the runner never hosted the activation")
	}
	if !trigger.wasActivated() {
		t.Fatal("the trigger handler was never activated")
	}

	// Confirm step 1 settled: A's pool is live and reports revision 1. Safe to
	// execute the module directly here -- only one digest has ever been
	// registered, so the C-9 mutation's collision cannot have happened yet.
	deadline = time.Now().Add(20 * time.Second)
	var out *types.Output
	for time.Now().Before(deadline) {
		out, err = (&node.ScriptNode{}).Execute(ctx, &types.Input{
			Params: map[string]any{
				"code":     base64.StdEncoding.EncodeToString(guestA),
				"language": "wasm",
				"runtime":  "wazero-reactor",
			},
			Data: map[string]any{"payload": "x"},
		})
		if err == nil && out.Port != "error" {
			if gen, _ := out.Data["config_generation"].(uint64); gen == wantRevision1 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("step 1: module A execution failed: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("step 1: module A refuses traffic (%#v)", out.Data)
	}
	if gen, _ := out.Data["config_generation"].(uint64); gen != wantRevision1 {
		t.Fatalf("step 1: config_generation = %d, want %d", gen, wantRevision1)
	}
	if log.count(digestA) < 1 {
		t.Fatalf("step 1: artifact resolver was never called with digest A (%s) -- the node never "+
			"actually resolved the pointer's initial value", digestA)
	}
	callsAfterStep1 := log.count(digestA)

	// --- Step 2: pointer -> B, revision 2. Must be resolved as a NEW digest. ---
	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestB+`","rules":[{"name":"from-b"}]}`))

	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && log.count(digestB) < 1 {
		time.Sleep(100 * time.Millisecond)
	}
	if log.count(digestB) < 1 {
		t.Fatalf("step 2: artifact resolver was never called with digest B (%s) within the "+
			"deadline -- the pointer flip from A (%s) to B was never picked up", digestB, digestA)
	}

	// --- Step 3: rollback, pointer -> A again (same content as step 1),
	// revision 3. Must be a live-engine hit: A is already configured, so the
	// resolver must NOT be asked for it again. This is exactly the
	// precondition of the bug probe ④ exists to catch (task-10-corrections.md
	// C-5), and it must hold under the C-9 mutation too -- the mutation
	// affects the MODULE-level consumer key, not this warm-up short-circuit. ---
	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestA+`","rules":[{"name":"from-a"}]}`))

	// Negative assertion: give the hint cycle real time to have acted (several
	// heartbeat intervals) before concluding "no new call happened".
	time.Sleep(3 * time.Second)
	if got := log.count(digestA); got != callsAfterStep1 {
		t.Fatalf("step 3 (rollback to A): artifact resolver was called with digest A (%s) %d "+
			"time(s) after rollback, want still %d (no new call) -- rollback to an already-"+
			"configured digest must be a live-engine hit, not a re-fetch", digestA, got, callsAfterStep1)
	}

	// --- Step 4 [the assertion with teeth]: content changes ("from-a" ->
	// "from-a-v2") while the pointer digest field stays on A, revision 4. A's
	// pool must pick up the new rules -- this is the module-level consumer
	// path, independent of the warm-up consumer's digest short-circuit, and is
	// exactly what the C-9 mutation (consumerKeyFor collapsing to a bare
	// supplyNode) breaks: B's module-level registration overwrote A's when B
	// was registered in step 2, so A's pool stops receiving broadcasts. ---
	putSupplyContent(t, supplies, supplyRes,
		[]byte(`{"digest":"`+digestA+`","rules":[{"name":"from-a-v2"}]}`))
	const wantRevision4 = uint64(4)

	deadline = time.Now().Add(20 * time.Second)
	var matched []any
	for time.Now().Before(deadline) {
		out, err = (&node.ScriptNode{}).Execute(ctx, &types.Input{
			Params: map[string]any{
				"code":     base64.StdEncoding.EncodeToString(guestA),
				"language": "wasm",
				"runtime":  "wazero-reactor",
			},
			Data: map[string]any{"payload": "x"},
		})
		if err == nil && out.Port != "error" {
			if gen, _ := out.Data["config_generation"].(uint64); gen == wantRevision4 {
				matched, _ = out.Data["matched"].([]any)
				if len(matched) == 1 && matched[0] == "from-a-v2" {
					break
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("step 4: module A execution failed: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("step 4: module A refuses traffic (%#v)", out.Data)
	}
	gen, _ := out.Data["config_generation"].(uint64)
	matched, _ = out.Data["matched"].([]any)
	if gen != wantRevision4 || len(matched) != 1 || matched[0] != "from-a-v2" {
		t.Fatalf("step 4: module A (digest %s) reports config_generation=%d matched=%#v, want "+
			"config_generation=%d matched=[from-a-v2] -- after rolling back to A and then "+
			"changing content while still pinned on A, the node's module must keep receiving "+
			"live content updates; a stale result here means A's consumer registration was lost "+
			"when B (digest %s) was registered (spec §4.2.1 candidate 2's key collision)",
			digestA, gen, matched, wantRevision4, digestB)
	}

	runnerCancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}
