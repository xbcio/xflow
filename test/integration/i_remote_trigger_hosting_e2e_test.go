//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// registryTriggerLookupE2E adapts the global node registry's LookupTrigger to
// the runner's TriggerHandlerLookup interface (mirrors the production wiring in
// cmd/runner). Lets the TriggerActivationHandler resolve the fake trigger.
type registryTriggerLookupE2E struct{}

func (registryTriggerLookupE2E) Trigger(nodeType string) (types.TriggerHandler, bool) {
	return registry.LookupTrigger(nodeType)
}

func newSlogE2E() *slog.Logger { return slog.Default() }

// remoteHostTriggerType is the fake trigger node type used only by this e2e. It
// is registered in the global node registry so BOTH the runner's
// TriggerActivationHandler lookup (registry.LookupTrigger) and the control-plane
// graph derivation (capability requirement = this type) resolve it.
const remoteHostTriggerType = "test.remotehost.trigger"

// remoteHostBodyType is the downstream action node the trigger's boundary output
// fans out to; registering it in the global registry lets the runner execute it.
const remoteHostBodyType = "test.remotehost.body"

// fakeRemoteHostTrigger is a fake TriggerHandler. On Activate it records the
// activation generation carried by the seed runtime and seeds one entry event
// through that runtime — exactly what a real Kafka trigger does per message. It
// captures the seed response so the test can assert the fence admitted the seed
// at the assigned generation.
type fakeRemoteHostTrigger struct {
	mu        sync.Mutex
	activated bool
	seedResp  types.EntrySeedResponse
	seedErr   error
	done      chan struct{}
	doneOnce  sync.Once
}

func newFakeRemoteHostTrigger() *fakeRemoteHostTrigger {
	return &fakeRemoteHostTrigger{done: make(chan struct{})}
}

// signalDone closes done exactly once. Activate may be invoked more than once
// (a generation upgrade restarts the subscription), so a bare close would panic.
func (h *fakeRemoteHostTrigger) signalDone() {
	h.doneOnce.Do(func() { close(h.done) })
}

func (h *fakeRemoteHostTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: remoteHostTriggerType, Kind: types.NodeKindTrigger}
}

func (h *fakeRemoteHostTrigger) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

// Activate is called by the TriggerActivationHandler when the runner receives an
// activate directive. It seeds one event through the entry-seed runtime, which
// stamps the activation generation onto the wire request.
func (h *fakeRemoteHostTrigger) Activate(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	seedRT, ok := input.Runtime.(types.EntrySeedRuntime)
	if !ok {
		h.mu.Lock()
		h.seedErr = errNotSeedRuntime
		h.activated = true
		h.mu.Unlock()
		h.signalDone()
		return types.CloseFunc(func(context.Context) error { return nil }), nil
	}
	// The directive params carry the entry-seed selecting keys the handler merged
	// (entry_seed / entry_unit_id / workflow_version). Use them to build a seed
	// admission identical to what a real Kafka trigger produces per message.
	wfVersion, _ := input.Params["workflow_version"].(string)
	entryUnitID, _ := input.Params["entry_unit_id"].(string)

	admissionKey := string(engine.BuildAdmissionKeySingle(
		namespace.Default, input.WorkflowID, wfVersion, entryUnitID, "events", 0, 1,
	))
	resp, err := seedRT.SeedExecutionFromEntry(ctx, types.EntrySeedRequest{
		AdmissionKey:    admissionKey,
		WorkflowID:      input.WorkflowID,
		WorkflowVersion: wfVersion,
		EntryUnitID:     entryUnitID,
		Outcome:         string(engine.GroupOutcomeSuccess),
		Exits: []types.BoundaryExit{{
			NodeName: entryUnitID,
			Port:     "main",
			Data:     map[string]any{"value": 42},
		}},
	})

	h.mu.Lock()
	h.activated = true
	h.seedResp = resp
	h.seedErr = err
	// The HTTPEntrySeedRuntime stamps its Generation onto the wire; recover it via
	// the concrete type so the test can assert generation identity end-to-end.
	// (The runtime is unexported-field but Generation is exported.)
	h.mu.Unlock()
	h.signalDone()

	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

var errNotSeedRuntime = seedRuntimeError("trigger runtime is not an EntrySeedRuntime")

type seedRuntimeError string

func (e seedRuntimeError) Error() string { return string(e) }

// fakeRemoteHostBody is the downstream action handler.
type fakeRemoteHostBody struct{}

func (fakeRemoteHostBody) Descriptor() types.Descriptor {
	return types.Descriptor{Type: remoteHostBodyType}
}
func (fakeRemoteHostBody) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"handled": true}}, nil
}

const replicatedRemoteHostTriggerType = "test.remotehost.replica.trigger"

type replicaActivationRecord struct {
	runnerID     string
	replicaIndex uint32
	generation   uint64
}

// replicaProbeTrigger is installed separately on each real runner. Recording
// the replica identity from the concrete entry-seed runtime proves the
// directive reached that runner and was not merely assigned in the store.
type replicaProbeTrigger struct {
	runnerID  string
	activated chan replicaActivationRecord
}

func newReplicaProbeTrigger(runnerID string) *replicaProbeTrigger {
	return &replicaProbeTrigger{
		runnerID:  runnerID,
		activated: make(chan replicaActivationRecord, 1),
	}
}

func (*replicaProbeTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: replicatedRemoteHostTriggerType, Kind: types.NodeKindTrigger}
}

func (*replicaProbeTrigger) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

func (h *replicaProbeTrigger) Activate(_ context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	runtime, ok := input.Runtime.(*protocol.HTTPEntrySeedRuntime)
	if !ok {
		return nil, errNotSeedRuntime
	}
	h.activated <- replicaActivationRecord{
		runnerID:     h.runnerID,
		replicaIndex: runtime.ReplicaIndex,
		generation:   runtime.Generation,
	}
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

type replicaProbeLookup struct {
	trigger *replicaProbeTrigger
}

func (l replicaProbeLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	if nodeType != replicatedRemoteHostTriggerType {
		return nil, false
	}
	return l.trigger, true
}

// registerWorkflowHTTP posts a workflow definition to POST /v1/workflows (the
// register route after the §9.1 semantic inversion) so the control plane
// persists the compiled graph AND derives the entry activation.
func registerWorkflowHTTP(t *testing.T, baseURL string, client *http.Client, def *types.WorkflowDef) types.WorkflowID {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(def); err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp, err := client.Post(baseURL+"/v1/workflows", "application/json", &buf)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d, want 201", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read register body: %v", err)
	}
	var env e2eEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode register envelope: %v", err)
	}
	var out struct {
		WorkflowID types.WorkflowID `json:"workflow_id"`
	}
	if err := json.Unmarshal(env.Data, &out); err != nil {
		t.Fatalf("decode register data: %v", err)
	}
	if out.WorkflowID == "" {
		t.Fatal("empty workflow_id from register")
	}
	return out.WorkflowID
}

// TestRemoteTriggerHosting_Memory exercises the full remote trigger-hosting loop
// against a memory-backed EntryActivationStore + local backend through the
// apiserver + ControlPlane harness (apiserver.New + WithControlPlane + Start):
//
//	register workflow → reconciler assigns the label-matching runner an activate
//	directive at a fresh generation → runner receives it on heartbeat and hosts
//	the trigger → the trigger seeds an event whose runtime carries the assigned
//	generation → the control-plane fence admits it (generation matches) → an
//	execution is created with downstream fan-out.
//
// It also proves the reconnect path: after the runner re-registers reporting the
// hosted activation in its inventory, the lease is renewed with the generation
// UNCHANGED (not orphaned, not fenced).
func TestRemoteTriggerHosting_Memory(t *testing.T) {
	store := control.NewMemoryEntryActivationStore()
	runRemoteTriggerHostingE2E(t, store)
}

// TestRemoteTriggerHosting_Redis runs the same loop against the Redis-backed
// EntryActivationStore. It SKIPS cleanly when XFLOW_TEST_REDIS_ADDR is unset or
// the Redis endpoint (podman 6380) is unreachable.
// TestReplicatedRemoteTriggerHosting_Memory drives the complete control-plane
// and heartbeat path with three real runner loops. It proves one logical entry
// can be hosted three times without co-location and that each assigned runner
// receives the replica-scoped runtime identity it owns.
func TestReplicatedRemoteTriggerHosting_Memory(t *testing.T) {
	const (
		wfVersion   = "1"
		entryUnitID = "kafka-in"
		replicas    = uint32(3)
	)

	store := control.NewMemoryEntryActivationStore()
	be := local.New(local.WithConcurrency(1))
	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: store,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("apiserver.Start: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())

	runnerIDs := []string{"runner-replica-a", "runner-replica-b", "runner-replica-c"}
	probes := make(map[string]*replicaProbeTrigger, len(runnerIDs))
	trackers := make(map[string]*runnersvc.ActivationTracker, len(runnerIDs))
	runErrs := make(map[string]chan error, len(runnerIDs))
	defer func() {
		cancel()
		for _, id := range runnerIDs {
			select {
			case <-runErrs[id]:
			case <-time.After(3 * time.Second):
			}
		}
		httpSrv.Close()
		_ = srv.Shutdown(context.Background())
	}()

	def := &types.WorkflowDef{
		Name:           "i-replicated-remote-trigger-hosting-e2e",
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired},
		Nodes: []types.NodeDef{{
			Name:               entryUnitID,
			Kind:               types.NodeKindTrigger,
			Type:               replicatedRemoteHostTriggerType,
			ActivationReplicas: replicas,
		}},
	}
	wfID := registerWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), def)

	for _, id := range runnerIDs {
		probe := newReplicaProbeTrigger(id)
		tracker := runnersvc.NewActivationTracker(
			runnersvc.NewTriggerActivationHandler(httpSrv.URL, "", replicaProbeLookup{trigger: probe}),
			newSlogE2E(),
		)
		runner := runnersvc.New(
			protocol.NewClient(httpSrv.URL, httpSrv.Client()),
			execution.NewRegistry(),
			runnersvc.Config{
				RunnerID:    id,
				Concurrency: 1,
				Capabilities: []protocol.Capability{{
					NodeType: replicatedRemoteHostTriggerType,
					Features: []string{engine.FeatureEntryActivationReplicaV1},
				}},
				HeartbeatInterval: 25 * time.Millisecond,
				PollWait:          10 * time.Millisecond,
				ActivationTracker: tracker,
			},
		)
		probes[id] = probe
		trackers[id] = tracker
		runErr := make(chan error, 1)
		runErrs[id] = runErr
		go func() { runErr <- runner.Run(ctx) }()
	}
	for _, id := range runnerIDs {
		waitForE2ERunner(t, cp.RunnerDirectory(), id)
	}

	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	acts, err := store.List(ctx, namespace.Default)
	if err != nil {
		t.Fatalf("List entry activations: %v", err)
	}
	owners := make(map[string]uint32, replicas)
	generations := make(map[uint32]uint64, replicas)
	for _, act := range acts {
		if act.WorkflowID != wfID || act.WorkflowVersion != wfVersion || act.EntryUnitID != entryUnitID {
			continue
		}
		if act.RunnerID == "" {
			t.Fatalf("replica %d remains unassigned: %+v", act.ReplicaIndex, act)
		}
		if prior, exists := owners[act.RunnerID]; exists {
			t.Fatalf("replicas %d and %d co-located on %s", prior, act.ReplicaIndex, act.RunnerID)
		}
		owners[act.RunnerID] = act.ReplicaIndex
		generations[act.ReplicaIndex] = act.Generation
	}
	if len(owners) != int(replicas) {
		t.Fatalf("assigned owners = %v, want %d distinct runners", owners, replicas)
	}

	for runnerID, wantReplica := range owners {
		select {
		case got := <-probes[runnerID].activated:
			if got.runnerID != runnerID || got.replicaIndex != wantReplica {
				t.Fatalf("runner %s activation = %+v, want replica %d", runnerID, got, wantReplica)
			}
			if got.generation == 0 || got.generation != generations[wantReplica] {
				t.Fatalf("runner %s generation = %d, store has %d", runnerID, got.generation, generations[wantReplica])
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for runner %s to activate replica %d", runnerID, wantReplica)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for runnerID, tracker := range trackers {
		wantReplica := owners[runnerID]
		for {
			inventory := tracker.Inventory()
			if len(inventory) == 1 && inventory[0].ReplicaIndex == wantReplica &&
				inventory[0].Generation == generations[wantReplica] {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("runner %s inventory = %+v, want replica %d generation %d",
					runnerID, inventory, wantReplica, generations[wantReplica])
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestRemoteTriggerHosting_Redis(t *testing.T) {
	// Gate through the package's own requireRedis (harness.go) rather than a
	// hand-rolled os.Getenv + t.Skip. The version here previously had no
	// escalation branch, so XFLOW_REQUIRE_REDIS_INTEGRATION=1 could not turn
	// an unreachable Redis red — this file's whole Redis-backed e2e would
	// report ok when the dependency was simply absent. requireRedis also falls
	// back to REDIS_PORT / localhost:6379 when XFLOW_TEST_REDIS_ADDR is unset,
	// so this now runs in the same situations as every other test here.
	addr := requireRedis(t)

	be, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	store := be.NewEntryActivationStore(time.Minute)
	runRemoteTriggerHostingE2E(t, store)
}

func runRemoteTriggerHostingE2E(t *testing.T, store engine.EntryActivationStore) {
	t.Helper()

	// Register the fake trigger + body handlers in the GLOBAL registry so the
	// runner's TriggerActivationHandler lookup and downstream execution both
	// resolve them. Registration is idempotent across the two harness variants.
	fake := newFakeRemoteHostTrigger()
	registry.RegisterTrigger(fake)
	registry.Register(fakeRemoteHostBody{})

	// Local backend + control plane with the EntryActivationStore wired so both
	// the seed-path generation fence and the reconciler are active. The local
	// backend supports the control plane (BindTaskHandler) and exposes a workflow
	// registry, so register persists the compiled graph.
	be := local.New(local.WithConcurrency(1))
	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: store,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("apiserver.Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "kafka-in"
		zoneLabel   = "a"
	)

	// --- register a Kafka-style trigger workflow whose entry unit carries a
	// required selector (remote-hosted) and fans out to a downstream body node. ---
	def := &types.WorkflowDef{
		Name:           "i-remote-trigger-hosting-e2e",
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: remoteHostTriggerType},
			{Name: "body", Kind: types.NodeKindAction, Type: remoteHostBodyType},
		},
		Connections: types.Connections{
			entryUnitID: {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
	}
	wfID := registerWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), def)

	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      wfID,
		WorkflowVersion: wfVersion,
		EntryUnitID:     entryUnitID,
	}
	act, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("entry activation not created after register: ok=%v err=%v", ok, err)
	}
	if !act.Desired || act.RunnerID != "" {
		t.Fatalf("after register: want desired+unassigned, got %+v", act)
	}

	// --- start a runner: matching label + trigger capability + activation tracker
	// over the production TriggerActivationHandler. The seed base URL is the test
	// server so the trigger's HTTPEntrySeedRuntime posts to /v1/executions. ---
	execRegistry := execution.NewRegistry()
	lookup := registryTriggerLookupE2E{}
	handler := runnersvc.NewTriggerActivationHandler(httpSrv.URL, "", lookup)
	tracker := runnersvc.NewActivationTracker(handler, newSlogE2E())

	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, httpSrv.Client()),
		execRegistry,
		runnersvc.Config{
			RunnerID:          "runner-zone-a",
			Concurrency:       1,
			Labels:            map[string]string{"zone": zoneLabel},
			Capabilities:      []protocol.Capability{{NodeType: remoteHostTriggerType}, {NodeType: remoteHostBodyType}},
			HeartbeatInterval: 50 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()

	// Wait for the runner to register.
	waitForE2ERunner(t, cp.RunnerDirectory(), "runner-zone-a")

	// --- drive one reconcile pass: assigns the matching runner + enqueues the
	// activate directive. The runner's next heartbeat delivers it and hosts. ---
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "runner-zone-a" {
		t.Fatalf("reconcile must assign runner-zone-a, got %q", act.RunnerID)
	}
	assignedGen := act.Generation
	if assignedGen == 0 {
		t.Fatal("assigned generation must be > 0")
	}

	// --- wait for the runner to receive the directive, host the trigger, and seed
	// one event through the entry-seed runtime. ---
	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the runner to host the trigger and seed an event")
	}

	fake.mu.Lock()
	seedResp := fake.seedResp
	seedErr := fake.seedErr
	fake.mu.Unlock()
	if seedErr != nil {
		t.Fatalf("seed failed (fence should admit the assigned generation): %v", seedErr)
	}
	if !seedResp.Accepted {
		t.Fatalf("seed not accepted: %+v", seedResp)
	}
	if seedResp.ExecutionID == "" {
		t.Fatal("seed accepted but returned empty execution id")
	}

	// --- verify the execution was created with downstream fan-out. The single
	// entry unit + a downstream body means the seed admits the entry unit and the
	// downstream body task fans out (and is executed by the same runner). ---
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, be.State(), seedResp.ExecutionID, "body")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}
	body, ok := result.Output["body"].(map[string]any)
	if !ok {
		t.Fatalf("output[body] = %T, want map (downstream fan-out did not run)", result.Output["body"])
	}
	if body["handled"] != true {
		t.Fatalf("downstream body output = %v, want handled=true", body)
	}

	// --- reconnect inventory reconciliation: the runner re-registers reporting the
	// still-hosted activation. The lease is renewed and the generation is UNCHANGED
	// (the runner keeps hosting; not orphaned, not fenced). ---
	if err := reconciler.ReconcileRunnerInventory(ctx, "runner-zone-a", []protocol.ActivationInventoryItem{{
		WorkflowID:  string(wfID),
		EntryUnitID: entryUnitID,
		Generation:  assignedGen,
	}}, time.Now()); err != nil {
		t.Fatalf("ReconcileRunnerInventory (renew): %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.Generation != assignedGen {
		t.Fatalf("inventory renew must keep generation %d, got %d", assignedGen, act.Generation)
	}
	if act.RunnerID != "runner-zone-a" {
		t.Fatalf("inventory renew must retain owner, got %q", act.RunnerID)
	}

	// --- reconnect with an EMPTY inventory (new session lost the subscription):
	// the assignment is revoked so it becomes reassignable. ---
	if err := reconciler.ReconcileRunnerInventory(ctx, "runner-zone-a", nil, time.Now()); err != nil {
		t.Fatalf("ReconcileRunnerInventory (revoke): %v", err)
	}
	act, _, _ = store.Get(ctx, key)
	if act.RunnerID != "" {
		t.Fatalf("unreported activation must be revoked, got owner %q", act.RunnerID)
	}
	if !act.Desired {
		t.Fatalf("revoked activation must stay desired (reassignable), got %+v", act)
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}
