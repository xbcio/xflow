//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

const (
	groupLocalTriggerType = "test.grouplocal.trigger"
	groupLocalMemberType  = "test.grouplocal.member"
)

// groupLocalMemberHandler is a REAL member node handler: it stamps
// seen_by_member=true on whatever data flows through it. Its execution (not
// its mere registration) is what this test proves — before this feature, a
// trigger-group batch never reached any member node at all, so this field
// would never appear anywhere downstream.
type groupLocalMemberHandler struct{}

func (groupLocalMemberHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: groupLocalMemberType}
}
func (groupLocalMemberHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	out := map[string]any{}
	for k, v := range input.Data {
		out[k] = v
	}
	out["seen_by_member"] = true
	return &types.Output{Data: out}, nil
}

// groupLocalFakeTrigger simulates a Kafka-style trigger's per-batch flush: on
// Activate it drives ONE batch through input.Runtime.(types.GroupExecRuntime)
// — the SAME capability node/internal/trigger/kafka.go's real flush() uses —
// so the group's REAL member node executes, and forwards the REAL resulting
// exits (never hand-constructed) to SeedExecutionFromEntry. Only the Kafka
// broker/consumer subscription is faked; the group execution and admission
// are genuine production code paths.
//
// The embedded types.TriggerHandler is filled by newGroupLocalFakeTrigger with
// a node.DefineTrigger definition — whose Execute *is* the production
// ExecuteTriggerEntry (node/internal/node.go:186), reached through the public
// node package rather than reimplemented.
//
// Test-fixture note (deviation from the brief, orthogonal to this task's own
// logic): the brief's original comment claimed Execute would be "promoted
// from the embedded types.TriggerHandler". That does not hold in Go — method
// promotion through an embedded *interface* only exposes the methods declared
// on that interface's method set (here: Descriptor, Activate), never the
// extra methods the concrete value stored in it happens to have. Since
// registry.RegisterTrigger registers the ActionHandler side via a type
// assertion (`h.(types.ActionHandler)`), a groupLocalFakeTrigger without its
// own Execute never satisfies that assertion, so no handler is ever
// registered for groupLocalTriggerType and package validation fails with
// "handler not available" before any group logic runs. The explicit Execute
// method below (forwarding to the embedded definition's own Execute) fixes
// the fixture without touching any production code or the assertions this
// test makes.
//
// This matters for spec 2026-08-07 §3.4. When GroupRuntime's inner engine
// dispatches the package's OWN entry node once per batch, production behavior
// is: copy input.Data, inject an empty types.TriggerEvent under the "trigger"
// key when absent, set Port "main". §3.4's claim that "the package's entry
// needs no modification" rests on exactly that method. A hand-written
// pass-through fake would diverge on the injected key and on Port, leaving the
// claim untested — so the real one is used. (node/internal is not importable
// from test/integration; node.DefineTrigger is the sanctioned route to the
// same code.)
type groupLocalFakeTrigger struct {
	types.TriggerHandler

	mu       sync.Mutex
	done     chan struct{}
	doneOnce sync.Once
	execErr  error
	execRes  types.GroupExecResult
	seedResp types.EntrySeedResponse
	seedErr  error
}

func newGroupLocalFakeTrigger() *groupLocalFakeTrigger {
	h := &groupLocalFakeTrigger{done: make(chan struct{})}
	// The definition supplies Descriptor (Type, Kind=trigger, Outputs=[main])
	// and the production Execute; activate below supplies the subscribe path.
	h.TriggerHandler = node.DefineTrigger(groupLocalTriggerType, h.activate)
	return h
}

// Execute forwards to the embedded node.DefineTrigger definition's own
// Execute (the production ExecuteTriggerEntry). See the type-level doc
// comment above: this cannot be left to interface-embedding promotion, since
// types.TriggerHandler's method set does not include Execute.
func (h *groupLocalFakeTrigger) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	return h.TriggerHandler.(types.ActionHandler).Execute(ctx, input)
}

// activate is the TriggerActivateFunc handed to node.DefineTrigger above; the
// definition promotes it as this handler's Activate method.
func (h *groupLocalFakeTrigger) activate(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	gr, ok := input.Runtime.(types.GroupExecRuntime)
	if !ok {
		return nil, errors.New("runtime does not support group execution")
	}
	sr, ok := input.Runtime.(types.EntrySeedRuntime)
	if !ok {
		return nil, errors.New("runtime does not support entry-seed admission")
	}

	execRes, err := gr.ExecuteGroup(ctx, map[string]any{"batch": "kafka-batch-1"})
	h.mu.Lock()
	h.execErr, h.execRes = err, execRes
	h.mu.Unlock()

	if err == nil && execRes.Outcome == "success" {
		entryUnitID, _ := input.Params["entry_unit_id"].(string)
		wfVersion, _ := input.Params["workflow_version"].(string)
		resp, serr := sr.SeedExecutionFromEntry(ctx, types.EntrySeedRequest{
			AdmissionKey:    "grouplocal/" + entryUnitID + "/batch-1",
			WorkflowID:      input.WorkflowID,
			WorkflowVersion: wfVersion,
			EntryUnitID:     entryUnitID,
			Outcome:         "success",
			Exits:           execRes.Exits, // REAL exits from ExecuteGroup — never hand-built.
		})
		h.mu.Lock()
		h.seedResp, h.seedErr = resp, serr
		h.mu.Unlock()
	}

	h.doneOnce.Do(func() { close(h.done) })
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

type groupLocalTriggerLookup struct{}

func (groupLocalTriggerLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	return registry.LookupTrigger(nodeType)
}

// TestTriggerGroupLocalExecution_RealMemberNodeRuns is the spec §3.7-mandated
// e2e: it proves a trigger-group batch executes the group's REAL member node
// on the runner and that the member's REAL output — not a hand-constructed
// stand-in — is what reaches the downstream node outside the group.
func TestTriggerGroupLocalExecution_RealMemberNodeRuns(t *testing.T) {
	fake := newGroupLocalFakeTrigger()
	registry.RegisterTrigger(fake)
	registry.Register(groupLocalMemberHandler{})

	be := local.New(local.WithConcurrency(1))
	store := control.NewMemoryEntryActivationStore()
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

	const wfVersion = "1"
	def := &types.WorkflowDef{
		Name:    "j-trigger-group-local-exec",
		Version: wfVersion,
		Nodes: []types.NodeDef{
			{Name: "kafka-in", Kind: types.NodeKindTrigger, Type: groupLocalTriggerType},
			{Name: "member", Kind: types.NodeKindAction, Type: groupLocalMemberType},
			{Name: "downstream", Kind: types.NodeKindAction, Type: groupLocalMemberType},
		},
		Connections: types.Connections{
			"kafka-in": {"main": {Targets: []types.Connection{{Node: "member", Input: "main"}}}},
			"member":   {"main": {Targets: []types.Connection{{Node: "downstream", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{
			Name:    "kafka-group",
			Members: []string{"kafka-in", "member"},
			RunnerSelector: &types.RunnerSelector{
				Mode:        types.RunnerSelectorModeRequired,
				MatchLabels: map[string]string{"zone": "a"},
			},
		}},
	}
	wfID := registerWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), def)

	// --- start a runner: matching label + member/trigger capabilities +
	// group.exec.v1, ActivationTracker over TriggerActivationHandler with
	// WithGroupRuntime — exactly the production wiring cmd/runner assembles. ---
	execRegistry := execution.NewRegistry()
	cache := runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: 16})
	groupRT := runnersvc.NewGroupRuntime(execRegistry, cache, runnersvc.WithSuspendDisabled())

	handler := runnersvc.NewTriggerActivationHandler(httpSrv.URL, "", groupLocalTriggerLookup{}, runnersvc.WithGroupRuntime(groupRT))
	tracker := runnersvc.NewActivationTracker(handler, newSlogE2E())

	runner := runnersvc.New(
		protocol.NewClient(httpSrv.URL, httpSrv.Client()),
		execRegistry,
		runnersvc.Config{
			RunnerID:    "runner-grouplocal-a",
			Concurrency: 1,
			Labels:      map[string]string{"zone": "a"},
			Capabilities: []protocol.Capability{
				{NodeType: groupLocalTriggerType},
				{NodeType: groupLocalMemberType},
				{NodeType: "xflow.group", Features: []string{"group.exec.v1"}},
			},
			HeartbeatInterval: 50 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
			GroupRuntime:      groupRT,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()

	waitForE2ERunner(t, cp.RunnerDirectory(), "runner-grouplocal-a")

	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the runner to host the group and run one batch")
	}

	fake.mu.Lock()
	execErr, execRes := fake.execErr, fake.execRes
	seedResp, seedErr := fake.seedResp, fake.seedErr
	fake.mu.Unlock()

	if execErr != nil {
		t.Fatalf("ExecuteGroup failed: %v", execErr)
	}
	if execRes.Outcome != "success" {
		t.Fatalf("group outcome = %s, want success; error = %s", execRes.Outcome, execRes.Error)
	}
	// The defining assertion: the exit came from the REAL "member" node
	// having actually executed, not from a hand-built map.
	if len(execRes.Exits) != 1 {
		t.Fatalf("exits = %+v, want exactly 1 (from the real member node)", execRes.Exits)
	}
	if execRes.Exits[0].NodeName != "member" {
		t.Fatalf("exit node = %q, want %q", execRes.Exits[0].NodeName, "member")
	}
	if execRes.Exits[0].Data["seen_by_member"] != true {
		t.Fatalf("exit data = %v, missing seen_by_member=true — the member node's own Execute never ran", execRes.Exits[0].Data)
	}
	if execRes.Exits[0].Data["batch"] != "kafka-batch-1" {
		t.Fatalf("exit data = %v, want batch passed through from the seeded input", execRes.Exits[0].Data)
	}
	// The package's OWN entry node — the trigger member — really was dispatched
	// by the inner engine on the way to "member" (spec §3.4). Its production
	// Execute (ExecuteTriggerEntry) injects an empty TriggerEvent under
	// "trigger" when the seeded input lacks one; that key's presence
	// downstream is the evidence it ran, and the evidence that §3.4's "the
	// package's entry needs no modification" holds.
	if _, ok := execRes.Exits[0].Data["trigger"]; !ok {
		t.Fatalf("exit data = %v, missing the \"trigger\" key — the package's "+
			"entry node was bypassed rather than dispatched", execRes.Exits[0].Data)
	}

	if seedErr != nil {
		t.Fatalf("SeedExecutionFromEntry failed: %v", seedErr)
	}
	if !seedResp.Accepted {
		t.Fatalf("seed not accepted: %+v", seedResp)
	}
	if seedResp.ExecutionID == "" {
		t.Fatal("seed accepted but returned empty execution id")
	}

	// --- verify the execution completed with downstream fan-out: the group's
	// real exit propagated to "downstream", outside the group. ---
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, be.State(), seedResp.ExecutionID, "downstream")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}
	downOut, ok := result.Output["downstream"].(map[string]any)
	if !ok {
		t.Fatalf("output[downstream] = %T, want map (downstream fan-out did not run)", result.Output["downstream"])
	}
	if downOut["seen_by_member"] != true {
		t.Fatalf("downstream output = %v, want seen_by_member=true carried through from the real member execution", downOut)
	}

	_ = wfID
	cancel()
	select {
	case <-runErr:
	case <-time.After(3 * time.Second):
	}
}
