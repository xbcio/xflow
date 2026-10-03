//go:build integration

package integration

import (
	"context"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xbcio/xflow/backend/providers/distributed"
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

// grpcAckDeclineTriggerType is the fake trigger node type for
// TestGRPCActivationAckClearsGatedActivationRealRedis. It is registered in the
// global node registry so the runner's TriggerActivationHandler lookup
// resolves it, mirroring remoteHostTriggerType in
// i_remote_trigger_hosting_e2e_test.go.
const grpcAckDeclineTriggerType = "test.grpcack.decline.trigger"

// grpcAckDeclineTrigger fails its first Activate call (simulating a gate
// decline the runner cannot take yet) and succeeds on every subsequent call.
// It records every Activate attempt's generation so the test can assert the
// reconciler redispatched a higher generation after the ack fenced the first.
type grpcAckDeclineTrigger struct {
	mu         sync.Mutex
	attempts   []uint64
	succeeded  chan struct{}
	closedOnce sync.Once
}

func newGRPCAckDeclineTrigger() *grpcAckDeclineTrigger {
	return &grpcAckDeclineTrigger{succeeded: make(chan struct{})}
}

func (*grpcAckDeclineTrigger) Descriptor() types.Descriptor {
	return types.Descriptor{Type: grpcAckDeclineTriggerType, Kind: types.NodeKindTrigger}
}

func (*grpcAckDeclineTrigger) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{}, nil
}

func (h *grpcAckDeclineTrigger) Activate(_ context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	runtime, ok := input.Runtime.(*protocol.HTTPEntrySeedRuntime)
	generation := uint64(0)
	if ok {
		generation = runtime.Generation
	}
	h.mu.Lock()
	n := len(h.attempts)
	h.attempts = append(h.attempts, generation)
	h.mu.Unlock()

	if n == 0 {
		// First attempt: decline, forcing the runner to send a failed
		// ActivationAck over whatever transport it was built with.
		return nil, errGRPCAckDeclineGate
	}
	h.closedOnce.Do(func() { close(h.succeeded) })
	return types.CloseFunc(func(context.Context) error { return nil }), nil
}

type grpcAckDeclineError string

func (e grpcAckDeclineError) Error() string { return string(e) }

const errGRPCAckDeclineGate = grpcAckDeclineError("gate not ready: supply unavailable")

type grpcAckTriggerLookup struct{ trigger *grpcAckDeclineTrigger }

func (l grpcAckTriggerLookup) Trigger(nodeType string) (types.TriggerHandler, bool) {
	if nodeType != grpcAckDeclineTriggerType {
		return nil, false
	}
	return l.trigger, true
}

// TestGRPCActivationAckClearsGatedActivationRealRedis proves the gRPC
// AckActivation RPC closes the same self-heal loop the HTTP transport already
// has (SUPPLY-NODE.md §9(a)): a gRPC-transport runner that declines an
// activate directive sends a failed ActivationAck over gRPC, the reconciler
// fences the activation (Store.Fence clears RunnerID), and the next Reconcile
// pass redispatches it at a higher generation without the runner restarting.
//
// Uses the real Redis-backed EntryActivationStore + RunnerDirectory (the
// production distributed backend), not memstore, so this proves the RPC
// against the same storage layer production deployments use.
func TestGRPCActivationAckClearsGatedActivationRealRedis(t *testing.T) {
	addr := requireRedis(t)

	trigger := newGRPCAckDeclineTrigger()
	registry.RegisterTrigger(trigger)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	flushXflowKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	be, err := distributed.New(addr, nil, distributed.WithConsumer(true), distributed.WithConcurrency(1))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	entryStore := be.NewEntryActivationStore(time.Minute)

	cp, err := control.NewControlPlane(control.Config{
		Backend:              be,
		EntryActivationStore: entryStore,
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

	// Expose the gRPC transport on the same control plane the HTTP server
	// already serves, mirroring TestServerRunnerE2EgRPCStreamRealRedis.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	srv.RegisterGRPC(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	reconciler := cp.EntryActivationReconciler()
	if reconciler == nil {
		t.Fatal("control plane must expose the entry activation reconciler")
	}

	const (
		wfVersion   = "1"
		entryUnitID = "entry-grpc-ack"
		zoneLabel   = "grpc-ack-zone"
		runnerID    = "runner-grpc-ack"
	)

	def := &types.WorkflowDef{
		Name:           "grpc-activation-ack-real-redis",
		Version:        wfVersion,
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": zoneLabel}},
		Nodes: []types.NodeDef{
			{Name: entryUnitID, Kind: types.NodeKindTrigger, Type: grpcAckDeclineTriggerType},
		},
	}
	wfID := registerWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), def)

	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      wfID,
		WorkflowVersion: wfVersion,
		EntryUnitID:     entryUnitID,
	}

	execRegistry := execution.NewRegistry()
	lookup := grpcAckTriggerLookup{trigger: trigger}
	activationHandler := runnersvc.NewTriggerActivationHandler(httpSrv.URL, "", lookup)
	tracker := runnersvc.NewActivationTracker(activationHandler, newSlogE2E())

	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		execRegistry,
		runnersvc.Config{
			RunnerID:          runnerID,
			Concurrency:       1,
			Labels:            map[string]string{"zone": zoneLabel},
			Capabilities:      []protocol.Capability{{NodeType: grpcAckDeclineTriggerType}},
			HeartbeatInterval: 25 * time.Millisecond,
			PollWait:          10 * time.Millisecond,
			ActivationTracker: tracker,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})
	waitForE2ERunner(t, cp.RunnerDirectory(), runnerID)

	// --- Round 1: reconcile assigns the runner; its heartbeat (over gRPC)
	// delivers the directive; Activate declines. The gRPC-transport runner
	// must send a failed ActivationAck via AckActivation. ---
	if err := reconciler.Reconcile(ctx, time.Now()); err != nil {
		t.Fatalf("Reconcile (round 1): %v", err)
	}
	act, ok, err := entryStore.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("entry activation not found after round 1: ok=%v err=%v", ok, err)
	}
	if act.RunnerID != runnerID {
		t.Fatalf("round 1: RunnerID = %q, want %q", act.RunnerID, runnerID)
	}
	firstGeneration := act.Generation

	// Wait for the decline to be observed, then for the fence to clear
	// RunnerID — proof the gRPC AckActivation RPC reached the server and
	// Core.activationAck ran MarkActivationFailed → Store.Fence, exactly as
	// the HTTP transport does.
	deadline := time.Now().Add(10 * time.Second)
	for {
		act, ok, err = entryStore.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get while waiting for fence: %v", err)
		}
		if ok && act.RunnerID == "" && act.Generation == firstGeneration {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for gRPC ActivationAck to fence the activation; last = %+v", act)
		}
		time.Sleep(20 * time.Millisecond)
	}

	trigger.mu.Lock()
	attemptsAfterFence := len(trigger.attempts)
	trigger.mu.Unlock()
	if attemptsAfterFence != 1 {
		t.Fatalf("Activate attempts after fence = %d, want exactly 1 (the declined attempt)", attemptsAfterFence)
	}

	// --- Round 2: reconcile redispatches the now-unassigned activation at a
	// higher generation. Passing a synthetic future "now" (mirroring
	// TestSupplyGateRetriesWithoutRestart) advances past the reconciler's
	// retry backoff (DefaultActivationRetryBackoffMin = 10s) without the test
	// sleeping in real wall-clock time. No runner restart was involved — the
	// self-heal loop closes purely through the gRPC ack + reconcile cycle.
	futureNow := time.Now().Add(15 * time.Second)
	if err := reconciler.Reconcile(ctx, futureNow); err != nil {
		t.Fatalf("Reconcile (round 2, past backoff): %v", err)
	}

	select {
	case <-trigger.succeeded:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the redispatched activation to succeed")
	}

	finalAct, ok, err := entryStore.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("entry activation not found after redispatch: ok=%v err=%v", ok, err)
	}
	if finalAct.RunnerID != runnerID {
		t.Fatalf("after redispatch: RunnerID = %q, want %q", finalAct.RunnerID, runnerID)
	}
	if finalAct.Generation <= firstGeneration {
		t.Fatalf("redispatch generation = %d, want > first generation %d", finalAct.Generation, firstGeneration)
	}
}
