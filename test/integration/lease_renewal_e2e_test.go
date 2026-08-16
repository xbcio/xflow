//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// slowLeaseHandler blocks until the test releases it, standing in for the
// handler this whole feature exists for: one whose own timeout legitimately
// exceeds the engine's lease TTL. It records how many times it was entered so
// a redelivered task — the exact failure renewal prevents — is visible as a
// second invocation rather than inferred.
type slowLeaseHandler struct {
	nodeType string
	started  chan struct{}
	release  chan struct{}

	startOnce   sync.Once
	invocations atomic.Int32
}

func newSlowLeaseHandler(nodeType string) *slowLeaseHandler {
	return &slowLeaseHandler{
		nodeType: nodeType,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (h *slowLeaseHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType}
}

func (h *slowLeaseHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.invocations.Add(1)
	h.startOnce.Do(func() { close(h.started) })
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	out := map[string]any{"handled": true}
	for k, v := range input.Data {
		out[k] = v
	}
	return &types.Output{Data: out}, nil
}

// leaseIsReclaimable reports whether the sweeper would currently reclaim this
// execution's lease. It asks the production ListExpiredLeases rather than
// reading lease_deadline_ms directly, so the assertion is about the behaviour
// that actually strands work, not about a field that happens to back it.
func leaseIsReclaimable(ctx context.Context, t *testing.T, h *serverRunnerHarness, execID types.ExecutionID) bool {
	t.Helper()
	expired, err := h.state.ListExpiredLeases(ctx, time.Now())
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	for _, e := range expired {
		if e.ExecutionID == execID {
			return true
		}
	}
	return false
}

// A runner whose handler outruns the lease TTL must keep its lease. Before
// renewal was wired, DefaultLeaseTTL was a hard, undocumented ceiling on node
// runtime: the sweeper reclaimed the lease mid-flight and redelivered the task
// while the first runner was still computing it.
//
// The reverse control for this test is the wiring itself — removing the
// renewal goroutine from Runner.executeAndReport makes it fail at the
// "still reclaimable" assertion, because the 2s TTL elapses long before the
// handler is released.
func TestLeaseRenewalE2E_NodeLeaseSurvivesATTL(t *testing.T) {
	addr := requireRedis(t)
	// TTL well under the handler's runtime so the lease would certainly expire
	// without renewal, and long enough that the default renewal interval
	// (min(TTL/3, 10s)) fires several times inside one TTL.
	h := newServerRunnerHarnessWithLeaseTTL(t, addr, 1, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := newSlowLeaseHandler("test.renew.slow")
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.renew.slow", handler)

	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-lease-renew-node",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.renew.slow"}},
			PollWait:     10 * time.Millisecond,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-lease-renew-node")

	wf := &types.WorkflowDef{
		Name: "lease-renew-node",
		Nodes: []types.NodeDef{
			{Name: "slow", Type: "test.renew.slow", Kind: types.NodeKindAction},
		},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "renew"})

	select {
	case <-handler.started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	// Hold the handler well past two full TTLs. Renewal must keep the lease
	// out of the sweeper's expired set the whole time.
	watchCtx, watchCancel := context.WithTimeout(ctx, 6*time.Second)
	defer watchCancel()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if leaseIsReclaimable(watchCtx, t, h, execID) {
			t.Fatalf("lease for %s became reclaimable while its handler was still running — "+
				"the sweeper will redeliver the task to a second runner", execID)
		}
		select {
		case <-watchCtx.Done():
			t.Fatalf("watch context ended early: %v", watchCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	close(handler.release)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "slow")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}
	if got := handler.invocations.Load(); got != 1 {
		t.Fatalf("handler invocations = %d, want 1 — a second invocation means the lease "+
			"was reclaimed and the task redelivered despite renewal", got)
	}
}

// The same guarantee on the group branch of executeAndReport. A group lease is
// the longest-running unit a runner holds — a whole subgraph under one lease —
// and it renews through RenewGroupLease rather than RenewTaskLease, so it needs
// its own end-to-end evidence.
func TestLeaseRenewalE2E_GroupLeaseSurvivesATTL(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarnessWithLeaseTTL(t, addr, 2, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := newSlowLeaseHandler("test.renew.group.slow")
	_, _ = startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "runner-lease-renew-group",
		handlers: map[string]types.ActionHandler{
			"test.renew.group.slow": slow,
			"test.group.member":     &groupMemberHandler{},
		},
	})

	wf := &types.WorkflowDef{
		Name: "lease-renew-group",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.renew.group.slow", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "renew-group"})

	select {
	case <-slow.started:
	case <-time.After(15 * time.Second):
		t.Fatal("group member never started")
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 6*time.Second)
	defer watchCancel()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if leaseIsReclaimable(watchCtx, t, h, execID) {
			t.Fatalf("group lease for %s became reclaimable while a member was still running — "+
				"the sweeper will redeliver the whole subgraph", execID)
		}
		select {
		case <-watchCtx.Done():
			t.Fatalf("watch context ended early: %v", watchCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	close(slow.release)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "out")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", result.Status)
	}
	if got := slow.invocations.Load(); got != 1 {
		t.Fatalf("group member invocations = %d, want 1 — a second invocation means the group "+
			"lease was reclaimed and the subgraph redelivered despite renewal", got)
	}
}

// The positive control for both tests above: a runner whose transport cannot
// renew (the gRPC client is the production instance of this) must still see its
// lease expire. Without this, a bug that made every lease immortal — a sweeper
// that stopped listing, a TTL that stopped being stamped — would leave the two
// renewal tests green and prove nothing.
func TestLeaseRenewalE2E_WithoutRenewalTheLeaseExpires(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarnessWithLeaseTTL(t, addr, 1, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := newSlowLeaseHandler("test.norenew.slow")
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.norenew.slow", handler)

	runner := runnersvc.New(
		&renewlessClient{inner: protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client())},
		registry,
		runnersvc.Config{
			RunnerID:     "runner-lease-norenew-node",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.norenew.slow"}},
			PollWait:     10 * time.Millisecond,
		},
	)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-lease-norenew-node")

	wf := &types.WorkflowDef{
		Name: "lease-norenew-node",
		Nodes: []types.NodeDef{
			{Name: "slow", Type: "test.norenew.slow", Kind: types.NodeKindAction},
		},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "norenew"})

	select {
	case <-handler.started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitCancel()
	if err := waitForCondition(waitCtx, func() bool {
		return leaseIsReclaimable(waitCtx, t, h, execID)
	}, 100*time.Millisecond); err != nil {
		t.Fatalf("lease for %s never became reclaimable without renewal (%v) — the TTL is not "+
			"actually enforced, so the two renewal tests prove nothing", execID, err)
	}

	close(handler.release)
	cancel()
	select {
	case <-runErr:
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
}

// renewlessClient is a protocol client with the renew capability removed. It
// reproduces exactly what a gRPC-transport runner is: the four required
// ProtocolClient methods and no RenewLease, so Runner.executeAndReport's type
// assertion fails and no renewal goroutine starts.
//
// The inner client is a named field, NOT embedded: embedding would promote
// RenewLease and this "renewless" client would quietly renew, turning the
// control into a second copy of the tests it exists to validate.
type renewlessClient struct {
	inner *protocol.Client
}

func (c *renewlessClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return c.inner.Register(ctx, req)
}

func (c *renewlessClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return c.inner.Heartbeat(ctx, req)
}

func (c *renewlessClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return c.inner.Poll(ctx, req)
}

func (c *renewlessClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return c.inner.ReportResult(ctx, req)
}

var _ runnersvc.ProtocolClient = (*renewlessClient)(nil)
