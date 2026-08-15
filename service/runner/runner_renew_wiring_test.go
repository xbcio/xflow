package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// renewCapturingClient is a fakeProtocolClient that also implements the
// optional renew capability, and blocks the handler until the test releases it
// so renewal has a window to happen in.
type renewCapturingClient struct {
	fakeProtocolClient

	mu       sync.Mutex
	requests []protocol.RenewLeaseRequest
	renewed  bool
	refuse   bool
}

func (c *renewCapturingClient) RenewLease(_ context.Context, req protocol.RenewLeaseRequest) (protocol.RenewLeaseResponse, error) {	c.mu.Lock()
	c.requests = append(c.requests, req)
	refuse := c.refuse
	c.renewed = true
	c.mu.Unlock()
	if refuse {
		return protocol.RenewLeaseResponse{Renewed: false, Error: "lease not found"}, nil
	}
	return protocol.RenewLeaseResponse{Renewed: true, Deadline: time.Now().UTC().Add(time.Minute)}, nil
}

// Register hands back a session so the renewal requests carry the identity the
// endpoint fences on. fakeProtocolClient's own Register returns none, which
// would make the session assertion vacuous.
func (c *renewCapturingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	c.registered = true
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: "session-1"}, nil
}

func (c *renewCapturingClient) seen() []protocol.RenewLeaseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.RenewLeaseRequest(nil), c.requests...)
}

// blockingRenewHandler blocks until released, standing in for the handler this whole
// feature exists for: one whose own timeout legitimately exceeds the lease TTL.
type blockingRenewHandler struct {
	started  chan struct{}
	release  chan struct{}
	startOne sync.Once

	mu     sync.Mutex
	ctxErr error
}

func (*blockingRenewHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.slow"}
}

func (h *blockingRenewHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.startOne.Do(func() { close(h.started) })
	select {
	case <-h.release:
	case <-ctx.Done():
		h.mu.Lock()
		h.ctxErr = ctx.Err()
		h.mu.Unlock()
	}
	return &types.Output{Data: input.Data}, nil
}

func (h *blockingRenewHandler) cancelledWith() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ctxErr
}

func newRenewTestRunner(t *testing.T, client ProtocolClient, handler types.ActionHandler) *Runner {
	t.Helper()
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.slow", handler)
	return New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "test.slow"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		Renewal:           RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 3},
	})
}

func renewTestLease() *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-1"),
		LeaseToken: engine.LeaseToken("token-1"),
		Task:       engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		Input:      &types.Input{Data: map[string]any{"x": 1}},
		NodeType:   "test.slow",
		TTL:        30 * time.Second,
	}
}

// Without this, DefaultLeaseTTL is a hard ceiling on how long any node may run:
// the engine stamps every lease with it and nothing clamps a node's own timeout
// (xflow.http's options.timeout, xflow.script's params.timeout) against it. The
// sweeper then reclaims and redelivers a task whose runner is healthily working,
// and two runners execute the same node.
func TestRunnerRenewsTheLeaseWhileAHandlerRuns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	client := &renewCapturingClient{}
	client.lease = renewTestLease()
	client.cancel = cancel

	r := newRenewTestRunner(t, client, handler)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	deadline := time.After(2 * time.Second)
	for {
		client.mu.Lock()
		got := client.renewed
		client.mu.Unlock()
		if got {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no renewal reached the server while a handler was running — the lease " +
				"expires under the sweeper and the task is redelivered to a second runner")
		case <-time.After(2 * time.Millisecond):
		}
	}

	close(handler.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}

	reqs := client.seen()
	if len(reqs) == 0 {
		t.Fatal("no renew requests recorded")
	}
	first := reqs[0]
	if first.LeaseID != "lease-1" || first.LeaseToken != "token-1" {
		t.Errorf("renew request = %+v, want the executing lease's identity", first)
	}
	if first.RunnerID != "runner-1" || first.SessionID == "" {
		t.Errorf("renew request = %+v, want runner and session identity — the endpoint "+
			"fences on both", first)
	}
	if first.Extend <= 0 {
		t.Errorf("Extend = %d, want a positive extension", first.Extend)
	}
}

// Once the handler returns, renewal must stop. A loop that outlived its task
// would keep a completed node's lease alive, blocking the sweeper from
// reclaiming it if the report never lands.
func TestRunnerStopsRenewingWhenTheHandlerReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	client := &renewCapturingClient{}
	client.lease = renewTestLease()
	client.cancel = cancel

	r := newRenewTestRunner(t, client, handler)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	<-handler.started
	close(handler.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}

	settled := len(client.seen())
	time.Sleep(50 * time.Millisecond)
	if got := len(client.seen()); got != settled {
		t.Fatalf("renew requests grew from %d to %d after the task finished — the renewal "+
			"loop outlived its lease", settled, got)
	}
}

// A refusal means the server has handed this node to another attempt. The
// handler must be cancelled rather than left to produce a second result for
// work someone else now owns.
func TestRunnerCancelsTheHandlerWhenRenewalIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	client := &renewCapturingClient{refuse: true}
	client.lease = renewTestLease()
	client.cancel = cancel

	r := newRenewTestRunner(t, client, handler)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// The handler is released only by a cancelled context here: nothing closes
	// its release channel, so if renewal refusal does not cancel it, this hangs.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a refused renewal did not cancel the running handler — the runner keeps " +
			"executing a node another attempt already owns")
	}
	if handler.cancelledWith() == nil {
		t.Error("handler's context was not cancelled on refusal")
	}
}

// A runner whose transport lacks the renew RPC (gRPC) must behave exactly as
// before: execute and report, no renewal, no failure.
func TestRunnerWithoutRenewCapabilityStillExecutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	close(handler.release)
	client := &fakeProtocolClient{lease: renewTestLease(), cancel: cancel}

	r := newRenewTestRunner(t, client, handler)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if client.reported.Lease == nil {
		t.Fatal("a runner without the renew capability did not report its result")
	}
}

// A group lease is the longest-running thing a runner executes — a whole
// subgraph of members under one lease — and it goes down a different branch of
// executeAndReport than a plain node. Renewal has to cover that branch too, or
// the co-located group that this feature most benefits keeps getting reclaimed
// mid-execution.
func TestRunnerRenewsAGroupLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.slow", handler)

	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "slow-group",
		EntryNode: "a",
		Def: &types.WorkflowDef{
			Name: "slow-group",
			Nodes: []types.NodeDef{
				{Name: "a", Type: "test.slow", Version: 1},
				{Name: "__collector_a_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"a": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_a_main"}}}},
			},
		},
		Exits:        []graph.SubgraphPackageExit{{CollectorNode: "__collector_a_main", SrcNode: "a", Port: "main"}},
		Requirements: []graph.Requirement{{NodeType: "test.slow", NodeVersion: 1}},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("ComputePackageHash: %v", err)
	}

	lease := renewTestLease()
	lease.NodeType = engine.GroupNodeType
	lease.GroupPayload = &engine.GroupLeasePayload{
		ProtocolVersion: 1,
		GroupExecID:     "gexec-renew",
		PackageHash:     hash,
		Package:         pkg,
		Input:           &types.Input{Data: map[string]any{"x": 1}},
		Deadline:        time.Now().Add(time.Minute),
	}

	client := &renewCapturingClient{}
	client.lease = lease
	client.cancel = cancel

	r := New(client, reg, Config{
		RunnerID:          "runner-1",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: engine.GroupNodeType}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		GroupRuntime:      NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}), WithSuspendDisabled()),
		Renewal:           RenewalConfig{Interval: 5 * time.Millisecond, MaxRetries: 3},
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("group member never started")
	}

	deadline := time.After(2 * time.Second)
	for len(client.seen()) == 0 {
		select {
		case <-deadline:
			t.Fatal("no renewal reached the server while a group was executing — the group's " +
				"unit lease expires and the whole subgraph is redelivered")
		case <-time.After(2 * time.Millisecond):
		}
	}

	close(handler.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
	if got := client.seen()[0]; got.LeaseID != "lease-1" || got.LeaseToken != "token-1" {
		t.Errorf("renew request = %+v, want the group lease's identity", got)
	}
}
