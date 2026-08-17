//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// This file is the one place the whole node-timeout chain is proven end to end.
// Each of the four execution paths that carries a node Timeout is exercised at
// the integration level: a plain action node, a group member (whose Timeout
// crosses a field-by-field projection in subgraph_package.go that drops anything
// not listed), a map body member (the same hazard in node_body_package.go), and
// the server-side renewal backstop (the only path that cannot be unit-tested
// against the real lease directory). A fifth test pins the no-retry contract.
//
// The runner-side deadline (Task 4) fires first on every path where a runner is
// executing, so the backstop test MUST NOT let a runner execute -- otherwise the
// terminal it observes could be the runner's, not the server's, and the test
// would stay green with Task 6 deleted. See TestServerBackstopTerminatesUncooperativeRunner.

// blockingTimeoutHandler is an action handler that blocks until released and
// ignores ctx.Done until the deadline-enforced goroutine has already abandoned
// it. It stands in for the exact handler this feature exists for: one whose own
// runtime legitimately exceeds the configured bound and which must be
// terminated by the deadline, not by its own cooperation.
//
// It MUST be released (close(h.release)) in cleanup: once the deadline fires the
// runner abandons the goroutine and starts a watcher that only decrements the
// abandoned gauge when the handler finally returns. A handler left blocked
// forever leaks the goroutine AND leaves the abandoned gauge pinned non-zero
// for the rest of the binary, polluting later tests.
type blockingTimeoutHandler struct {
	nodeType string
	started  chan struct{}
	release  chan struct{}

	startOnce   sync.Once
	invocations atomic.Int32
}

func newBlockingTimeoutHandler(nodeType string) *blockingTimeoutHandler {
	return &blockingTimeoutHandler{
		nodeType: nodeType,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (h *blockingTimeoutHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType}
}

func (h *blockingTimeoutHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	h.invocations.Add(1)
	h.startOnce.Do(func() { close(h.started) })
	// Block until released. ctx.Done() is deliberately NOT selected here: this
	// is the uncooperative handler the deadline machinery exists to bound. A
	// cooperative handler that returned ctx.Err() would still terminate via
	// reclassifyTimeout, which proves the same thing less aggressively; this
	// shape exercises the abandon branch (the watcher + abandoned gauge) too.
	<-h.release
	return &types.Output{Data: map[string]any{"handled": true}}, nil
}

// metricsHarness is a serverRunnerHarness variant whose ControlPlane has its
// metrics observer wired (control.Config.Metrics), so the server-side backstop
// actually emits xflow_node_timeouts_total{source="server"}. The basic harness
// leaves Core.timeoutObserver nil by design (it passes no Metrics), so without
// this variant the backstop's metric emission -- the binding discriminator --
// would be invisible and a test that asserts it would be a false red.
//
// m is the shared registry: the ControlPlane writes the server-side timeout
// counter to it, and the test scrapes it via m.Handler() exactly as /metrics
// would. It is NOT exposed over HTTP here -- the assertion is about whether the
// observer fires, not about the /metrics route, which has its own coverage.
type metricsHarness struct {
	*serverRunnerHarness
	m *metrics.Metrics
}

func newServerRunnerHarnessWithMetrics(t *testing.T, addr string, concurrency int) *metricsHarness {
	t.Helper()
	b, err := distributed.New(addr, nil, distributed.WithConcurrency(concurrency), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	flushXflowKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	m := metrics.New()
	buf := engine.NewRuntimeEvidenceBuffer(64)
	cp, err := control.NewControlPlane(control.Config{
		Backend:               b,
		Metrics:               m,
		RuntimeEvidenceBuffer: buf,
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
	h := &serverRunnerHarness{
		srv:      srv,
		httpSrv:  httpSrv,
		state:    b.State(),
		runners:  cp.RunnerDirectory(),
		cp:       cp,
		cancel:   cancel,
		evidence: buf,
	}
	t.Cleanup(func() { h.stop() })
	return &metricsHarness{serverRunnerHarness: h, m: m}
}

// leaseCapturingClient is a protocol client that swallows the lease a runner
// polls, so the runner claims the lease (it is finalized in the server's
// directory during dispatch) but never executes it. This reproduces the one
// scenario the server-side backstop exists for: a runner that holds a lease past
// its ExecutionDeadline without reporting a terminal. The runner's own
// deadline-enforced commit never fires because the runner never calls Execute.
//
// It is a named-field wrapper (NOT embedded), mirroring renewlessClient in
// lease_renewal_e2e_test.go: embedding would promote RenewLease, and although no
// renewal loop starts without execution, keeping the surface identical to the
// production "cannot renew" client makes the scenario's transport shape
// unambiguous. It does NOT implement leaseRenewClient, so even if execution
// were reached the renewal loop would not start.
type leaseCapturingClient struct {
	inner *protocol.Client

	mu        sync.Mutex
	runnerID  string
	sessionID string
	captured  *engine.TaskLease
}

func (c *leaseCapturingClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	resp, err := c.inner.Register(ctx, req)
	if err == nil {
		c.mu.Lock()
		c.runnerID = req.RunnerID
		c.sessionID = resp.SessionID
		c.mu.Unlock()
	}
	return resp, err
}

func (c *leaseCapturingClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return c.inner.Heartbeat(ctx, req)
}

func (c *leaseCapturingClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	resp, err := c.inner.Poll(ctx, req)
	if err != nil {
		return resp, err
	}
	if resp.Lease != nil {
		// Swallow the lease: the server has already finalized it in the
		// directory during this poll, so it is now an active, unreported lease
		// whose ExecutionDeadline will pass. Returning an empty response makes
		// the runner's pollLoop treat this as "no task" and keep polling without
		// ever handing the lease to a worker.
		c.mu.Lock()
		c.captured = resp.Lease
		c.mu.Unlock()
		return protocol.PollTaskResponse{}, nil
	}
	return resp, nil
}

func (c *leaseCapturingClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return c.inner.ReportResult(ctx, req)
}

func (c *leaseCapturingClient) capturedLease() (*engine.TaskLease, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.captured, c.runnerID, c.sessionID
}

var _ runnersvc.ProtocolClient = (*leaseCapturingClient)(nil)

// scrapeMetrics returns the /metrics text body for the harness's registry, the
// same way the unit tests in service/control/renew_deadline_test.go do. It is
// the only assertion surface for "the server-side observer fired": the basic
// harness wires no observer, so a test that only checks the node's terminal
// state cannot tell the server's commit from a runner's.
func scrapeMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

// TestNodeTimeoutTerminatesStuckHandler proves the runner-side deadline (Task 4)
// bounds an uncooperative handler: configured with a 500ms Timeout, the node
// reaches a terminal state well inside the engine default (30m). Without the
// deadline, this handler blocks forever and the execution never terminates, so
// the "reached terminal within 5s" assertion is a real red against a Task-4
// deletion, not a tautology.
func TestNodeTimeoutTerminatesStuckHandler(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := newBlockingTimeoutHandler("test.timeout.slow")
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.timeout.slow", handler)
	defer close(handler.release)

	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-node-timeout",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.timeout.slow"}},
			PollWait:     10 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-node-timeout")

	wf := &types.WorkflowDef{
		Name: "node-timeout-stuck",
		Nodes: []types.NodeDef{
			{Name: "slow", Type: "test.timeout.slow", Kind: types.NodeKindAction, Timeout: 500 * time.Millisecond},
		},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "stuck"})

	select {
	case <-handler.started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started within 10s -- runner did not pick up the task")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "slow")
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed (a node whose handler blocks past its 500ms "+
			"Timeout must be terminated by the runner-side deadline, not left running)", result.Status)
	}
	if got := handler.invocations.Load(); got != 1 {
		t.Fatalf("handler invocations = %d, want 1", got)
	}
}

// TestGroupMemberTimeoutHonoursOwnBudget proves the group-package projection
// (subgraph_package.go:165, one of the two field-by-field copy points) actually
// delivers a member's Timeout to the handler that runs it. A unit test of the
// projection compiles; it does not prove the value survived dispatch. Here the
// member's own 500ms bound must terminate it; without the projection line the
// member downgrades to the 30m engine default and the execution hangs past the
// test's wait, so this assertion is a real red against a Task-2 deletion.
func TestGroupMemberTimeoutHonoursOwnBudget(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := newBlockingTimeoutHandler("test.group.timeout.slow")
	defer close(slow.release)
	_, _ = startGroupRunner(t, ctx, h, groupRunnerOpts{
		runnerID: "runner-group-member-timeout",
		handlers: map[string]types.ActionHandler{
			"test.group.timeout.slow": slow,
			"test.group.member":        &groupMemberHandler{},
		},
	})

	wf := &types.WorkflowDef{
		Name: "group-member-timeout",
		Nodes: []types.NodeDef{
			// g.source carries the 500ms bound being tested.
			{Name: "g.source", Type: "test.group.timeout.slow", Kind: types.NodeKindAction, Timeout: 500 * time.Millisecond},
			{Name: "g.sink", Type: "test.group.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}}},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "group-timeout"})

	select {
	case <-slow.started:
	case <-time.After(10 * time.Second):
		t.Fatal("group member never started within 10s")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "out")
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed (a group member whose 500ms Timeout was projected "+
			"must terminate it; if it stayed running the projection dropped the field)", result.Status)
	}
}

// TestMapBodyNodeTimeout proves the map-body projection
// (node_body_package.go:135, the second field-by-field copy point) carries a
// body member's Timeout through to the inner engine that runs it. The body runs
// on the runner's SubgraphRuntime against a local backend whose embedded
// dispatcher is the same execution.Runner that enforces node deadlines, so a
// 500ms bound on the body member must terminate it. Without the projection line
// the member falls back to the 30m default and the execution hangs.
func TestMapBodyNodeTimeout(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slow := newBlockingTimeoutHandler("test.mapbody.timeout.slow")
	defer close(slow.release)

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.mapbody.timeout.slow", slow)

	cache := runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: 16, MaxPackageBytes: 10 * 1024 * 1024})
	subgraphRT := runnersvc.NewSubgraphRuntime(registry, cache)
	groupRT := runnersvc.NewGroupRuntime(registry, cache, runnersvc.WithSuspendDisabled())

	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:      "runner-mapbody-timeout",
			Concurrency:   2,
			PollWait:      10 * time.Millisecond,
			GroupRuntime:  groupRT,
			SubgraphRuntime: subgraphRT,
			Capabilities: []protocol.Capability{
				{NodeType: "xflow.map"},
				{NodeType: "test.mapbody.timeout.slow"},
			},
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-mapbody-timeout")

	// The body member is authored as a subgraph node whose "timeout" field
	// (nanoseconds, the JSON encoding of time.Duration) must survive
	// decodeSubgraphMembers -> node_body_package.go's hand copy -> inner
	// compile -> the inner engine's lease stamp.
	bodyMemberTimeout := int64(500 * time.Millisecond)
	wf := &types.WorkflowDef{
		Name: "map-body-timeout",
		Nodes: []types.NodeDef{{
			Name: "m",
			Type: "xflow.map",
			Kind: types.NodeKindAction,
			Parameters: map[string]any{
				"items":      "$input.rows",
				"batch_size": 1,
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{
								"name":    "slow",
								"type":    "test.mapbody.timeout.slow",
								"timeout": bodyMemberTimeout,
							},
						},
					},
				},
			},
		}},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{
		"rows": []any{map[string]any{"id": 1}},
	})

	select {
	case <-slow.started:
	case <-time.After(10 * time.Second):
		t.Fatal("map body member never started within 10s")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "m")
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed (a map body member whose 500ms Timeout was projected "+
			"must terminate it; if it stayed running the projection dropped the field)", result.Status)
	}
}

// TestTimeoutDoesNotRetry proves a timeout is a verdict, not a transient: a
// node configured with Retry must still be invoked exactly once when it blocks
// past its Timeout. The runner reclassifies the deadline-fired ctx.Err() into a
// Permanent error (reclassifyTimeout), and the retry short-circuit in
// tryRetryWithAttempt declines to re-run a Permanent error. Without that
// reclassification the ctx.Err() would be retried and invocations would exceed 1.
func TestTimeoutDoesNotRetry(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarness(t, addr, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := newBlockingTimeoutHandler("test.noretry.slow")
	defer close(handler.release)
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.noretry.slow", handler)

	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-noretry-timeout",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.noretry.slow"}},
			PollWait:     10 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-noretry-timeout")

	wf := &types.WorkflowDef{
		Name: "timeout-no-retry",
		Nodes: []types.NodeDef{{
			Name:    "slow",
			Type:    "test.noretry.slow",
			Kind:    types.NodeKindAction,
			Timeout: 500 * time.Millisecond,
			// Retry is configured and would re-run a transient failure. A
			// timeout must NOT be retried even with this set.
			Retry: &types.RetrySettings{Enabled: true, MaxAttempts: 3},
		}},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "noretry"})

	select {
	case <-handler.started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started within 10s")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "slow")
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed", result.Status)
	}
	if got := handler.invocations.Load(); got != 1 {
		t.Fatalf("handler invocations = %d, want 1 -- a timeout must not be retried even when Retry is "+
			"configured; more than one invocation means the deadline-fired error was treated as transient", got)
	}
}

// TestServerBackstopTerminatesUncooperativeRunner is the binding test: it is
// the only end-to-end proof that the server-side renewal backstop (Task 6)
// commits a terminal when a runner holds a lease past its ExecutionDeadline
// without reporting. The discriminator is the source="server" metric, emitted
// ONLY from group_control_loop.go's renewLease branch. A test that merely
// asserted the node reached a terminal would pass on the runner's own commit
// too (the runner-side deadline fires on every normal path) and stay green with
// Task 6 deleted -- the vacuous-pass trap this dispatch warns about.
//
// ROUTE: the renewal endpoint is plain HTTP and resolves the lease itself via
// LookupLease, so a test can acquire a lease for a short-timeout node, NOT
// execute it (the leaseCapturingClient swallows the polled lease so the runner
// never calls Execute), wait past the deadline, and POST a renewal directly.
// No runner is executing, so the runner cannot win; the only thing that can
// commit the terminal is the server's renewLease branch. With that branch
// removed, the renewal would succeed (Renewed=true) and the node would stay
// running -- a real red.
func TestServerBackstopTerminatesUncooperativeRunner(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarnessWithMetrics(t, addr, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const nodeType = "test.backstop.slow"
	captureClient := &leaseCapturingClient{inner: protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client())}
	// The runner needs a handler registered only so the registry is non-empty;
	// the leaseCapturingClient ensures Execute is never reached. Without a
	// registered handler the runner's lookup would fail on a claimed lease,
	// but no claim ever reaches Execute here.
	registry := execution.NewRegistry()
	registry.RegisterGlobal(nodeType, newBlockingTimeoutHandler(nodeType))

	runner := runnersvc.New(
		captureClient,
		registry,
		runnersvc.Config{
			RunnerID:     "runner-backstop",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: nodeType}},
			PollWait:     10 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-backstop")

	// 1s node timeout -- short enough that the deadline passes well inside the
	// 30s lease TTL, so the sweeper cannot reclaim the lease before the
	// renewal lands.
	wf := &types.WorkflowDef{
		Name: "server-backstop",
		Nodes: []types.NodeDef{
			{Name: "stuck", Type: nodeType, Kind: types.NodeKindAction, Timeout: 1 * time.Second},
		},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "backstop"})

	// Wait for the runner to claim the lease (it is finalized in the server's
	// directory during the poll that leaseCapturingClient swallows).
	var lease *engine.TaskLease
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		lease, _, _ = captureClient.capturedLease()
		if lease != nil {
			break
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("context cancelled before lease was claimed: %v", ctx.Err())
		}
	}
	if lease == nil {
		t.Fatal("runner never claimed the lease within 10s -- the task was not dispatched")
	}
	runnerID, sessionID := func() (string, string) {
		_, rid, sid := captureClient.capturedLease()
		return rid, sid
	}()

	// Wait past the ExecutionDeadline so the renewal branch's predicate is true.
	// 1.5s gives a comfortable margin over the 1s bound while staying far inside
	// the 30s lease TTL.
	time.Sleep(1500 * time.Millisecond)

	if !lease.ExecutionDeadline.IsZero() {
		t.Logf("captured lease ExecutionDeadline=%s (now=%s, elapsed past=%v)",
			lease.ExecutionDeadline.Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano),
			time.Since(lease.ExecutionDeadline))
	} else {
		t.Fatal("captured lease has a ZERO ExecutionDeadline -- the node's 1s Timeout was not stamped on " +
			"the lease during dispatch; the backstop predicate can never fire")
	}

	// POST the renewal directly, exactly as the runner's renewal loop would.
	renewResp, err := protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()).RenewLease(ctx,
		protocol.RenewLeaseRequest{
			RunnerID:   runnerID,
			SessionID:  sessionID,
			LeaseID:    string(lease.LeaseID),
			LeaseToken: string(lease.LeaseToken),
			Extend:     30000,
		})
	if err != nil {
		t.Fatalf("RenewLease error = %v", err)
	}
	if renewResp.Renewed {
		t.Fatal("RenewLease Renewed=true for a lease past its ExecutionDeadline; the backstop must REFUSE " +
			"renewal so the sweeper does not re-enqueue the task one TTL later")
	}
	if renewResp.Error == "" {
		t.Fatal("RenewLease Error is empty, want a non-empty refusal reason")
	}

	// The node must have reached a terminal state via the server's commit.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "stuck")
	if result.Status != types.ExecutionStatusFailed {
		t.Fatalf("execution status = %s, want failed (the server backstop must commit a terminal via " +
			"CommitTaskTimeout; refusing alone leaves the sweeper to re-enqueue)", result.Status)
	}

	// THE DISCRIMINATOR: source="server" is emitted ONLY from the renewLease
	// branch (group_control_loop.go via Core.observeNodeTimeout). source="runner"
	// is emitted only from execution/runner.go, which never runs in this test.
	// If Task 6's branch were deleted, this metric would not appear and the
	// renewal would have succeeded above -- so this assertion, combined with the
	// Renewed=false check, makes the test go red on a Task-6 deletion rather than
	// green-lighting on a vacuous terminal.
	body := scrapeMetrics(t, h.m)
	want := `xflow_node_timeouts_total{node_type="` + nodeType + `",source="server"} 1`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics body missing %q -- the server-side backstop did not emit the source=server "+
			"timeout; without it the terminal above could be a runner commit, not the server's:\n%s", want, body)
	}
	// source=runner must NOT appear: no runner ever executed this lease.
	if strings.Contains(body, `source="runner"`) {
		t.Fatalf("metrics body contains a source=runner timeout -- no runner executed this lease, so a "+
			"runner-source count means the source label is wired to the wrong origin:\n%s", body)
	}
}

// TestGroupLeaseBackstopNonCollision is the integration-level check my ruling
// asked for: verify the T5/T6 interaction rather than assuming it. Task 5 wires
// a group's own Timeout through GroupLeasePayload.Deadline (engine/group_lease.go
// stamps payload.Deadline from gm.Timeout, NOT taskLease.ExecutionDeadline).
// Task 6's renewLease backstop fires only when !resolved.ExecutionDeadline.IsZero()
// AND the deadline has passed -- so a group lease, whose ExecutionDeadline is
// deliberately zero, must NEVER enter the backstop branch even after the group's
// own deadline has elapsed. That "never meets" is controller-reasoned in the
// code comments; this test measures it against a real lease directory.
//
// A group lease is NOT refused by the backstop past its group deadline -- group
// timeouts are NOT covered by the backstop (a documented limit). This test does
// NOT assert the group is terminated; it asserts the backstop branch was
// skipped (no source="server" metric) and the renewal reached the group renewal
// path instead.
func TestGroupLeaseBackstopNonCollision(t *testing.T) {
	addr := requireRedis(t)
	h := newServerRunnerHarnessWithMetrics(t, addr, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	captureClient := &leaseCapturingClient{inner: protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client())}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.group.col.member", &groupMemberHandler{})

	// Advertise group.exec.v1 so the control plane routes the group task here.
	runner := runnersvc.New(
		captureClient,
		registry,
		runnersvc.Config{
			RunnerID:    "runner-group-backstop",
			Concurrency: 1,
			PollWait:    10 * time.Millisecond,
			Capabilities: []protocol.Capability{
				{NodeType: "test.group.col.member"},
				{NodeType: engine.GroupNodeType, Features: []string{engine.FeatureGroupExecV1}},
			},
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-group-backstop")

	// A group WITH a 1s group-level Timeout. The deadline lands in
	// GroupLeasePayload.Deadline; the TaskLease.ExecutionDeadline must stay zero.
	wf := &types.WorkflowDef{
		Name: "group-backstop-noncollision",
		Nodes: []types.NodeDef{
			{Name: "g.source", Type: "test.group.col.member", Kind: types.NodeKindAction},
			{Name: "g.sink", Type: "test.group.col.member", Kind: types.NodeKindAction},
			{Name: "out", Type: "test.group.col.member", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"g.source": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			"g.sink":   {"main": {Targets: []types.Connection{{Node: "out", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"g.source", "g.sink"}, Timeout: 1 * time.Second}},
	}
	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), wf, map[string]any{"seed": "group-backstop"})

	// Wait for the group lease to be claimed.
	var lease *engine.TaskLease
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		lease, _, _ = captureClient.capturedLease()
		if lease != nil {
			break
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("context cancelled before group lease was claimed: %v", ctx.Err())
		}
	}
	if lease == nil {
		t.Fatal("runner never claimed the group lease within 10s -- the group task was not dispatched")
	}
	runnerID, sessionID := func() (string, string) {
		_, rid, sid := captureClient.capturedLease()
		return rid, sid
	}()

	// THE INVARIANT: a group lease's TaskLease.ExecutionDeadline is zero. The
	// group's own deadline lives in GroupLeasePayload.Deadline, which the renewLease
	// backstop never reads. If this were non-zero, the backstop would fire on a
	// group lease -- the collision this test exists to rule out.
	if !lease.ExecutionDeadline.IsZero() {
		t.Fatalf("group lease TaskLease.ExecutionDeadline = %s, want zero -- a group lease must "+
			"not carry ExecutionDeadline or the T6 backstop collides with the T5 group deadline",
			lease.ExecutionDeadline.Format(time.RFC3339Nano))
	}
	if lease.Task.Type != engine.TaskTypeGroupExec {
		t.Fatalf("captured lease is not a group task (Type=%q) -- the group was not dispatched as a "+
			"group unit; this test then proves nothing about the group path", lease.Task.Type)
	}

	// Wait past the group's 1s deadline.
	time.Sleep(1500 * time.Millisecond)

	// POST a renewal. The backstop branch must be skipped (isGroupTask guard +
	// zero ExecutionDeadline), so this must reach the group renewal path, NOT be
	// refused with "execution deadline exceeded".
	renewResp, err := protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()).RenewLease(ctx,
		protocol.RenewLeaseRequest{
			RunnerID:   runnerID,
			SessionID:  sessionID,
			LeaseID:    string(lease.LeaseID),
			LeaseToken: string(lease.LeaseToken),
			Extend:     30000,
		})
	if err != nil {
		t.Fatalf("RenewLease error = %v", err)
	}
	if renewResp.Error == "execution deadline exceeded" {
		t.Fatal("group lease was REFUSED by the node-timeout backstop -- the T6 backstop collided " +
			"with the T5 group deadline (a group lease entered the node-backstop branch)")
	}

	// No source="server" timeout must have been emitted for the group lease.
	// This is the behavioural proof the backstop branch was skipped: the metric
	// is emitted ONLY from that branch.
	body := scrapeMetrics(t, h.m)
	if strings.Contains(body, "xflow_node_timeouts_total") {
		t.Fatalf("a node-timeout metric was emitted during a group lease renewal -- the backstop "+
			"branch fired for a group lease, which is the T5/T6 collision this test rules out:\n%s", body)
	}

	// The group execution is left running (no terminal commit): group timeouts
	// are NOT covered by the backstop. Cancel to tear down; the harness cleanup
	// stops the control plane.
	_ = execID
}
