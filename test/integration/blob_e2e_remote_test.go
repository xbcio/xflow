//go:build integration

package integration

import (
	"context"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	_ "github.com/xbcio/xflow/node" // registers the builtin node types in nodereg
	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// blobE2EClient is the runner's view of the control plane. It passes every call
// through and records what the control plane did on the batch path: the batch
// leases it handed out and any poll that returned an error (a runner ends its
// session on one). It can also hold the runner back: once the map node's own
// lease has gone out, polls are answered with "nothing to do" until the test
// opens the gate, which gives the test a window after the map node expanded and
// before any batch is claimed.
type blobE2EClient struct {
	inner runnersvc.ProtocolClient

	mu        sync.Mutex
	runnerID  string
	sessionID string
	registers int                 // sessions the runner opened: more than one means a poll error ended one
	leases    []*engine.TaskLease // batch leases handed to the runner
	pollErrs  []error             // polls that returned an error
	mapOut    bool                // the map node's own lease went to the runner
	open      bool                // the test released the polls the gate holds back
}

var _ runnersvc.ProtocolClient = (*blobE2EClient)(nil)

func (c *blobE2EClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	resp, err := c.inner.Register(ctx, req)
	if err == nil {
		c.mu.Lock()
		c.runnerID, c.sessionID = req.RunnerID, resp.SessionID
		c.registers++
		c.mu.Unlock()
	}
	return resp, err
}

func (c *blobE2EClient) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return c.inner.Heartbeat(ctx, req)
}

func (c *blobE2EClient) ReportResult(ctx context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return c.inner.ReportResult(ctx, req)
}

// held reports whether the gate is closed: the map node's lease is out and the
// test has not opened it.
func (c *blobE2EClient) held() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapOut && !c.open
}

// Poll passes the poll through unless the gate is closed, and records what came
// back. The map node's own lease is the one for node "fan" that is not a batch;
// the batch leases are the ones that carry a SubgraphPayload.
func (c *blobE2EClient) Poll(ctx context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	if c.held() {
		select {
		case <-ctx.Done():
			return protocol.PollTaskResponse{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return protocol.PollTaskResponse{Wait: 20 * time.Millisecond}, nil
	}
	resp, err := c.inner.Poll(ctx, req)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			c.pollErrs = append(c.pollErrs, err)
		}
		return resp, err
	}
	switch l := resp.Lease; {
	case l == nil:
	case l.SubgraphPayload != nil:
		c.leases = append(c.leases, l)
	case l.Task.NodeName == "fan" && l.Task.Type != engine.TaskTypeNodeBatch:
		c.mapOut = true
	}
	return resp, err
}

// release opens the gate.
func (c *blobE2EClient) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.open = true
}

// seen returns what the control plane did on the batch path so far.
func (c *blobE2EClient) seen() (leases []*engine.TaskLease, pollErrs []error, registers int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append(leases, c.leases...), append(pollErrs, c.pollErrs...), c.registers
}

// blobE2ERemote is the remote batch path: a control plane over a distributed
// backend that writes blobs, the engine it creates (which hands batches to
// runners instead of running them), and one real runner that reaches the control
// plane through blobE2EClient.
type blobE2ERemote struct {
	run    *blobE2ERun
	cp     *control.ControlPlane
	client *blobE2EClient
	caps   []protocol.Capability
	reg    *execution.Registry
}

func newBlobE2ERemote(t *testing.T, addr string) *blobE2ERemote {
	t.Helper()
	r := newBlobE2EBase(t, addr, true, true)
	cp, err := control.NewControlPlane(control.Config{Backend: r.b, PollWait: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatalf("ControlPlane.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cp.Shutdown(ctx); err != nil {
			t.Errorf("ControlPlane.Shutdown: %v", err)
		}
	})
	reg := execution.NewRegistry()
	var caps []protocol.Capability
	for _, nodeType := range []string{"xflow.start", "xflow.map", "xflow.function", "xflow.end"} {
		h, ok := nodereg.Lookup(nodeType)
		if !ok {
			t.Fatalf("builtin %s is not registered", nodeType)
		}
		reg.RegisterGlobal(nodeType, h)
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}
	return &blobE2ERemote{run: r, cp: cp, client: &blobE2EClient{inner: cp.RunnerServer().InProcessRunnerClient("")}, caps: caps, reg: reg}
}

// startRunner runs one real runner against the control plane through the
// recording client and waits for it to register.
func (rm *blobE2ERemote) startRunner(t *testing.T) {
	t.Helper()
	const runnerID = "blob-e2e-runner"
	cache := runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: 64})
	runner := runnersvc.New(rm.client, rm.reg, runnersvc.Config{
		RunnerID:        runnerID,
		Concurrency:     1,
		Capabilities:    rm.caps,
		PollWait:        10 * time.Millisecond,
		Namespaces:      []namespace.Namespace{rm.run.ns},
		SubgraphRuntime: runnersvc.NewSubgraphRuntime(rm.reg, cache),
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("runner did not stop in time")
		}
	})
	waitForE2ERunner(t, rm.cp.RunnerDirectory(), runnerID)
}

// submit starts an execution of the workflow in the run's namespace. The
// context ends the waits after blobE2EWait.
func (rm *blobE2ERemote) submit(t *testing.T, batchSize int) (context.Context, types.ExecutionID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(namespace.WithNamespace(context.Background(), rm.run.ns), blobE2EWait)
	t.Cleanup(cancel)
	g, err := graph.Compile(blobE2EWorkflow(batchSize))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id, err := rm.cp.Engine().Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return ctx, id
}

// atGate returns once the map node has expanded and its batch entries sit in the
// runner directory, with the gate still closed: no runner has been handed a
// batch. The entries are told apart from the outbox's copy by their key.
func (rm *blobE2ERemote) atGate(ctx context.Context, t *testing.T) {
	t.Helper()
	dpUntil(ctx, t, "the batch entries to reach the runner directory", func() bool {
		keys, err := blobE2EHolders(ctx, rm.run.rdb, rm.run.ns, "items_ref")
		if err != nil {
			t.Fatalf("scan redis: %v", err)
		}
		for _, k := range keys {
			if strings.Contains(k, "runner-directory") {
				return true
			}
		}
		return false
	})
	if leases, _, _ := rm.client.seen(); len(leases) != 0 {
		t.Fatalf("%d batch lease(s) went out before the gate was opened", len(leases))
	}
	if _, _, regs := rm.client.seen(); regs != 1 {
		t.Fatalf("runner sessions = %d before the gate, want 1", regs)
	}
}

// awaitEnd waits for the execution to end, and fails at once if a poll of the
// runner returned an error: the runner ends its session on one, so the run would
// never settle and the test would only learn that when the wait ran out.
func (rm *blobE2ERemote) awaitEnd(ctx context.Context, t *testing.T, id types.ExecutionID) {
	t.Helper()
	dpUntil(ctx, t, "the execution to end", func() bool {
		if _, errs, _ := rm.client.seen(); len(errs) > 0 {
			t.Fatalf("a runner poll returned an error: %v", errs[0])
		}
		snap, err := rm.run.b.State().GetExecution(ctx, id)
		return err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status)
	})
}

// session returns the runner id and session id the runner registered with.
func (c *blobE2EClient) session() (runnerID, sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runnerID, c.sessionID
}

// blobE2EOriginalItems is the array the map node's expression produces.
func blobE2EOriginalItems() []any {
	out := make([]any, blobE2EItems)
	for i := range out {
		out[i] = map[string]any{"id": float64(i + 1), "pad": blobE2EMarker}
	}
	return out
}

// assertNoMarkerInRedis fails if any Redis key of the run's namespace holds a
// copy of the items.
func (rm *blobE2ERemote) assertNoMarkerInRedis(ctx context.Context, t *testing.T, when string) {
	t.Helper()
	holders, err := blobE2EHolders(ctx, rm.run.rdb, rm.run.ns, blobE2EMarker)
	if err != nil {
		t.Fatalf("scan redis: %v", err)
	}
	if len(holders) != 0 {
		t.Errorf("%s: the items are in Redis under %v, want them only in the blob store", when, holders)
	}
}

// blobE2EWindow returns the [items_start, items_end) bounds a batch task carries
// next to its items_ref.
func blobE2EWindow(t *testing.T, task *engine.Task) (start, end int) {
	t.Helper()
	if task.Payload == nil {
		t.Fatalf("batch task has no payload")
	}
	num := func(key string) int {
		v, _ := blobE2ENormalize(t, task.Payload.Data[key]).(float64)
		return int(v)
	}
	return num("items_start"), num("items_end")
}

// assertBatchLease checks one batch lease the control plane built from a blob:
// the runner got the whole array and its own window as values, while the task
// that travelled through the queue and the directory kept only the reference.
func assertBatchLease(t *testing.T, l *engine.TaskLease, original []any) {
	t.Helper()
	data := l.Task.Payload.Data
	if _, ok := data["items_ref"]; !ok {
		t.Errorf("lease task payload has no items_ref: %v", data)
	}
	for _, key := range []string{"items", "all_items"} {
		if _, ok := data[key]; ok {
			t.Errorf("lease task payload carries %q inline, want only the reference", key)
		}
	}
	start, end := blobE2EWindow(t, &l.Task)
	p := l.SubgraphPayload
	if got := blobE2ENormalize(t, p.AllItems); !reflect.DeepEqual(got, blobE2ENormalize(t, original)) {
		t.Errorf("lease AllItems differ from the original array (%d vs %d items)", len(p.AllItems), len(original))
	}
	if start < 0 || end > len(original) || start >= end {
		t.Fatalf("lease window [%d,%d) is not inside the %d items", start, end, len(original))
	}
	if got := blobE2ENormalize(t, p.Items); !reflect.DeepEqual(got, blobE2ENormalize(t, original[start:end])) {
		t.Errorf("lease Items differ from original[%d:%d]", start, end)
	}
}

// TestBlobE2ERemoteBatchLeaseFromBlob: the control plane builds each batch lease
// from the blob. The runner is held back until the map node's batch entries sit
// in the runner directory, so that moment can be inspected: the items are in no
// Redis key, only the reference is. Each lease it then gets carries the whole
// array and its window as values, the task inside it keeps the bare reference,
// and the map results come back in item order.
func TestBlobE2ERemoteBatchLeaseFromBlob(t *testing.T) {
	addr := requireRedis(t)
	rm := newBlobE2ERemote(t, addr)
	rm.startRunner(t)
	ctx, id := rm.submit(t, 7)
	rm.atGate(ctx, t)
	rm.assertNoMarkerInRedis(ctx, t, "with the batches waiting in the directory")
	if puts, _, _ := rm.run.store.counts(); puts == 0 || len(blobE2EObjects(rm.run.dir)) != 1 {
		t.Fatalf("blob puts = %d, objects = %v, want the items stored once", puts, blobE2EObjects(rm.run.dir))
	}
	rm.client.release()
	rm.awaitEnd(ctx, t, id)

	res := waitForCompletion(ctx, t, rm.run.b.State(), id, blobE2ENodes...)
	fan, err := rm.run.b.State().GetNode(ctx, id, "fan")
	if err != nil || fan == nil {
		t.Fatalf("GetNode(fan) = %v, %v", fan, err)
	}
	blobE2EAssertSuccess(t, blobE2EOutcome{id: id, res: res, fan: fan})
	leases, pollErrs, sessions := rm.client.seen()
	if len(pollErrs) != 0 || sessions != 1 {
		t.Errorf("poll errors = %v, runner sessions = %d, want none and 1", pollErrs, sessions)
	}
	if len(leases) != 5 {
		t.Fatalf("batch leases handed out = %d, want 5 (30 items in windows of 7)", len(leases))
	}
	starts := map[int]bool{}
	for _, l := range leases {
		assertBatchLease(t, l, blobE2EOriginalItems())
		start, _ := blobE2EWindow(t, &l.Task)
		starts[start] = true
	}
	if len(starts) != 5 {
		t.Errorf("distinct batch windows = %v, want 5", starts)
	}
	if _, gets, fails := rm.run.store.counts(); gets == 0 || fails != 0 {
		t.Errorf("blob reads = %d (failed %d), want reads that all succeeded", gets, fails)
	}
}

// pollByHand asks the control plane for work as the gated runner would, and
// returns the answer. The runner itself is held back, so this is the only poll
// that reaches the control plane.
func (rm *blobE2ERemote) pollByHand(ctx context.Context, t *testing.T) (protocol.PollTaskResponse, error) {
	t.Helper()
	runnerID, sessionID := rm.client.session()
	return rm.client.inner.Poll(ctx, protocol.PollTaskRequest{
		RunnerID: runnerID, SessionID: sessionID, Capacity: 1, Capabilities: rm.caps,
	})
}

// TestBlobE2ERemoteBatchLeaseBlobMissing: the blob is gone when the control plane
// builds the batch lease. That is permanent, so the poll must answer "no work"
// without an error (an error would end the runner's session), settle the batch
// as failed so the map node ends, and drop the claim so no later poll is handed
// it again.
func TestBlobE2ERemoteBatchLeaseBlobMissing(t *testing.T) {
	addr := requireRedis(t)
	rm := newBlobE2ERemote(t, addr)
	rm.startRunner(t)
	ctx, id := rm.submit(t, blobE2EItems)
	rm.atGate(ctx, t)
	if _, gets, _ := rm.run.store.counts(); gets != 0 {
		t.Fatalf("blob reads before the loss = %d, want 0 (nothing may be cached from it)", gets)
	}
	objs := blobE2EObjects(rm.run.dir)
	if len(objs) != 1 || os.Remove(objs[0]) != nil {
		t.Fatalf("could not remove the one blob object %v", objs)
	}

	resp, err := rm.pollByHand(ctx, t)
	if err != nil || resp.Lease != nil {
		t.Fatalf("poll = lease %v, err %v; want no lease and no error", resp.Lease, err)
	}
	if leases, _, _ := rm.client.seen(); len(leases) != 0 {
		t.Errorf("%d batch lease(s) were handed out for a batch with no items", len(leases))
	}
	// The runner stays held back, so only that one poll could have settled the
	// batch: the map node has to end from it alone.
	rm.awaitEnd(ctx, t, id)

	res := waitForCompletion(ctx, t, rm.run.b.State(), id, blobE2ENodes...)
	fan, err := rm.run.b.State().GetNode(ctx, id, "fan")
	if err != nil || fan == nil {
		t.Fatalf("GetNode(fan) = %v, %v", fan, err)
	}
	if res.Status != types.ExecutionStatusFailed || fan.Status != types.NodeStatusFailed ||
		!strings.Contains(fan.Error, "batch items unavailable") {
		t.Fatalf("execution %s, fan %s %q; want failed with a batch items unavailable reason", res.Status, fan.Status, fan.Error)
	}
	// The claim is gone: the directory has nothing left to hand this runner, and
	// the runner's session was never ended by an error.
	runnerID, sessionID := rm.client.session()
	again := control.ClaimRequest{RunnerID: runnerID, SessionID: sessionID, Now: time.Now()}
	if claim, ok, err := rm.cp.RunnerDirectory().ClaimForRunner(ctx, again); ok || err != nil {
		t.Errorf("a claim for %s is still available (err %v), want it dropped", claim.Assignment.AssignmentID, err)
	}
	if resp, err := rm.pollByHand(ctx, t); err != nil || resp.Lease != nil {
		t.Errorf("second poll = lease %v, err %v; want no lease and no error", resp.Lease, err)
	}
	if _, errs, sessions := rm.client.seen(); len(errs) != 0 || sessions != 1 {
		t.Errorf("poll errors = %v, runner sessions = %d, want none and 1", errs, sessions)
	}
	blobE2EAssertQueuesIdle(t, rm.run)
}
