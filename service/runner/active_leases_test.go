package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// activeLeasePollClient hands out one lease and then records what every
// subsequent poll reports as in flight.
type activeLeasePollClient struct {
	mu     sync.Mutex
	lease  *engine.TaskLease
	polls  [][]string
	cancel context.CancelFunc
}

func (c *activeLeasePollClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{RunnerID: "runner-1", SessionID: "session-1"}, nil
}

func (c *activeLeasePollClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}

func (c *activeLeasePollClient) Poll(_ context.Context, req protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	c.mu.Lock()
	c.polls = append(c.polls, append([]string(nil), req.ActiveLeaseIDs...))
	lease := c.lease
	c.lease = nil
	c.mu.Unlock()
	if lease == nil {
		return protocol.PollTaskResponse{Wait: time.Millisecond}, nil
	}
	return protocol.PollTaskResponse{Lease: lease}, nil
}

func (c *activeLeasePollClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.cancel()
	return protocol.ReportResultResponse{Accepted: true}, nil
}

func (c *activeLeasePollClient) seen() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.polls...)
}

var _ ProtocolClient = (*activeLeasePollClient)(nil)

// The control plane replays any lease it has recorded as leased to this runner
// and session. It cannot distinguish one whose poll response was lost from one
// a worker is executing right now, so the runner reports what it holds. Without
// this every idle worker of a Concurrency > 1 runner is handed its busy
// sibling's lease, and the node executes once per unit of concurrency.
func TestRunnerReportsTheLeasesItIsExecutingOnEveryPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockingRenewHandler{started: make(chan struct{}), release: make(chan struct{})}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.slow", handler)

	client := &activeLeasePollClient{cancel: cancel}
	client.lease = &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-active"),
		LeaseToken: engine.LeaseToken("token-active"),
		Task:       engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "slow"},
		Input:      &types.Input{Data: map[string]any{"x": 1}},
		NodeType:   "test.slow",
	}

	r := New(client, registry, Config{
		RunnerID:          "runner-1",
		Concurrency:       2,
		Capabilities:      []protocol.Capability{{NodeType: "test.slow"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// The second worker is idle and polling while the first executes. Wait for
	// a poll that happened strictly after the handler entered.
	deadline := time.After(2 * time.Second)
	var reported bool
	for !reported {
		for _, ids := range client.seen() {
			for _, id := range ids {
				if id == "lease-active" {
					reported = true
				}
			}
		}
		if reported {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no poll reported lease-active as in flight while its handler was running — " +
				"the server will replay it to this runner's other workers and the node runs twice")
		case <-time.After(2 * time.Millisecond):
		}
	}

	close(handler.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

// The complement: once the work is over, the lease must stop being reported.
// A runner that kept naming a finished lease would suppress the replay that
// exists to recover a result whose report never landed.
func TestRunnerStopsReportingALeaseOnceItsWorkIsDone(t *testing.T) {
	active := newActiveLeases()
	active.add("lease-1")
	if got := active.snapshot(); len(got) != 1 || got[0] != "lease-1" {
		t.Fatalf("snapshot after add = %v, want [lease-1]", got)
	}
	active.remove("lease-1")
	if got := active.snapshot(); got != nil {
		t.Fatalf("snapshot after remove = %v, want nil so the field is omitted entirely", got)
	}
}

// A replayed lease legitimately arrives while an earlier copy is still held —
// that is what replay means when the server never saw a report. A plain set
// would be cleared by whichever copy finishes first, unmarking a lease the
// other worker is still executing and re-opening the duplication this whole
// mechanism closes.
func TestActiveLeasesSurvivesUntilEveryHolderIsDone(t *testing.T) {
	active := newActiveLeases()
	active.add("lease-1")
	active.add("lease-1")
	active.remove("lease-1")
	if got := active.snapshot(); len(got) != 1 || got[0] != "lease-1" {
		t.Fatalf("snapshot with one holder remaining = %v, want [lease-1]", got)
	}
	active.remove("lease-1")
	if got := active.snapshot(); got != nil {
		t.Fatalf("snapshot after the last holder = %v, want nil", got)
	}
}
