package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// recordingQueueLogger is an engine.Logger that keeps every Error/Errorf line.
// The inner queue calls it from its own consumer goroutine while the test waits
// on the main one, so every field is mutex-guarded.
type recordingQueueLogger struct {
	mu     sync.Mutex
	errors []string
}

func (r *recordingQueueLogger) record(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, msg)
}

func (r *recordingQueueLogger) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.errors...)
}

func (r *recordingQueueLogger) Error(msg string, _ ...any)     { r.record(msg) }
func (r *recordingQueueLogger) Errorf(format string, _ ...any) { r.record(format) }
func (r *recordingQueueLogger) Panic(msg string, _ ...any)     { r.record(msg) }
func (r *recordingQueueLogger) Panicf(format string, _ ...any) { r.record(format) }

func (r *recordingQueueLogger) Debug(string, ...any)  {}
func (r *recordingQueueLogger) Debugf(string, ...any) {}
func (r *recordingQueueLogger) Info(string, ...any)   {}
func (r *recordingQueueLogger) Infof(string, ...any)  {}
func (r *recordingQueueLogger) Warn(string, ...any)   {}
func (r *recordingQueueLogger) Warnf(string, ...any)  {}

// queueDispatchFailureMarker is the leading text of the line the inner queue
// emits through its logger on every failed dispatch
// (backend/providers/local/memory_queue.go:192). It is quoted rather than
// referenced so that renaming the message breaks this test: the message is the
// operator-visible contract, and a test that reads the constant back out of the
// source would pass no matter what the source said.
const queueDispatchFailureMarker = "requeueing task after transient dispatch failure"

// undeclaredHandlerPackage is a group whose single member's node type is not
// registered anywhere, and — this is the load-bearing part — is not listed in
// Requirements either.
//
// That combination is what makes the failure reach the QUEUE rather than any of
// the layers that would otherwise report it:
//
//   - validatePackage (execution/subgraph/cache.go:158) iterates pkg.Requirements
//     and nothing else. It never cross-checks Def.Nodes against that list, so an
//     under-declared package passes validation instead of being rejected up front
//     with "handler not available".
//   - The handler lookup then fails at dispatch time, and Registry.Get's miss
//     returns a bare fmt.Errorf (execution/registry.go:258) — not an
//     *ExecutorFailure. ClassifyExecutorFailure (execution/dispatcher.go:106)
//     maps anything else to ExecutorFailureUnknown, whose case in HandleTask is
//     the default at execution/dispatcher.go:177: `return executeErr`, handed
//     straight back to the queue rather than committed.
//
// This is not a contrived shape. It is exactly what a runner that is one deploy
// behind looks like: the control plane sends a group whose member type that
// runner does not have.
func undeclaredHandlerPackage() *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "n",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "n", Type: "test.never_registered", Version: 1},
				{Name: "__collector_n_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"n": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_n_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_n_main", SrcNode: "n", Port: "main"},
		},
	}
}

// awaitQueueLogLine polls until the logger has recorded a line containing
// marker, and fails the test if the deadline passes first.
//
// Polling, rather than waiting for the runtime call to return, IS the contract
// being pinned: on the unfixed code that call does not return until its
// deadline, and the whole point of the logger is that the trouble is on the
// record long before then.
func awaitQueueLogLine(t *testing.T, logs *recordingQueueLogger, marker, whatFailed string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range logs.snapshot() {
			if strings.Contains(line, marker) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: no queue log line containing %q; recorded = %q", whatFailed, marker, logs.snapshot())
}

// runDetached starts fn on its own goroutine and makes the test wait for it to
// unwind before finishing.
//
// Both callers must run their runtime detached, because an inner task the queue
// keeps requeueing never commits and the call therefore blocks to its deadline.
// The wait matters as much as the goroutine: without it the queue's requeue
// goroutines outlive the test, and the next test in the package inherits them.
func runDetached(t *testing.T, cancel context.CancelFunc, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("runtime did not unwind within 10s of cancellation")
		}
	})
}

// TestGroupRuntime_QueueLoggerRecordsInnerDispatchFailure is the direct
// criterion for WithGroupQueueLogger.
//
// The per-attempt inner backend this runtime builds had no logger at all, so
// everything its queue said about a task it could not dispatch went nowhere. The
// outer result cannot stand in: the task never commits, the inner execution
// never reaches a terminal state, and subgraph.Executor's WaitDone therefore
// blocks until the deadline and reports the single string "deadline exceeded"
// (execution/subgraph/subgraph.go:321) — the same words a genuinely slow group
// produces, for a group that could never have run.
//
// The assertion is on the log line rather than the outcome precisely because
// the outcome is what fails to distinguish the two.
func TestGroupRuntime_QueueLoggerRecordsInnerDispatchFailure(t *testing.T) {
	reg := execution.NewRegistry()

	logs := &recordingQueueLogger{}
	rt := NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSuspendDisabled(), WithGroupQueueLogger(logs))

	pkg := undeclaredHandlerPackage()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	runDetached(t, cancel, func() {
		_, _ = rt.ExecuteRequest(ctx, subgraph.Request{
			Package:     pkg,
			PackageHash: hash,
			Input:       &types.Input{Data: map[string]any{"n": 1.0}},
		})
	})

	awaitQueueLogLine(t, logs, queueDispatchFailureMarker,
		"the logger handed to NewGroupRuntime never reached the per-attempt backend's queue")
}

// TestSubgraphRuntime_QueueLoggerReachesInnerBackend is the map-batch
// counterpart. SubgraphRuntime builds its own backend per item through a
// separate constructor, so wiring the group side proves nothing about it — the
// two options are independent and each needs its own criterion.
//
// This one carries the larger share of the traffic on the collection workload:
// one backend, and therefore one queue, per map item.
func TestSubgraphRuntime_QueueLoggerReachesInnerBackend(t *testing.T) {
	reg := execution.NewRegistry()

	logs := &recordingQueueLogger{}
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSubgraphQueueLogger(logs))

	body := undeclaredHandlerPackage()
	hash, err := graph.ComputePackageHash(body)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	runDetached(t, cancel, func() {
		_, _ = rt.Execute(ctx, &engine.TaskLease{
			Task: engine.Task{ExecutionID: types.ExecutionID("e1"), NodeName: "m/_batch/0"},
			SubgraphPayload: &engine.SubgraphLeasePayload{
				ParentNode:  "m",
				Package:     body,
				PackageHash: hash,
				BatchIndex:  0,
				BatchSize:   1,
				Items:       []any{map[string]any{"n": 1.0}},
			},
		})
	})

	awaitQueueLogLine(t, logs, queueDispatchFailureMarker,
		"WithSubgraphQueueLogger did not reach the per-item backend's local.New")
}
