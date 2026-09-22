package distributed

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Two assertions about this package's own hop into the Asynq transport. The
// queue names themselves are pinned in the transport's package; what is pinned
// here is that the real backend, constructed the way a host constructs it,
// produces to them.
//
// This is not redundant with the transport's tests. The transport is only
// reachable through distributed.New, and nothing else in this package asserts on
// the queue a task lands in: a construction path that built the transport
// against a different queue set would leave every other test in the repository
// green. It matters most in the embedding case — a host application running its
// own asynq work in the same process and the same Redis, which is the deployment
// that motivated the prefixed names.

// asynqDefaultQueue is asynq's own bare queue name, spelled out rather than
// imported: this package must have no constant for it, because a constant is
// what a later reader would be tempted to enqueue to. It is the name a host
// application's own tasks live under, and the one xflow stays out of.
const asynqDefaultQueue = "default"

// The prefixed queue is what the real backend produces to. A regression that
// reverted the name, or dropped the explicit Queue option so the task fell
// through to asynq's default, shows up here as a key this test forbids.
func TestBackendEnqueuesToThePrefixedQueueNotAsynqDefault(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer mr.Close()

	b, err := New(mr.Addr(), nil, WithConsumer(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer b.nonConsumerStop()()

	task := &engine.Task{
		ExecutionID: types.ExecutionID("queue-name-probe"),
		NodeName:    "n",
		Type:        engine.TaskTypeNodeExec,
	}
	if err := b.Queue().Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Queue().Enqueue() error = %v", err)
	}

	for _, key := range mr.Keys() {
		if strings.HasPrefix(key, "asynq:{"+asynqDefaultQueue+"}:") {
			t.Errorf("Redis holds %q: the backend produced into asynq's bare %q "+
				"queue, which a host application embedding this server shares",
				key, asynqDefaultQueue)
		}
	}
	if n, _ := mr.List("asynq:{xflow:default}:pending"); len(n) != 1 {
		t.Errorf("asynq:{xflow:default}:pending holds %d tasks, want 1", len(n))
	}
}

// The batch queue is reached through the same backend and must be prefixed too:
// it is a second queue name and therefore a second chance to collide. Both are
// asserted so a fix applied to one and not the other fails.
func TestBackendRoutesBatchTasksToThePrefixedBatchQueue(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer mr.Close()

	b, err := New(mr.Addr(), nil, WithConsumer(false))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer b.nonConsumerStop()()

	if err := b.Queue().Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("batch-probe"),
		NodeName:    "m",
		Type:        engine.TaskTypeNodeBatch,
	}); err != nil {
		t.Fatalf("Queue().Enqueue() error = %v", err)
	}

	if n, _ := mr.List("asynq:{xflow:batch}:pending"); len(n) != 1 {
		t.Errorf("asynq:{xflow:batch}:pending holds %d tasks, want 1: batch "+
			"continuations did not land in the prefixed batch queue", len(n))
	}
	for _, key := range mr.Keys() {
		if strings.HasPrefix(key, "asynq:{"+asynqDefaultQueue+"}:") {
			t.Errorf("Redis holds %q: a batch task reached asynq's bare queue", key)
		}
	}
}
