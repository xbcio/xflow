package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Batch continuations must ride their own asynq queue, and the consumer must
// poll it. Producer-side routing alone is a silent black hole: asynq's default
// server config processes only "default", so batch tasks sent anywhere else are
// enqueued successfully, never delivered, and never reported.
func TestBatchTasksRideTheirOwnQueueAndAreStillConsumed(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	batch := &engine.Task{
		ExecutionID: types.ExecutionID("exec-batch"),
		NodeName:    "m",
		Type:        engine.TaskTypeNodeBatch,
	}
	normal := &engine.Task{
		ExecutionID: types.ExecutionID("exec-normal"),
		NodeName:    "n",
		Type:        engine.TaskTypeNodeExec,
	}
	ctx := context.Background()
	if err := transport.Enqueue(ctx, batch); err != nil {
		t.Fatalf("Enqueue batch: %v", err)
	}
	if err := transport.Enqueue(ctx, normal); err != nil {
		t.Fatalf("Enqueue normal: %v", err)
	}

	// asynq keys its pending list per queue name. Assert on the keys directly:
	// a consumer-side count alone cannot tell the two queues apart.
	if n, _ := server.List("asynq:{" + batchQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("batch queue holds %d tasks, want 1 — the batch task did not "+
			"route to %q", len(n), batchQueueName)
	}
	if n, _ := server.List("asynq:{" + defaultQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("default queue holds %d tasks, want 1 (only the normal task)", len(n))
	}

	seen := make(chan types.ExecutionID, 2)
	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 2},
		func(_ context.Context, task *engine.Task) error {
			seen <- task.ExecutionID
			return nil
		})
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	defer stop()

	got := map[types.ExecutionID]bool{}
	deadline := time.After(10 * time.Second)
	for len(got) < 2 {
		select {
		case id := <-seen:
			got[id] = true
		case <-deadline:
			t.Fatalf("consumer delivered %v, want both exec-batch and exec-normal — "+
				"a queue the server does not poll swallows tasks silently", got)
		}
	}
}

// A weighted split, not a strict one: strict priority lets a steady stream of
// ordinary tasks starve an in-flight map, and asynq reports nothing when it
// happens.
//
// The weights are pinned to their literal values rather than to an ordering,
// because the ordering is not the contract. transport.go's own comment states
// it: "At 8:1 the batch queue keeps roughly 1/9 of consumer throughput." An
// ordering check is green for 8:7 -- batch would take ~47% of consumer slots
// instead of ~11%, defeating the isolation the split exists to provide -- and
// equally green for 8000:1, which is strict priority in all but name, the very
// thing this test is named against. Only the numbers themselves separate those.
//
// The literals are written out here on purpose: importing the constants would
// make both sides of the comparison move together and assert nothing.
func TestConsumerWeightsBothQueuesWithoutStrictPriority(t *testing.T) {
	w := queueWeights()
	if w[defaultQueueName] != 8 {
		t.Errorf("default queue weight = %d, want 8 (see transport.go's 8:1 contract)",
			w[defaultQueueName])
	}
	if w[batchQueueName] != 1 {
		t.Errorf("batch queue weight = %d, want 1; zero or negative makes asynq "+
			"ignore the queue entirely, and anything larger erodes the ~1/9 share "+
			"that keeps a map batch from crowding out ordinary tasks",
			w[batchQueueName])
	}
	// Spelled out as a consequence so a future retune reads what it is trading.
	share := float64(w[batchQueueName]) / float64(w[batchQueueName]+w[defaultQueueName])
	if share < 0.10 || share > 0.12 {
		t.Errorf("batch queue share = %.2f of consumer slots, want ~0.11", share)
	}
}

func TestQueueForRoutesOnlyBatchTasksToTheBatchQueue(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  engine.TaskType
		want string
	}{
		{"batch", engine.TaskTypeNodeBatch, batchQueueName},
		{"exec", engine.TaskTypeNodeExec, defaultQueueName},
		{"resume", engine.TaskTypeNodeResume, defaultQueueName},
		{"group", engine.TaskTypeGroupExec, defaultQueueName},
	} {
		if got := queueFor(&engine.Task{Type: tc.typ}); got != tc.want {
			t.Errorf("queueFor(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := queueFor(nil); got != defaultQueueName {
		t.Errorf("queueFor(nil) = %q, want %q", got, defaultQueueName)
	}
}
