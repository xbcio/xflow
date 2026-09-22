package asynq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Asynq addresses a queue as asynq:{<queue>}:* and keeps its bookkeeping under
// unprefixed asynq:* keys, so a queue name is the only namespace this
// application has. asynq's own bare name "default" is shared with every other
// asynq application in the same Redis database, which is the case that matters
// when a host application that already uses asynq mounts an xflow server in the
// same process: both asynq servers poll asynq:{default}, and whichever takes a
// task it has no handler for returns asynq's *retryable* "handler not found"
// error, so the task churns through asynq:{default}:retry instead of reaching
// its owner.
//
// These tests pin the isolation at both ends: nothing this package produces may
// land in the bare queue, and the consumer may not read it.
//
// asynqDefaultQueueName is asynq's own name, deliberately written here rather
// than shared with the package: this package must have no constant for it, since
// having one is what a later reader would be tempted to poll.
const asynqDefaultQueueName = "default"

// The invariant behind the fix, asserted over the whole routing table rather
// than over the two names that happen to exist today: a task routed by queueFor
// and a queue polled by the consumer must both live under the prefix. Written
// against the constants so adding a fourth queue without a prefix fails here.
func TestQueueNamesCarryTheIsolationPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		task *engine.Task
	}{
		{"nil", nil},
		{"exec", &engine.Task{Type: engine.TaskTypeNodeExec}},
		{"resume", &engine.Task{Type: engine.TaskTypeNodeResume}},
		{"group", &engine.Task{Type: engine.TaskTypeGroupExec}},
		{"batch", &engine.Task{Type: engine.TaskTypeNodeBatch}},
	} {
		got := queueFor(tc.task)
		if !strings.HasPrefix(got, "xflow:") {
			t.Errorf("queueFor(%s) = %q, want an %q-prefixed queue: an unprefixed "+
				"name collides with every other asynq application in the same "+
				"Redis database", tc.name, got, "xflow:")
		}
	}

	for _, name := range []string{defaultQueueName, batchQueueName} {
		if !strings.HasPrefix(name, "xflow:") {
			t.Errorf("queue %q lacks the %q prefix", name, "xflow:")
		}
	}

	// The consumer must not touch asynq's bare queue at all: polling it is what
	// lets a host application's own tasks be pulled into xflow.
	w := queueWeights()
	if _, ok := w[asynqDefaultQueueName]; ok {
		t.Errorf("default consumer polls %q (weights %v): a consumer that reads it "+
			"accepts tasks belonging to any other asynq application in this Redis "+
			"database", asynqDefaultQueueName, w)
	}
	if _, ok := w[defaultQueueName]; !ok {
		t.Errorf("default consumer does not poll %q (weights %v)", defaultQueueName, w)
	}
}

// The producer half, asserted against Redis rather than against queueFor: the
// keys that actually appear are the contract. A task enqueued without an
// explicit Queue option is what asynq puts in its own "default" queue, so the
// absence of asynq:{default}:pending is the whole property under test.
func TestProducerNeverEnqueuesToAsynqDefaultQueue(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	transport := New(srv.Addr())
	defer func() { _ = transport.Close() }()

	ctx := context.Background()
	if err := transport.Enqueue(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("exec-normal"), NodeName: "n", Type: engine.TaskTypeNodeExec,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := transport.Enqueue(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("exec-batch"), NodeName: "m", Type: engine.TaskTypeNodeBatch,
	}); err != nil {
		t.Fatalf("Enqueue batch: %v", err)
	}
	if err := transport.EnqueueDelayed(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("exec-delayed"), NodeName: "d", Type: engine.TaskTypeNodeExec,
	}, time.Minute); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}

	// Asserted over every key in the database rather than over :pending, because
	// enqueue paths differ by state — Enqueue lands in :pending, EnqueueDelayed
	// in :scheduled — and a state nobody thought to list is exactly where a
	// regression would hide. The property is "this application wrote nothing
	// under asynq's shared bare name", in any state.
	for _, key := range srv.Keys() {
		if strings.HasPrefix(key, "asynq:{"+asynqDefaultQueueName+"}:") {
			t.Errorf("Redis holds %q after enqueueing three tasks: this application "+
				"is writing into the queue every other asynq application in this "+
				"database reads", key)
		}
	}

	if n, _ := srv.List("asynq:{" + defaultQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want 1 (the immediate task)",
			defaultQueueName, len(n))
	}
	// scheduled is a ZSET, not a list; List() on it returns an error and no
	// members, which would make this assertion vacuously true.
	if n, _ := srv.ZMembers("asynq:{" + defaultQueueName + "}:scheduled"); len(n) != 1 {
		t.Errorf("asynq:{%s}:scheduled holds %d tasks, want 1 (the delayed task)",
			defaultQueueName, len(n))
	}
	if n, _ := srv.List("asynq:{" + batchQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want 1", batchQueueName, len(n))
	}
}

// The consumer half, and the failure this whole change is about: a host
// application's own task sitting in the shared bare queue. It must still be
// there afterwards, and the xflow handler must never have seen it.
func TestConsumerLeavesForeignDefaultQueueAlone(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	// The host application: a plain asynq client with no Queue option, so its
	// task lands in asynq's own default queue — exactly what a bare
	// defaultQueueName would make xflow share with it.
	host := asynqlib.NewClient(asynqlib.RedisClientOpt{Addr: srv.Addr()})
	defer func() { _ = host.Close() }()
	if _, err := host.Enqueue(asynqlib.NewTask("host:job", []byte(`{}`))); err != nil {
		t.Fatalf("host enqueue: %v", err)
	}

	transport := New(srv.Addr())
	defer func() { _ = transport.Close() }()

	called := make(chan struct{}, 1)
	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1},
		func(_ context.Context, _ *engine.Task) error {
			called <- struct{}{}
			return nil
		})
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	defer stop()

	// Give the consumer long enough to poll every queue it was configured with
	// several times; asynq's default task-check interval is one second.
	time.Sleep(2 * time.Second)

	if n, _ := srv.List("asynq:{" + asynqDefaultQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want the host's 1 still waiting: "+
			"this consumer took the embedding application's task out of the shared "+
			"bare queue", asynqDefaultQueueName, len(n))
	}
	select {
	case <-called:
		t.Error("the xflow handler ran for the host application's task type")
	default:
	}
}

// The other direction, with a real host consumer rather than a host client:
// inside one process, an asynq server configured the way a host application
// configures its own — no Queues, which asynq resolves to its bare "default"
// (server.go defaultQueueConfig) — must not pull an xflow task out of the
// xflow queue, and must never run a handler for it.
//
// This is the embedding topology that motivated the prefix, asserted from the
// host's side. The test above only proves xflow's consumer leaves the host's
// tasks alone; without this one, nothing would fail if a future change made
// xflow produce into a queue a plain asynq server polls.
func TestHostAsynqServerDoesNotStealXFloTasks(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	// The host application's own asynq server, default config: it polls
	// asynq's bare "default" and nothing else.
	hostMux := asynqlib.NewServeMux()
	hostRan := make(chan string, 4)
	hostMux.HandleFunc("host:job", func(_ context.Context, at *asynqlib.Task) error {
		hostRan <- at.Type()
		return nil
	})
	hostSrv := asynqlib.NewServer(asynqlib.RedisClientOpt{Addr: srv.Addr()},
		asynqlib.Config{Concurrency: 2})
	if err := hostSrv.Start(hostMux); err != nil {
		t.Fatalf("host server start: %v", err)
	}
	defer hostSrv.Shutdown()

	// xflow's producer, in the same process against the same Redis.
	transport := New(srv.Addr())
	defer func() { _ = transport.Close() }()

	if err := transport.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("exec-embedding"), NodeName: "n",
		Type: engine.TaskTypeNodeExec,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Long enough for the host server to poll its queue several times; asynq's
	// default task-check interval is one second.
	time.Sleep(2 * time.Second)

	if n, _ := srv.List("asynq:{" + defaultQueueName + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want xflow's 1 still waiting: "+
			"the hosting application's own asynq server took it", defaultQueueName, len(n))
	}
	select {
	case got := <-hostRan:
		t.Errorf("the host server ran a handler for xflow's task type %q", got)
	default:
	}
}
