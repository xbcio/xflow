package asynq

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

const suffixTestNS = "local-jack"

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// seenIDs is a goroutine-safe set of execution IDs a handler has received.
type seenIDs struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func newSeenIDs() *seenIDs { return &seenIDs{ids: map[string]struct{}{}} }

func (s *seenIDs) handler(_ context.Context, task *engine.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids[string(task.ExecutionID)] = struct{}{}
	return nil
}

func (s *seenIDs) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

func (s *seenIDs) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

// Mutation note: appending the suffix to the wrong side, or dropping the
// "xflow:" prefix when a suffix is set, must turn this red. The empty-suffix
// rows pin the rolling-upgrade invariant: the default deployment's queue names
// stay byte-identical to v0.0.47's.
func TestQueueNamesSuffix(t *testing.T) {
	legacy := queueNames{}
	if got := legacy.For(&engine.Task{Type: engine.TaskTypeNodeExec}); got != "xflow:default" {
		t.Errorf("legacy exec queue = %q, want xflow:default", got)
	}
	if got := legacy.For(&engine.Task{Type: engine.TaskTypeNodeBatch}); got != "xflow:batch" {
		t.Errorf("legacy batch queue = %q, want xflow:batch", got)
	}

	q := queueNames{suffix: suffixTestNS}
	if got := q.For(&engine.Task{Type: engine.TaskTypeNodeExec}); got != "xflow:default:ns:"+suffixTestNS {
		t.Errorf("suffixed exec queue = %q", got)
	}
	if got := q.For(&engine.Task{Type: engine.TaskTypeNodeBatch}); got != "xflow:batch:ns:"+suffixTestNS {
		t.Errorf("suffixed batch queue = %q", got)
	}
	if got := q.For(nil); got != "xflow:default:ns:"+suffixTestNS {
		t.Errorf("suffixed nil-task queue = %q, want the suffixed default queue", got)
	}

	// Weights and All must describe exactly the queues For can return, with the
	// same weights: a queue For routes to but Weights omits is never consumed.
	w := q.Weights()
	if len(w) != 2 || w["xflow:default:ns:"+suffixTestNS] != defaultQueueWeight ||
		w["xflow:batch:ns:"+suffixTestNS] != batchQueueWeight {
		t.Errorf("suffixed weights = %v", w)
	}
	all := q.All()
	if len(all) != len(w) {
		t.Fatalf("All() = %v, want the same queues as Weights() %v", all, w)
	}
	for _, name := range all {
		if _, ok := w[name]; !ok {
			t.Errorf("All() lists %q which Weights() does not", name)
		}
		if !strings.HasPrefix(name, "xflow:") {
			t.Errorf("queue %q lost the isolation prefix", name)
		}
	}
}

// Mutation note: making WithQueueSuffix a no-op, or having Enqueue/EnqueueDelayed
// keep calling the package-level queueFor, must turn this red — a dedicated
// deployment would then write into the shared deployment's queues, which is the
// exact poisoning this change exists to prevent.
func TestSuffixedProducerWritesOnlyToItsOwnQueues(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	transport := New(srv.Addr(), WithQueueSuffix(suffixTestNS))
	defer func() { _ = transport.Close() }()

	ctx := context.Background()
	if err := transport.Enqueue(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("e-exec"), NodeName: "n", Type: engine.TaskTypeNodeExec,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := transport.Enqueue(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("e-batch"), NodeName: "m", Type: engine.TaskTypeNodeBatch,
	}); err != nil {
		t.Fatalf("Enqueue batch: %v", err)
	}
	if err := transport.EnqueueDelayed(ctx, &engine.Task{
		ExecutionID: types.ExecutionID("e-delayed"), NodeName: "d", Type: engine.TaskTypeNodeExec,
	}, time.Minute); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}

	own := "xflow:default:ns:" + suffixTestNS
	ownBatch := "xflow:batch:ns:" + suffixTestNS
	if n, _ := srv.List("asynq:{" + own + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want 1", own, len(n))
	}
	if n, _ := srv.ZMembers("asynq:{" + own + "}:scheduled"); len(n) != 1 {
		t.Errorf("asynq:{%s}:scheduled holds %d tasks, want 1", own, len(n))
	}
	if n, _ := srv.List("asynq:{" + ownBatch + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want 1", ownBatch, len(n))
	}

	// Asserted over every key, as the isolation tests above do: whichever state
	// a regression writes in, the shared deployment's queues must stay empty.
	for _, key := range srv.Keys() {
		for _, shared := range []string{defaultQueueName, batchQueueName, asynqDefaultQueueName} {
			if strings.HasPrefix(key, "asynq:{"+shared+"}:") {
				t.Errorf("Redis holds %q: a suffixed deployment wrote into the shared queue %q", key, shared)
			}
		}
	}
}

// Mutation note: making Weights() (or All()) ignore the suffix must turn this
// red — a dedicated consumer would poll the shared queue and take the deployed
// SAS's tasks (and vice versa), the cross-consumption half of the poisoning.
// Both directions are asserted: shared->dedicated and dedicated->shared.
func TestSuffixedAndLegacyConsumersDoNotCrossConsume(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	shared := New(srv.Addr())
	defer func() { _ = shared.Close() }()
	dedicated := New(srv.Addr(), WithQueueSuffix(suffixTestNS))
	defer func() { _ = dedicated.Close() }()

	sharedSeen, dedicatedSeen := newSeenIDs(), newSeenIDs()
	stopShared, err := shared.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, sharedSeen.handler)
	if err != nil {
		t.Fatalf("shared StartConsumer: %v", err)
	}
	defer stopShared()
	stopDedicated, err := dedicated.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, dedicatedSeen.handler)
	if err != nil {
		t.Fatalf("dedicated StartConsumer: %v", err)
	}
	defer stopDedicated()

	ctx := context.Background()
	for _, id := range []string{"for-shared-1", "for-shared-2"} {
		if err := shared.Enqueue(ctx, &engine.Task{
			ExecutionID: types.ExecutionID(id), NodeName: "n", Type: engine.TaskTypeNodeExec,
		}); err != nil {
			t.Fatalf("shared Enqueue: %v", err)
		}
	}
	for _, id := range []string{"for-dedicated-1", "for-dedicated-2"} {
		if err := dedicated.Enqueue(ctx, &engine.Task{
			ExecutionID: types.ExecutionID(id), NodeName: "n", Type: engine.TaskTypeNodeExec,
		}); err != nil {
			t.Fatalf("dedicated Enqueue: %v", err)
		}
	}

	// Positive delivery first: the isolation assertions below are only
	// meaningful if each consumer demonstrably works on its own queue.
	waitFor(t, 10*time.Second, "each consumer to receive its own two tasks", func() bool {
		return sharedSeen.has("for-shared-1") && sharedSeen.has("for-shared-2") &&
			dedicatedSeen.has("for-dedicated-1") && dedicatedSeen.has("for-dedicated-2")
	})
	// Let both servers poll a few more times (asynq's task-check interval is one
	// second) so a late cross-delivery would surface rather than race the check.
	time.Sleep(2 * time.Second)

	if n := sharedSeen.size(); n != 2 {
		t.Errorf("shared consumer received %d tasks, want exactly its own 2 (it took the dedicated deployment's work)", n)
	}
	if n := dedicatedSeen.size(); n != 2 {
		t.Errorf("dedicated consumer received %d tasks, want exactly its own 2 (it took the shared deployment's work)", n)
	}
}

// Mutation note: dropping the suffix from only the consumer half (leaving the
// producer suffixed) must turn this red; so must the reverse. A producer and a
// consumer that disagree on queue names strand every task silently, which is
// why the suffix is one transport option feeding all three halves.
func TestMismatchedSuffixStrandsTasks(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	producer := New(srv.Addr(), WithQueueSuffix(suffixTestNS))
	defer func() { _ = producer.Close() }()
	consumer := New(srv.Addr())
	defer func() { _ = consumer.Close() }()

	seen := newSeenIDs()
	stop, err := consumer.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, seen.handler)
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	defer stop()

	if err := producer.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("stranded"), NodeName: "n", Type: engine.TaskTypeNodeExec,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	time.Sleep(2 * time.Second)

	if seen.size() != 0 {
		t.Errorf("an unsuffixed consumer received a task from a suffixed producer")
	}
	own := "xflow:default:ns:" + suffixTestNS
	if n, _ := srv.List("asynq:{" + own + "}:pending"); len(n) != 1 {
		t.Errorf("asynq:{%s}:pending holds %d tasks, want the stranded 1", own, len(n))
	}
}

// statsCollector records the queue names a StatsObserver was called with.
type statsCollector struct {
	mu    sync.Mutex
	names map[string]struct{}
}

func (s *statsCollector) OnQueueStats(q string, _, _, _ int, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.names == nil {
		s.names = map[string]struct{}{}
	}
	s.names[q] = struct{}{}
}

func (s *statsCollector) snapshot() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]struct{}, len(s.names))
	for k := range s.names {
		out[k] = struct{}{}
	}
	return out
}

// Mutation note: leaving the sampler on the package-level queue names (the
// pre-change loop over defaultQueueName/batchQueueName) must turn this red —
// the depth gauge would then describe the shared deployment's queues from a
// dedicated process, so xflow_queue_depth would be wrong exactly where the
// separation matters.
func TestStatsSamplerReportsTheSuffixedQueues(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	transport := New(srv.Addr(), WithQueueSuffix(suffixTestNS))
	defer func() { _ = transport.Close() }()

	obs := &statsCollector{}
	stop, err := transport.StartConsumer(queue.ConsumerConfig{
		Concurrency:   1,
		StatsObserver: obs,
		StatsInterval: 50 * time.Millisecond,
	}, newSeenIDs().handler)
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	defer stop()

	want := queueNames{suffix: suffixTestNS}.All()
	waitFor(t, 5*time.Second, "both suffixed queues to be sampled", func() bool {
		got := obs.snapshot()
		for _, name := range want {
			if _, ok := got[name]; !ok {
				return false
			}
		}
		return true
	})

	got := obs.snapshot()
	if len(got) != len(want) {
		t.Errorf("sampler reported queues %v, want exactly %v", got, want)
	}
	for _, shared := range []string{defaultQueueName, batchQueueName} {
		if _, ok := got[shared]; ok {
			t.Errorf("sampler reported the shared queue %q from a suffixed transport", shared)
		}
	}
}
