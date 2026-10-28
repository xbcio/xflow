//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// Header names the Kafka transport stamps on the records it parks or dead-letters
// (backend/providers/distributed/internal/queue/kafka/message.go). That package is
// internal, so the names are repeated here; a rename there turns these tests red
// instead of silently weakening them.
const (
	tfHdrAttempt = "x-xflow-attempt"
	tfHdrTarget  = "x-xflow-target"
	tfHdrSource  = "x-xflow-source"
	tfHdrError   = "x-xflow-error"
)

// tfNode is the only node the injectors target. Every other task the engine
// dispatches (advance, skip, ...) passes straight through to the real handler.
const tfNode = "n"

// tfCountHandler is the real node handler. calls is how many times the engine
// actually ran the node, which a failure injected in front of the engine must
// leave at exactly the number of successful deliveries.
type tfCountHandler struct {
	typ   string
	calls atomic.Int32
}

func (h *tfCountHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.typ} }

func (h *tfCountHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.calls.Add(1)
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// tfHandlerInjector wraps the task handler the Kafka consumer calls for every
// record (the product's execution.Dispatcher.HandleTask). It counts the
// deliveries of the target task and hands the n-th one back to the transport as
// an error, before the engine has seen it, whenever fail(n) says so.
//
// The existing failure scenarios only fail the node handler. The engine then
// commits the failure itself and returns nil to the transport, so the
// transport's own retry, delay-topic and dead-letter paths are never driven.
// Failing here is what a registry miss, a StateStore error or a lease-acquire
// error looks like to the transport: the handler returns an error.
type tfHandlerInjector struct {
	fail  func(n int32) error
	calls atomic.Int32
}

func (i *tfHandlerInjector) wrap(inner func(context.Context, *engine.Task) error) func(context.Context, *engine.Task) error {
	return func(ctx context.Context, task *engine.Task) error {
		if task.Type != engine.TaskTypeNodeExec || task.NodeName != tfNode {
			return inner(ctx, task)
		}
		if err := i.fail(i.calls.Add(1)); err != nil {
			return err
		}
		return inner(ctx, task)
	}
}

// tfExecutorInjector wraps the executor the dispatcher runs a lease on. It fails
// the first failFirst executions of the target node with a plain error, after
// the dispatcher has already acquired the lease: the dispatcher reads a plain
// error as an unknown outcome (execution.ExecutorFailureUnknown), leaves the
// lease fenced and returns the error to the transport. The real handler is not
// run for those calls, so the node stays Running under a lease nobody commits.
type tfExecutorInjector struct {
	inner     execution.Executor
	failFirst int32
	calls     atomic.Int32
}

func (x *tfExecutorInjector) Execute(ctx context.Context, lease *engine.TaskLease) (engine.TaskResult, error) {
	if lease.Task.NodeName == tfNode && x.calls.Add(1) <= x.failFirst {
		return engine.TaskResult{}, errors.New("e2e: executor response lost after the lease was taken")
	}
	return x.inner.Execute(ctx, lease)
}

// kafkaTFOpts assembles one process whose Kafka retry settings are small enough
// for a test and whose task handler is chosen by the scenario.
type kafkaTFOpts struct {
	// maxRetries is kafka.Config.MaxRetries: redeliveries before dead-lettering.
	maxRetries int
	// maxRetryBackoff caps the delay-topic wait of a retry (the base is 1s, so a
	// small cap makes every retry wait about that long).
	maxRetryBackoff time.Duration
	// leaseTTL is the engine's task lease TTL.
	leaseTTL time.Duration
	// handler builds the task handler the consumer calls, from the engine and the
	// backend's registry.
	handler  func(eng *engine.Engine, reg engine.HandlerRegistry) func(context.Context, *engine.Task) error
	register func(engine.HandlerRegistrar)
}

// startKafkaTFNode is startKafkaE2ENode with the two things that helper cannot
// express: a KafkaConfig carrying MaxRetries/MaxRetryBackoff, and a custom task
// handler. It binds through Backend.BindTaskHandler, which starts the same
// consumer, 1s outbox dispatcher and lease timeout monitor that StartBinding
// starts; only the handler differs, and scenarios build it from the product's
// own execution.Dispatcher. The shared helper file stays untouched.
func startKafkaTFNode(t *testing.T, env *kafkaE2EEnv, o kafkaTFOpts) *kafkaE2ENode {
	t.Helper()
	env.kafkaUsed = true
	b, err := distributed.New(env.addr, nil,
		distributed.WithConcurrency(4),
		distributed.WithOwnedNamespaces([]namespace.Namespace{env.ns}),
		distributed.WithKafkaQueue(distributed.KafkaConfig{
			Brokers: env.brokers, CreateTopics: true, Partitions: e2ePartitions, ReplicationFactor: 1,
			MaxRetries: o.maxRetries, MaxRetryBackoff: o.maxRetryBackoff,
		}))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("registry %T is not a HandlerRegistrar", b.Registry())
	}
	o.register(reg)
	eng := engine.New(b.State(), b.Queue(), engine.WithDefaultLeaseTTL(o.leaseTTL), engine.WithNamespace(env.ns))
	stop, err := b.BindTaskHandler(eng, o.handler(eng, b.Registry()))
	if err != nil {
		t.Fatalf("BindTaskHandler: %v", err)
	}
	t.Cleanup(stop)
	return &kafkaE2ENode{b: b, eng: eng}
}

// submitTF compiles a one-node workflow whose node "n" has the given type and
// submits it.
func submitTF(t *testing.T, env *kafkaE2EEnv, node *kafkaE2ENode, typ string) types.ExecutionID {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "kafka-e2e-transport-failure",
		Nodes: []types.NodeDef{{Name: tfNode, Type: typ}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id, err := node.eng.Submit(env.ctx(t, 90*time.Second), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return id
}

// tfRecord is one record of a deployment topic, with the task it carries.
type tfRecord struct {
	msg  kafka.Message
	exec string
	node string
}

func (r tfRecord) header(key string) string {
	for _, h := range r.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// readTopicRecords returns every record the topic holds now, partition by
// partition. It reads without a consumer group, so it creates no group and
// commits nothing; the env cleanup deletes the topic as usual.
func readTopicRecords(t *testing.T, env *kafkaE2EEnv, topic string) []tfRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := &kafka.Client{Addr: kafka.TCP(env.brokers...), Timeout: 10 * time.Second}
	var out []tfRecord
	for p := 0; p < e2ePartitions; p++ {
		var first, last int64
		pollUntil(ctx, t, "offsets of "+topic, func() (bool, error) {
			resp, err := cl.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: cl.Addr, Topics: map[string][]kafka.OffsetRequest{
				topic: {kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p)},
			}})
			if err != nil {
				return false, err
			}
			for _, po := range resp.Topics[topic] {
				if errors.Is(po.Error, kafka.NotLeaderForPartition) || errors.Is(po.Error, kafka.LeaderNotAvailable) {
					return false, nil
				}
				if po.Error != nil {
					return false, po.Error
				}
				if po.Partition == p {
					first, last = po.FirstOffset, po.LastOffset
				}
			}
			return true, nil
		})
		if last <= first {
			continue
		}
		r := kafka.NewReader(kafka.ReaderConfig{Brokers: env.brokers, Topic: topic, Partition: p, MinBytes: 1, MaxBytes: 1 << 20})
		if err := r.SetOffset(first); err != nil {
			_ = r.Close()
			t.Fatalf("set offset %d on %s/%d: %v", first, topic, p, err)
		}
		for off := first; off < last; off++ {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				_ = r.Close()
				t.Fatalf("read %s/%d at offset %d: %v", topic, p, off, err)
			}
			rec := tfRecord{msg: msg}
			var task struct {
				ExecutionID string `json:"execution_id"`
				NodeName    string `json:"node_name"`
			}
			if json.Unmarshal(msg.Value, &task) == nil {
				rec.exec, rec.node = task.ExecutionID, task.NodeName
			}
			out = append(out, rec)
		}
		_ = r.Close()
	}
	return out
}

// recordsOf keeps the records that carry a task of the given execution's node "n".
func recordsOf(recs []tfRecord, id types.ExecutionID) []tfRecord {
	var out []tfRecord
	for _, r := range recs {
		if r.exec == string(id) && r.node == tfNode {
			out = append(out, r)
		}
	}
	return out
}

// tfHandlerFor builds the consumer's task handler: the product dispatcher with
// inj in front of it.
func tfHandlerFor(inj *tfHandlerInjector) func(*engine.Engine, engine.HandlerRegistry) func(context.Context, *engine.Task) error {
	return func(eng *engine.Engine, reg engine.HandlerRegistry) func(context.Context, *engine.Task) error {
		return inj.wrap(execution.NewEmbeddedDispatcher(eng, reg).HandleTask)
	}
}

// TestKafkaQueueE2E_TransportFailure drives the Kafka consumer's own failure
// paths (retry through the delay topic, dead-lettering) with the real engine,
// Redis and Kafka behind them.
//
// What it guards: process() in the transport's consumer.go retries any handler
// error through the delay topic and dead-letters a permanent one, but until now
// only a bare transport with a fake handler had exercised that. The engine
// commits its own node failures and returns nil, so the E2E failure scenarios
// never reach those paths.
//
// How the failure is injected: the consumer's task handler is wrapped (see
// tfHandlerInjector), so the error reaches the transport before the engine has
// seen the task, exactly as a StateStore or registry error would. No product
// code is changed.
func TestKafkaQueueE2E_TransportFailure(t *testing.T) {
	t.Run("transient_before_engine_then_success", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &tfCountHandler{typ: "test.e2e.tf.transient"}
		// The first two deliveries fail with an ordinary error, the third goes to
		// the engine. MaxRetries leaves room for them, and the 300ms cap keeps the
		// two parked waits (base 1s) to 300ms each.
		inj := &tfHandlerInjector{fail: func(n int32) error {
			if n <= 2 {
				return errors.New("e2e: transient failure before the engine")
			}
			return nil
		}}
		node := startKafkaTFNode(t, env, kafkaTFOpts{
			maxRetries: 5, maxRetryBackoff: 300 * time.Millisecond, leaseTTL: time.Minute,
			handler:  tfHandlerFor(inj),
			register: func(r engine.HandlerRegistrar) { r.RegisterGlobal(h.typ, h) },
		})
		id := submitTF(t, env, node, h.typ)

		ctx := env.ctx(t, 60*time.Second)
		res := waitForCompletion(ctx, t, node.b.State(), id, tfNode)
		snap, err := node.b.State().GetNode(ctx, id, tfNode)
		if err != nil || snap == nil {
			t.Fatalf("GetNode(%s): %v", tfNode, err)
		}
		if res.Status != types.ExecutionStatusSuccess || snap.Status != types.NodeStatusSuccess {
			t.Fatalf("execution=%s node=%s, want success/success", res.Status, snap.Status)
		}
		// Two failures in front of the engine and one delivery that ran: the real
		// handler ran once and the node took its lease once. A late redelivery
		// would show up in either count.
		if got := settle(ctx, t, &inj.calls, 2*time.Second); got != 3 {
			t.Fatalf("deliveries at the transport boundary = %d, want 3 (two injected failures, then one success)", got)
		}
		if got := h.calls.Load(); got != 1 || snap.Attempt != 1 {
			t.Fatalf("handler ran %d times, node attempt %d, want 1 and 1: the failures never reached the engine", got, snap.Attempt)
		}

		// Proof that the retries travelled through the delay topic rather than
		// the engine's own retry: two parked copies, attempt 1 then 2, each aimed
		// back at the default topic.
		parked := recordsOf(readTopicRecords(t, env, env.topics()[2]), id)
		if len(parked) != 2 {
			t.Fatalf("delay topic holds %d records of the task, want 2 (one per injected failure)", len(parked))
		}
		attempts := map[string]bool{}
		for _, r := range parked {
			attempts[r.header(tfHdrAttempt)] = true
			if got := r.header(tfHdrTarget); got != env.topics()[0] {
				t.Errorf("parked copy targets %q, want the default topic %q", got, env.topics()[0])
			}
		}
		if !attempts["1"] || !attempts["2"] {
			t.Errorf("parked copies carry attempts %v, want 1 and 2", attempts)
		}
		assertTopicEmpty(t, env, env.topics()[3], "dlq")
	})

	// permanent_before_engine_goes_to_dlq also records, over a bounded window and
	// without any intervention, what becomes of the execution. The final block of
	// assertions DOCUMENTS the behaviour observed on this code; it is not a
	// contract and nothing promises it stays this way. If a later change makes a
	// stranded task recoverable (a sweeper that sees tasks with no lease, a
	// dead-letter consumer, ...), those assertions are the ones expected to turn
	// red, and they should then be rewritten, not worked around.
	t.Run("permanent_before_engine_goes_to_dlq", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &tfCountHandler{typ: "test.e2e.tf.permanent"}
		inj := &tfHandlerInjector{fail: func(int32) error {
			return types.NewPermanentError("e2e.tf.permanent", "permanent failure before the engine")
		}}
		node := startKafkaTFNode(t, env, kafkaTFOpts{
			maxRetries: 5, maxRetryBackoff: 300 * time.Millisecond, leaseTTL: 3 * time.Second,
			handler:  tfHandlerFor(inj),
			register: func(r engine.HandlerRegistrar) { r.RegisterGlobal(h.typ, h) },
		})
		rdb := redis.NewClient(&redis.Options{Addr: env.addr})
		t.Cleanup(func() { _ = rdb.Close() })
		id := submitTF(t, env, node, h.typ)

		ctx := env.ctx(t, 90*time.Second)
		var dlq []tfRecord
		pollUntil(ctx, t, "the permanent failure to be dead-lettered", func() (bool, error) {
			dlq = recordsOf(readTopicRecords(t, env, env.topics()[3]), id)
			return len(dlq) > 0, nil
		})
		// Exactly one dead-letter record, taken from the default topic, carrying
		// the transport's reason; no retry copy was ever parked; and the
		// engine saw neither the task nor a retry.
		if len(dlq) != 1 {
			t.Fatalf("dlq holds %d records of the task, want 1", len(dlq))
		}
		if got := dlq[0].header(tfHdrSource); got != env.topics()[0] {
			t.Errorf("dead-letter source = %q, want the default topic %q", got, env.topics()[0])
		}
		if got := dlq[0].header(tfHdrError); !strings.Contains(got, "permanent failure before the engine") {
			t.Errorf("dead-letter reason = %q, want it to carry the handler error", got)
		}
		if n := len(recordsOf(readTopicRecords(t, env, env.topics()[2]), id)); n != 0 {
			t.Errorf("delay topic holds %d records of the task, want 0: a permanent error is not retried", n)
		}

		// Observation window: 15s, no intervention. Sampled every 2s; a sweep is
		// driven by hand at the end, since this stack runs no background sweeper.
		const window = 15 * time.Second
		start := time.Now()
		var last tfObservation
		for {
			last = observeTF(ctx, t, env, node, rdb, id)
			t.Logf("t+%2ds %s | real handler calls=%d, transport deliveries=%d",
				int(time.Since(start).Seconds()), last, h.calls.Load(), inj.calls.Load())
			if !last.stranded() {
				t.Errorf("at t+%s the execution is no longer stranded: %s", time.Since(start).Round(time.Second), last)
				break
			}
			if time.Since(start) >= window {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("observation window cut short: %v", ctx.Err())
			case <-time.After(2 * time.Second):
			}
		}
		if swept := newTFSweeper(node).SweepOnce(ctx); swept != 0 {
			t.Errorf("a lease sweep reclaimed %d leases, want 0: there is no lease to reclaim", swept)
		}
		if got := inj.calls.Load(); got != 1 {
			t.Errorf("transport deliveries = %d, want 1: nothing redelivered the dead-lettered task", got)
		}
		if got := h.calls.Load(); got != 0 {
			t.Errorf("real handler ran %d times, want 0: the engine never saw the task", got)
		}
	})

	// transient_after_lease_is_reclaimed fails AFTER the engine took the lease:
	// the injection sits on the executor the dispatcher runs the lease on (see
	// tfExecutorInjector), so the node is Running under a live lease when the
	// error goes back to the transport. Wrapping HandleTask itself after the real
	// call would not model this: the engine would already have committed the
	// result and there would be nothing left to reclaim.
	//
	// Expected, and what the test checks: the transport's retry is useless here
	// (the redelivered task finds the lease still held and is dropped as a
	// duplicate), the execution stays Running, and only a lease sweep revokes the
	// lease and re-delivers the task through the outbox. The embedded stack runs
	// no sweeper, so the test drives the product's own LeaseSweeper by hand, the
	// component that ControlPlane.Start runs in the server topology.
	t.Run("transient_after_lease_is_reclaimed", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &tfCountHandler{typ: "test.e2e.tf.afterlease"}
		var xi *tfExecutorInjector
		// deliveries only counts: its fail never fails, it just sees every delivery
		// of the target task at the transport boundary.
		deliveries := &tfHandlerInjector{fail: func(int32) error { return nil }}
		node := startKafkaTFNode(t, env, kafkaTFOpts{
			maxRetries: 5, maxRetryBackoff: 300 * time.Millisecond, leaseTTL: 2 * time.Second,
			handler: func(eng *engine.Engine, reg engine.HandlerRegistry) func(context.Context, *engine.Task) error {
				xi = &tfExecutorInjector{inner: execution.NewRunner(reg), failFirst: 1}
				return deliveries.wrap(execution.NewDispatcher(eng, xi).HandleTask)
			},
			register: func(r engine.HandlerRegistrar) { r.RegisterGlobal(h.typ, h) },
		})
		rdb := redis.NewClient(&redis.Options{Addr: env.addr})
		t.Cleanup(func() { _ = rdb.Close() })
		id := submitTF(t, env, node, h.typ)

		ctx := env.ctx(t, 60*time.Second)
		// The failure happened and the transport parked the task for a retry.
		pollUntil(ctx, t, "the retry copy on the delay topic", func() (bool, error) {
			return len(recordsOf(readTopicRecords(t, env, env.topics()[2]), id)) > 0, nil
		})
		// Wait for the retry copy to come back to the transport (a second delivery
		// of the task) and for the count to settle, so the redelivery has been
		// handled before the state is read. It finds the lease still held and is
		// dropped: the executor is not called again.
		pollUntil(ctx, t, "the retried task to be redelivered", func() (bool, error) {
			return deliveries.calls.Load() >= 2, nil
		})
		settle(ctx, t, &deliveries.calls, time.Second)
		if got := xi.calls.Load(); got != 1 {
			t.Fatalf("executor calls = %d after the redelivery, want 1: the redelivered task must be dropped while the lease is held", got)
		}
		// The node still holds its lease and (unlike scenario 2) the sweeper's
		// discovery finds it. That doubles as the positive control for the
		// sweeper check used in scenario 2.
		before := observeTF(ctx, t, env, node, rdb, id)
		t.Logf("before the sweep: %s | real handler calls=%d, executor calls=%d", before, h.calls.Load(), xi.calls.Load())
		if before.execStatus != types.ExecutionStatusRunning || before.nodeStatus != types.NodeStatusRunning || before.leaseToken == "" || !before.sweepSeesIt {
			t.Fatalf("before the sweep: %s, want a running execution with a running, leased node the sweeper can see", before)
		}
		if h.calls.Load() != 0 {
			t.Fatalf("real handler ran %d times before the sweep, want 0", h.calls.Load())
		}

		// The lease TTL is 2s; the sweep reclaims once it has passed.
		sweeper := newTFSweeper(node)
		reclaimed := 0
		pollUntil(ctx, t, "the lease sweep to reclaim the lease", func() (bool, error) {
			reclaimed += sweeper.SweepOnce(ctx)
			return reclaimed > 0, nil
		})
		res := waitForCompletion(ctx, t, node.b.State(), id, tfNode)
		if res.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution = %s after the reclaim, want success", res.Status)
		}
		if got := settle(ctx, t, &xi.calls, 2*time.Second); got != 2 {
			t.Fatalf("executor calls = %d, want 2 (the lost one, then the reclaimed redelivery)", got)
		}
		if got := h.calls.Load(); got != 1 {
			t.Fatalf("real handler ran %d times, want 1", got)
		}
		assertTopicEmpty(t, env, env.topics()[3], "dlq")
	})
}

// tfObservation is what the scheduler's own state says about one execution at
// one moment: the execution and node status, whether the node holds a lease, the
// size of the execution's durable outbox, and how many expired leases the lease
// sweeper's discovery call (StateStore.ListExpiredLeases) would report for it.
type tfObservation struct {
	execStatus  types.ExecutionStatus
	nodeStatus  types.NodeStatus
	leaseToken  string
	attempt     int
	outboxReady int64
	outboxBody  int64
	outboxDead  int64
	sweepSeesIt bool
	nodeSeen    bool
}

// observeTF reads the state without changing it. The sweeper check asks for the
// leases expired as of one hour from now, so a lease of any age would be listed;
// an empty answer for this execution means the sweeper has nothing to reclaim.
func observeTF(ctx context.Context, t *testing.T, env *kafkaE2EEnv, node *kafkaE2ENode, rdb *redis.Client, id types.ExecutionID) tfObservation {
	t.Helper()
	var o tfObservation
	st := node.b.State()
	ex, err := st.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if ex != nil {
		o.execStatus = ex.Status
	}
	ns, err := st.GetNode(ctx, id, tfNode)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if ns != nil {
		o.nodeSeen = true
		o.nodeStatus, o.leaseToken, o.attempt = ns.Status, string(ns.LeaseToken), ns.Attempt
	}
	key := func(suffix string) string { return fmt.Sprintf("xflow:ns:%s:exec:{%s}:%s", env.ns, id, suffix) }
	o.outboxReady = rdb.ZCard(ctx, key("outbox:ready")).Val()
	o.outboxBody = rdb.HLen(ctx, key("outbox:body")).Val()
	o.outboxDead = rdb.ZCard(ctx, key("outbox:dead")).Val()
	expired, err := st.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredLeases: %v", err)
	}
	for _, l := range expired {
		if l.ExecutionID == id {
			o.sweepSeesIt = true
		}
	}
	return o
}

func (o tfObservation) String() string {
	return fmt.Sprintf("execution=%q nodeSnapshot=%t node=%q lease=%t attempt=%d outbox{ready=%d body=%d dead=%d} sweeperSeesIt=%t",
		o.execStatus, o.nodeSeen, o.nodeStatus, o.leaseToken != "", o.attempt, o.outboxReady, o.outboxBody, o.outboxDead, o.sweepSeesIt)
}

// stranded reports the shape observed after a permanent failure in front of the
// engine: the execution is still running, but the node has no snapshot and no
// lease (it was never acquired), the durable outbox has been acked down to
// nothing, and the lease sweeper's discovery finds nothing for it.
func (o tfObservation) stranded() bool {
	return o.execStatus == types.ExecutionStatusRunning && !o.nodeSeen && o.leaseToken == "" &&
		o.outboxReady == 0 && o.outboxBody == 0 && o.outboxDead == 0 && !o.sweepSeesIt
}

// newTFSweeper builds the product's lease sweeper over the node's state and
// engine, the way service/control.NewControlPlane does. The embedded stack that
// startKafkaTFNode builds (the one sdk/xflow's NewCluster builds) runs no
// sweeper at all, so a test that wants a sweep has to drive one by hand.
func newTFSweeper(node *kafkaE2ENode) *control.LeaseSweeper {
	return control.NewLeaseSweeper(node.b.State(), node.eng, control.LeaseSweeperConfig{})
}
