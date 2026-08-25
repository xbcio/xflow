package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// namespaceRecorder records the namespace each observation was filed under.
//
// It reads the namespace exactly the way observability/metrics does —
// namespace.FromContext on the ctx the callback was handed — so a sample this
// recorder files under "default" is a sample the real observer would label
// "default" too.
//
// Counts, not a sample list: the discard arm reports buffer_overflow on every
// message it sheds at cap, which is hundreds of thousands of calls in a
// sub-second window. Counting per namespace keeps that bounded while still
// distinguishing "all samples right" from "most samples right".
type namespaceRecorder struct {
	mu   sync.Mutex
	seen map[string]map[namespace.Namespace]int
}

func newNamespaceRecorder() *namespaceRecorder {
	return &namespaceRecorder{seen: make(map[string]map[namespace.Namespace]int)}
}

func (r *namespaceRecorder) record(callback string, ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byNS := r.seen[callback]
	if byNS == nil {
		byNS = make(map[namespace.Namespace]int)
		r.seen[callback] = byNS
	}
	byNS[namespace.FromContext(ctx)]++
}

func (r *namespaceRecorder) namespacesFor(callback string) map[namespace.Namespace]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[namespace.Namespace]int, len(r.seen[callback]))
	for ns, n := range r.seen[callback] {
		out[ns] = n
	}
	return out
}

func (r *namespaceRecorder) OnMessageDiscarded(ctx context.Context, _, _ string) {
	r.record("OnMessageDiscarded", ctx)
}
func (r *namespaceRecorder) OnMessageDeadLettered(ctx context.Context, _, _ string) {
	r.record("OnMessageDeadLettered", ctx)
}
func (r *namespaceRecorder) OnConsumerLag(ctx context.Context, _ string, _ int, _ int64, _ time.Time) {
	r.record("OnConsumerLag", ctx)
}
func (r *namespaceRecorder) OnConsumptionBlocked(ctx context.Context, _ string, _ int, _ bool) {
	r.record("OnConsumptionBlocked", ctx)
}
func (r *namespaceRecorder) OnBatchFlushed(ctx context.Context, _, _ string, _ int) {
	r.record("OnBatchFlushed", ctx)
}
func (r *namespaceRecorder) OnBatchFlushOutcome(ctx context.Context, _, _, _ string) {
	r.record("OnBatchFlushOutcome", ctx)
}
func (r *namespaceRecorder) OnBatchAdmission(ctx context.Context, _, _, _ string) {
	r.record("OnBatchAdmission", ctx)
}

// TestKafkaAggregateReportsUnderTheActivationNamespace pins that the aggregate
// trigger's observations are filed under the namespace the trigger was
// activated in.
//
// Three call sites reached for context.Background() instead of the activation
// context: the per-partition attempt context that carries every flush
// observation, the overflow discard report, and the backpressure report.
// observability/metrics reads the namespace off the ctx for every label set
// (withNamespace), so all three were filed under namespace.Default regardless
// of who the trigger belonged to.
//
// For the counters that is a mislabel. For OnConsumptionBlocked it is worse
// than a mislabel: that callback drives a Set gauge, so two namespaces
// consuming a topic of the same name write the same series, the later write
// wins, and the survivor looks authoritative. A tenant whose partition has
// stopped fetching reads as healthy because another tenant's healthy partition
// overwrote the sample.
//
// Reaching for Background was not gratuitous — these paths must not be cut
// short when the activation context is canceled, and a partition aggregator
// drains on its own schedule. The fix is context.WithoutCancel, which keeps the
// values and drops the Done channel; this test asserts the values arrived and
// TestKafkaAggregateBlockedPartitionStillCloses covers the shutdown side.
func TestKafkaAggregateReportsUnderTheActivationNamespace(t *testing.T) {
	const tenant = namespace.Namespace("tenant-b")

	rec := newNamespaceRecorder()
	SetObserver(rec)
	t.Cleanup(func() { SetObserver(nil) })

	// Both arms use a downstream that never returns, so the partition fills to
	// its retained bound and stays there. That is what reaches the overflow and
	// backpressure reports; the flush reports are reached on the way in.
	activate := func(t *testing.T, onOverflow string, partition int) {
		t.Helper()

		consumer := newUnboundedProducerConsumer("t", partition)
		orig := newConsumer
		newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
		defer func() { newConsumer = orig }()

		release := make(chan struct{})
		rt := triggertest.NewFakeRuntime()
		rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
			select {
			case <-release:
				return "exec-1", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})

		tr := New().
			Brokers("localhost:9092").
			Topic("t").
			Group("g").
			AggregateByPartition(4, 50*time.Millisecond)
		if onOverflow == onOverflowBlock {
			tr = tr.BlockOnOverflow()
		}
		// The whole point: activate under a namespace that is NOT the default.
		ctx := namespace.WithNamespace(context.Background(), tenant)
		sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
			WorkflowID: "wf-ns",
			NodeName:   "kafka",
			Params:     tr.RawParams().(map[string]any),
			Runtime:    rt,
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(600 * time.Millisecond)
		close(release)
		_ = sub.Close(context.Background())
		_ = consumer.Close()
	}

	// discard reaches OnMessageDiscarded("buffer_overflow"); block reaches
	// OnConsumptionBlocked. Both reach the flush reports.
	activate(t, onOverflowDiscard, 0)
	activate(t, onOverflowBlock, 1)

	// Each of these must have fired at least once under the tenant, or the
	// assertion below has nothing to look at and passes against a trigger that
	// reported nothing at all.
	for _, callback := range []string{
		"OnBatchFlushed",
		"OnBatchFlushOutcome",
		"OnMessageDiscarded",
		"OnConsumptionBlocked",
	} {
		got := rec.namespacesFor(callback)
		if got[tenant] == 0 {
			t.Fatalf("%s produced no sample under %q (saw %v); the assertion below "+
				"would prove nothing about it, because a path that never reported "+
				"has no wrongly-labelled sample to find. The harness did not reach "+
				"the path.", callback, tenant, got)
		}
		// Assert that NO other namespace appears, not that the right one does. A
		// path reporting N times from the activation context and once from
		// Background is exactly the shape of the defect, and "the tenant shows up"
		// cannot see it.
		for ns, n := range got {
			if ns != tenant {
				t.Errorf("%s filed %d of %d samples under namespace %q instead of %q. "+
					"The report was built from a context that carries no namespace, so "+
					"the observation is attributed to the wrong tenant — and for the "+
					"Set-based gauges it overwrites that tenant's series.",
					callback, n, n+got[tenant], ns, tenant)
			}
		}
	}
}
