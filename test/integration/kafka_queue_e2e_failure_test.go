//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// e2eFailHandler fails its first failBefore calls with a transient error (every
// call when failBefore < 0), or with a permanent error when permanent is set,
// and succeeds afterwards. calls counts every Execute, which is the number of
// times the engine delivered the node, whatever the transport did.
type e2eFailHandler struct {
	typ        string
	failBefore int32
	permanent  bool
	calls      atomic.Int32
}

func (h *e2eFailHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.typ} }

func (h *e2eFailHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	n := h.calls.Add(1)
	switch {
	case h.permanent:
		return nil, types.NewPermanentError("e2e.permanent", "permanent e2e failure")
	case h.failBefore < 0 || n <= h.failBefore:
		return nil, types.NewTransientError("e2e.transient", "transient e2e failure")
	}
	return &types.Output{Data: map[string]any{"attempt": n}}, nil
}

// runE2EFail runs a one-node workflow around h with the given retry settings and
// returns the terminal result plus the node's final snapshot.
func runE2EFail(t *testing.T, env *kafkaE2EEnv, h *e2eFailHandler, retry *types.RetrySettings) (types.Result, *engine.NodeSnapshot) {
	t.Helper()
	node := startKafkaE2ENode(t, env, kafkaE2EOpts{register: func(r engine.HandlerRegistrar) {
		r.RegisterGlobal(h.typ, h)
	}})
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "kafka-e2e-fail",
		Nodes: []types.NodeDef{{Name: "n", Type: h.typ, Retry: retry}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := env.ctx(t, 60*time.Second)
	id, err := node.eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res := waitForCompletion(ctx, t, node.b.State(), id, "n")
	snap, err := node.b.State().GetNode(ctx, id, "n")
	if err != nil || snap == nil {
		t.Fatalf("GetNode(n): %v", err)
	}
	return res, snap
}

// settle waits for the handler call count to stop changing for settleFor, so a
// late redelivery would be counted before the assertion reads it.
func settle(ctx context.Context, t *testing.T, calls *atomic.Int32, settleFor time.Duration) int32 {
	t.Helper()
	last, since := calls.Load(), time.Now()
	pollUntil(ctx, t, "handler call count to settle", func() (bool, error) {
		if cur := calls.Load(); cur != last {
			last, since = cur, time.Now()
		}
		return time.Since(since) >= settleFor, nil
	})
	return last
}

// TestKafkaQueueE2E_Failure checks that the engine's retry and permanent-error
// semantics are unchanged when the tasks travel over Kafka.
func TestKafkaQueueE2E_Failure(t *testing.T) {
	retry := &types.RetrySettings{MaxAttempts: 3, Strategy: types.RetryStrategyFixed, InitialInterval: 200}

	t.Run("transient_exhausted", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &e2eFailHandler{typ: "test.e2e.transient", failBefore: -1}
		res, snap := runE2EFail(t, env, h, retry)
		if res.Status != types.ExecutionStatusFailed || snap.Status != types.NodeStatusFailed {
			t.Fatalf("execution=%s node=%s, want failed/failed", res.Status, snap.Status)
		}
		if got := settle(env.ctx(t, 30*time.Second), t, &h.calls, 3*time.Second); got != 3 || snap.Attempt != 3 {
			t.Fatalf("handler calls=%d node attempt=%d, want 3 and 3 (MaxAttempts)", got, snap.Attempt)
		}
		assertTopicEmpty(t, env, env.topics()[3], "dlq")
	})

	t.Run("transient_then_success", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &e2eFailHandler{typ: "test.e2e.flaky", failBefore: 2}
		res, snap := runE2EFail(t, env, h, retry)
		if res.Status != types.ExecutionStatusSuccess || snap.Status != types.NodeStatusSuccess {
			t.Fatalf("execution=%s node=%s, want success/success", res.Status, snap.Status)
		}
		if got := settle(env.ctx(t, 30*time.Second), t, &h.calls, 2*time.Second); got != 3 {
			t.Fatalf("handler calls=%d, want 3 (two failures, then success)", got)
		}
	})

	t.Run("permanent_not_redelivered", func(t *testing.T) {
		env := newKafkaE2EEnv(t)
		h := &e2eFailHandler{typ: "test.e2e.permanent", permanent: true}
		res, snap := runE2EFail(t, env, h, retry)
		if res.Status != types.ExecutionStatusFailed || snap.Status != types.NodeStatusFailed {
			t.Fatalf("execution=%s node=%s, want failed/failed", res.Status, snap.Status)
		}
		if got := settle(env.ctx(t, 30*time.Second), t, &h.calls, 4*time.Second); got != 1 {
			t.Fatalf("handler calls=%d, want exactly 1: a permanent error must not be retried or redelivered", got)
		}
		assertTopicEmpty(t, env, env.topics()[3], "dlq")
	})
}

// assertTopicEmpty fails when the topic holds any record. The engine commits a
// node failure itself, so a failed node must not reach the consumer-level
// dead-letter topic.
func assertTopicEmpty(t *testing.T, env *kafkaE2EEnv, topic, what string) {
	t.Helper()
	if n := topicRecordCount(t, env, topic); n != 0 {
		t.Fatalf("%s topic %s holds %d records, want 0", what, topic, n)
	}
}
