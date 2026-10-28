//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// TestKafkaQueueE2E_ProductOutboxDispatcher is the regression test for the
// outbox dispatcher in a non-default namespace.
//
// Setup: one backend that owns a single dedicated namespace (so it is a
// non-default deployment), the product's own StartBinding, a 500ms timer wait,
// the execution submitted under that namespace. Nothing else is special, and the
// transport does not matter (the same applied on Asynq).
//
// Expected: the timer resume is delivered within a few outbox ticks and the
// execution succeeds, as it does in the default namespace.
//
// What it guards: the dispatcher that Backend.StartBinding starts runs on a
// context with no namespace, while LeaseOutbox builds its keys from
// namespace.FromContext(ctx). Discovery used to return bare execution IDs, so the
// execution was found and then claimed from the "default" namespace, where it has
// no outbox: the resume entry sat due in xflow:ns:<ns>:exec:{<id>}:outbox:ready
// and was never delivered, and the execution stayed "running" for as long as it
// was watched. Every outbox entry not delivered inline at commit time was
// affected: timer resumes and the retry backoff of a failed node. Discovery now
// reports each execution with its namespace (engine.NamespacedOutboxLister) and
// the dispatcher flushes it under that namespace.
//
// It runs against the product's dispatcher alone: the scenarios no longer start a
// second, namespaced one, so they exercise the same delivery path.
func TestKafkaQueueE2E_ProductOutboxDispatcher(t *testing.T) {
	env := newKafkaE2EEnv(t)
	node := startKafkaE2ENode(t, env, kafkaE2EOpts{})
	id := submitWait(t, env, node, map[string]any{"mode": "timer", "duration": "500ms"})

	ctx := env.ctx(t, 12*time.Second)
	var last types.ExecutionStatus
	pollUntil(ctx, t, "the 500ms timer to resume the execution via the product's outbox dispatcher", func() (bool, error) {
		snap, err := node.b.State().GetExecution(ctx, id)
		if err != nil || snap == nil {
			return false, err
		}
		last = snap.Status
		return types.IsTerminalExecutionStatus(snap.Status), nil
	})
	if last != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", last)
	}
}
