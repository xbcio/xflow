package local

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// Concurrent FlushOutbox calls must not each deliver the same intent.
//
// Every worker calls ListOutbox on the same execution, and an entry stays
// listable until AckOutbox. Without a delivery lease each worker lists the same
// un-acked entries, enqueues them, and acks them — so a map body runs once per
// worker that happened to see it, not once per item. Measured before the lease:
// 800 items at concurrency 4 ran the body 826–1110 times; at concurrency 1
// exactly 800.
//
// Delivery is documented at-least-once and body nodes must be idempotent, so
// this is a waste-of-work bound rather than a correctness bound. The lease
// narrows it to the cases the at-least-once contract actually exists for
// (crash, response loss, lease expiry) instead of ordinary concurrency.
func TestConcurrentFlushDoesNotRedeliverTheSameIntent(t *testing.T) {
	const perMap, maps = 200, 4
	b, eng, body, stop := newFanOutEngine(t, 4, perMap)
	defer stop()

	nodes := []types.NodeDef{{Name: "start", Type: "test.echo"}}
	conns := types.Connections{"start": {"main": {Targets: []types.Connection{}}}}
	for i := 0; i < maps; i++ {
		name := fmt.Sprintf("m%d", i)
		nodes = append(nodes, types.NodeDef{Name: name, Type: "xflow.map", Parameters: map[string]any{
			"items": "$input.items", "body": mapBodyDef(),
		}})
		tg := conns["start"]["main"]
		tg.Targets = append(tg.Targets, types.Connection{Node: name, Input: "main"})
		conns["start"]["main"] = tg
	}
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "redelivery-probe", Nodes: nodes, Connections: conns,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, compiled, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := b.WaitDone(ctx, id); err != nil {
		t.Fatalf("WaitDone: %v (body ran %d/%d)", err, body.count(), perMap*maps)
	}
	want := perMap * maps
	if got := body.count(); got != want {
		t.Errorf("body ran %d times, want exactly %d — concurrent FlushOutbox "+
			"redelivered %d intents that were already in flight", got, want, got-want)
	}
}

// A leased entry must become listable again once its visibility timeout
// elapses: the lease narrows ordinary concurrency, it must not turn a crashed
// worker's in-flight entry into a permanently stuck one.
//
// This drives the state store directly. Going through a real fan-out cannot
// distinguish "the lease expired and the entry was redelivered" from "the entry
// was never leased at all".
func TestOutboxDeliveryLeaseExpires(t *testing.T) {
	b := New(WithConcurrency(1))
	state := b.State().(engine.AtomicStateStore)
	ctx := context.Background()

	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "lease-expiry",
		Nodes: []types.NodeDef{{Name: "only", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id := types.ExecutionID("outbox-lease-expiry-1")
	idx, _ := g.NodeIndex("only")
	entry := engine.OutboxEntry{
		ID: "root/outbox-lease-expiry-1/only/1",
		Task: engine.Task{
			ExecutionID: id, NodeName: "only", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox: %v", err)
	}

	now := time.Now().UTC()
	first, err := state.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("no outbox entries to lease")
	}

	// Immediately re-listing must see nothing: the entries are leased.
	again, err := state.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox (leased): %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("re-list returned %d leased entries, want 0", len(again))
	}

	// Past the visibility timeout they must reappear, un-acked, so a crashed
	// worker's work is not lost.
	future := now.Add(engine.OutboxDeliveryLeaseTTL + time.Second)
	recovered, err := state.ListOutbox(ctx, id, future, 16)
	if err != nil {
		t.Fatalf("ListOutbox (expired): %v", err)
	}
	if len(recovered) != len(first) {
		t.Fatalf("after the visibility timeout got %d entries, want %d", len(recovered), len(first))
	}
}
