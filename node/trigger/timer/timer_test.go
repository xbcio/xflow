package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func TestTimerTriggerEmitsAtInterval(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	tr := New().Every(10 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()
	if !rt.WaitEmit(time.Second) {
		t.Fatal("timer did not emit")
	}
}

func TestTimerTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	rt.SetDedupFunc(func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	})
	tr := New().Every(5 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitDedup(time.Second) {
		t.Fatal("timer did not attempt dedup")
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestTimerTriggerContinuesAfterEmitError(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	var calls atomic.Int64
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		if calls.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	})
	tr := New().Every(5 * time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "timer",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.EmitCount())
	}
}

func TestTimerTriggerEventUsesDeterministicIntervalBucket(t *testing.T) {
	interval := 10 * time.Second
	firstTick := time.Unix(1_700_000_000, 100_000_000).UTC()
	secondTick := firstTick.Add(700 * time.Millisecond)
	thirdTick := firstTick.Add(interval)

	firstEvent := newTimerTriggerEvent("wf-1", "timer", interval, firstTick)
	secondEvent := newTimerTriggerEvent("wf-1", "timer", interval, secondTick)
	thirdEvent := newTimerTriggerEvent("wf-1", "timer", interval, thirdTick)

	if firstEvent.ID != secondEvent.ID {
		t.Fatalf("same bucket IDs = %q/%q, want equal", firstEvent.ID, secondEvent.ID)
	}
	if firstEvent.ID == thirdEvent.ID {
		t.Fatalf("different bucket IDs = %q/%q, want different", firstEvent.ID, thirdEvent.ID)
	}

	wantScheduled := firstTick.Truncate(interval).Format(time.RFC3339Nano)
	if got := firstEvent.Data["scheduled_time"]; got != wantScheduled {
		t.Fatalf("scheduled_time = %#v, want %q", got, wantScheduled)
	}
	if !firstEvent.Time.Equal(firstTick) {
		t.Fatalf("first event time = %s, want %s", firstEvent.Time, firstTick)
	}
	if !secondEvent.Time.Equal(secondTick) {
		t.Fatalf("second event time = %s, want %s", secondEvent.Time, secondTick)
	}
}

// TestTimerNodeTypeAndParamsAreFrozen guards the two things the package split
// must not touch: the node type string (persisted in workflow definitions and
// sent in runner capability declarations) and the RawParams key (the YAML/JSON
// DSL contract).
func TestTimerNodeTypeAndParamsAreFrozen(t *testing.T) {
	n := New().Every(90 * time.Second)
	if got := n.NodeType(); got != "xflow.trigger.timer" {
		t.Fatalf("NodeType() = %q, want xflow.trigger.timer", got)
	}
	if got := n.Descriptor().Type; got != "xflow.trigger.timer" {
		t.Fatalf("Descriptor().Type = %q, want xflow.trigger.timer", got)
	}
	params, ok := n.RawParams().(map[string]any)
	if !ok {
		t.Fatalf("RawParams() = %T, want map[string]any", n.RawParams())
	}
	if got := params["interval"]; got != "1m30s" {
		t.Fatalf("RawParams()[\"interval\"] = %#v, want \"1m30s\"", got)
	}
	if len(params) != 1 {
		t.Fatalf("RawParams() has %d keys, want exactly 1 (interval)", len(params))
	}
}
