package flow_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestWait_Factory_Signal(t *testing.T) {
	b := node.Wait("order_paid")
	params := b.RawParams().(map[string]any)
	if params["signal_name"] != "order_paid" {
		t.Fatalf("expected signal_name=order_paid, got %v", params)
	}
}

func TestWait_Factory_Duration(t *testing.T) {
	b := node.WaitDuration("5m")
	params := b.RawParams().(map[string]any)
	if params["mode"] != "timer" {
		t.Fatalf("expected mode=timer, got %v", params["mode"])
	}
	if params["duration"] != "5m" {
		t.Fatalf("expected duration=5m, got %v", params["duration"])
	}
}

func TestWait_WithTimeoutAddsTimeoutParam(t *testing.T) {
	b := node.Wait("order_paid").WithTimeout("30m")
	params := b.RawParams().(map[string]any)

	if params["timeout"] != "30m" {
		t.Fatalf("timeout = %v, want 30m", params["timeout"])
	}
}

func TestWait_PrepareSuspend_Signal(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	b := node.Wait("order_paid")
	input := &types.Input{
		Params:   b.RawParams().(map[string]any),
		NodeName: "wait_1",
	}
	spec, err := sh.PrepareSuspend(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Mode != types.ModeSignal {
		t.Fatalf("expected types.ModeSignal, got %v", spec.Mode)
	}
	if len(spec.Signals) != 1 || spec.Signals[0] != "order_paid" {
		t.Fatalf("expected signal [order_paid], got %v", spec.Signals)
	}
}

func TestWait_PrepareSuspend_SignalDefault(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Params:   map[string]any{"mode": "signal"},
		NodeName: "my_wait",
	}
	spec, err := sh.PrepareSuspend(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Signals[0] != "my_wait/signal" {
		t.Fatalf("expected default signal name \"my_wait/signal\", got %q", spec.Signals[0])
	}
}

func TestWait_PrepareSuspend_MultiSignal(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Params: map[string]any{
			"mode":    "signal",
			"signals": []any{"sig_a", "sig_b", "sig_c"},
			"quorum":  2,
		},
		NodeName: "wait_multi",
	}
	spec, err := sh.PrepareSuspend(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Mode != types.ModeMultiSignal {
		t.Fatalf("expected types.ModeMultiSignal, got %v", spec.Mode)
	}
	if len(spec.Signals) != 3 {
		t.Fatalf("expected 3 signals, got %d", len(spec.Signals))
	}
	if spec.Quorum != 2 {
		t.Fatalf("expected quorum=2, got %d", spec.Quorum)
	}
}

func TestWait_PrepareSuspend_Timer(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	b := node.WaitDuration("5m")
	input := &types.Input{
		Params:   b.RawParams().(map[string]any),
		NodeName: "wait_timer",
	}
	spec, err := sh.PrepareSuspend(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Mode != types.ModeTimer {
		t.Fatalf("expected types.ModeTimer, got %v", spec.Mode)
	}
	if spec.Timer.Minutes() != 5 {
		t.Fatalf("expected 5m timer, got %v", spec.Timer)
	}
}

func TestWait_PrepareSuspend_TimerMissingDuration(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Params:   map[string]any{"mode": "timer"},
		NodeName: "wait_timer",
	}
	_, err := sh.PrepareSuspend(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for timer mode without duration")
	}
}

func TestWait_OnResume_Signal(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Data:     map[string]any{"existing": "data"},
		NodeName: "wait_1",
	}
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "order_paid",
		Data:      map[string]any{"order_id": "123"},
	}
	out, err := sh.OnResume(context.Background(), input, signal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port \"main\", got %q", out.Port)
	}
	if out.Data["signal_name"] != "order_paid" {
		t.Fatalf("expected signal_name=order_paid, got %v", out.Data["signal_name"])
	}
	if out.Data["existing"] != "data" {
		t.Fatalf("expected existing data preserved, got %v", out.Data)
	}
}

func TestWait_OnResume_SignalIncludesInputs(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Data: map[string]any{"existing": "data"},
		Inputs: map[string]any{
			"branch_a": map[string]any{"value": "a"},
			"branch_b": map[string]any{"value": "b"},
		},
		NodeName: "wait_1",
	}
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "order_paid",
		Data:      map[string]any{"order_id": "123"},
	}
	out, err := sh.OnResume(context.Background(), input, signal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Values, not just presence. Two distinct non-nil payloads are in play, so a
	// merge that records "this branch fired" instead of what it produced -- or
	// that copies one branch's value under the other's key -- passes a nil check
	// while every downstream node reads the wrong data.
	for _, br := range []struct{ key, want string }{{"branch_a", "a"}, {"branch_b", "b"}} {
		got, _ := out.Data[br.key].(map[string]any)
		if got == nil || got["value"] != br.want {
			t.Errorf("%s = %#v, want map with value %q", br.key, out.Data[br.key], br.want)
		}
	}
	if out.Data["existing"] != "data" {
		t.Fatalf("expected existing data preserved, got %v", out.Data)
	}
}

// The signal's own payload is why a resume happens at all, and neither key it
// lands under was read by any test in the tree: deleting both assignments in
// wait.go left the whole suite green while a resumed workflow saw the signal's
// name but never its body. $nodes['wait_1'].signal_data is the documented way a
// downstream node reads what the caller sent.
func TestWait_OnResume_ProjectsSignalPayloadAndSignalSet(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{NodeName: "wait_1"}
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "order_paid",
		Data:      map[string]any{"order_id": "123"},
		All: map[string]map[string]any{
			"order_paid":     {"order_id": "123"},
			"stock_reserved": {"sku": "X-1"},
		},
	}
	out, err := sh.OnResume(context.Background(), input, signal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body, _ := out.Data["signal_data"].(map[string]any)
	if body == nil || body["order_id"] != "123" {
		t.Errorf("signal_data = %#v, want the signal's own body {order_id: 123}", out.Data["signal_data"])
	}
	// A wait_all resume carries every signal it waited on; dropping the set
	// collapses a multi-signal join to whichever one happened to arrive last.
	all, _ := out.Data["signals"].(map[string]map[string]any)
	if len(all) != 2 {
		t.Fatalf("signals = %#v, want both signals the node waited on", out.Data["signals"])
	}
	if all["stock_reserved"]["sku"] != "X-1" {
		t.Errorf("signals[stock_reserved] = %#v, want {sku: X-1}", all["stock_reserved"])
	}
}

func TestWait_OnResume_Timeout(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{NodeName: "wait_1"}
	signal := &types.SignalPayload{Triggered: types.TimeoutFired}
	out, err := sh.OnResume(context.Background(), input, signal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "timeout" {
		t.Fatalf("expected port \"timeout\", got %q", out.Port)
	}
}

func TestWait_OnResume_Timer(t *testing.T) {
	h, _ := registry.Lookup("xflow.wait")
	sh := h.(types.SuspendingHandler)

	input := &types.Input{
		Data:     map[string]any{"key": "val"},
		NodeName: "wait_1",
	}
	signal := &types.SignalPayload{Triggered: types.TimerFired}
	out, err := sh.OnResume(context.Background(), input, signal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("expected port \"main\", got %q", out.Port)
	}
	if out.Data["key"] != "val" {
		t.Fatalf("expected data passthrough, got %v", out.Data)
	}
}
