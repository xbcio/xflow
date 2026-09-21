package xflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestFAFSnapshotRetainsOverlappingSliceViewShapes(t *testing.T) {
	var seen struct {
		wide   fafSnapshotSliceView
		narrow fafSnapshotSliceView
	}
	eng, workflowID, handler := newFAFSnapshotRegression(t, func(input *types.Input) error {
		wide, ok := input.Data["wide"].([]string)
		if !ok {
			return fmt.Errorf("wide = %T, want []string", input.Data["wide"])
		}
		narrow, ok := input.Data["narrow"].([]string)
		if !ok {
			return fmt.Errorf("narrow = %T, want []string", input.Data["narrow"])
		}
		seen.wide = snapshotSliceView(wide)
		seen.narrow = snapshotSliceView(narrow)
		return nil
	})

	backing := []string{"zero", "one", "two", "three"}
	wide := backing[:2]     // len 2, cap 4
	narrow := backing[:3:3] // same start, distinct len and cap
	dispatchFAFSnapshotRegression(t, eng, workflowID, handler, map[string]any{
		"wide":   wide,
		"narrow": narrow,
	}, func() {
		copy(backing, []string{"after-zero", "after-one", "after-two", "after-three"})
	})

	assertSnapshotSliceView(t, "wide", seen.wide, 2, 4, []string{"zero", "one", "two", "three"})
	assertSnapshotSliceView(t, "narrow", seen.narrow, 3, 3, []string{"zero", "one", "two"})
}

func TestFAFSnapshotClonesExportedFieldsBesidePrivateField(t *testing.T) {
	type payload struct {
		Labels  map[string]string
		Items   []string
		private int
	}

	var seen payload
	eng, workflowID, handler := newFAFSnapshotRegression(t, func(input *types.Input) error {
		got, ok := input.Data["payload"].(payload)
		if !ok {
			return fmt.Errorf("payload = %T, want snapshot payload", input.Data["payload"])
		}
		seen = got
		return nil
	})

	original := payload{
		Labels:  map[string]string{"state": "before"},
		Items:   []string{"before"},
		private: 7,
	}
	dispatchFAFSnapshotRegression(t, eng, workflowID, handler, map[string]any{"payload": original}, func() {
		original.Labels["state"] = "after"
		original.Items[0] = "after"
	})

	if got := seen.Labels["state"]; got != "before" {
		t.Fatalf("snapshotted exported map value = %q, want before", got)
	}
	if got := seen.Items[0]; got != "before" {
		t.Fatalf("snapshotted exported slice value = %q, want before", got)
	}
	if seen.private != 7 {
		t.Fatalf("snapshotted private field = %d, want 7", seen.private)
	}
}

func TestFAFSnapshotPreservesCyclicPayload(t *testing.T) {
	var sawInputSelfCycle bool
	var sawRuntimeSelfCycle bool
	var seenInputValue string
	var seenRuntimeValue string
	eng, workflowID, handler := newFAFSnapshotRegression(t, func(input *types.Input) error {
		cycle, ok := input.Data["cycle"].(map[string]any)
		if !ok {
			return fmt.Errorf("input cycle = %T, want map[string]any", input.Data["cycle"])
		}
		self, ok := cycle["self"].(map[string]any)
		if !ok {
			return fmt.Errorf("input cycle self = %T, want map[string]any", cycle["self"])
		}
		sawInputSelfCycle = reflect.ValueOf(cycle).Pointer() == reflect.ValueOf(self).Pointer()
		seenInputValue, _ = cycle["value"].(string)

		runtimeCycle := input.Runtime.Vars
		runtimeSelf, ok := runtimeCycle["self"].(map[string]any)
		if !ok {
			return fmt.Errorf("runtime cycle self = %T, want map[string]any", runtimeCycle["self"])
		}
		sawRuntimeSelfCycle = reflect.ValueOf(runtimeCycle).Pointer() == reflect.ValueOf(runtimeSelf).Pointer()
		seenRuntimeValue, _ = runtimeCycle["value"].(string)
		return nil
	})

	inputCycle := make(map[string]any)
	inputCycle["self"] = inputCycle
	inputCycle["value"] = "before"
	runtimeCycle := make(map[string]any)
	runtimeCycle["self"] = runtimeCycle
	runtimeCycle["value"] = "before"
	dispatchFAFSnapshotRegression(t, eng, workflowID, handler, map[string]any{"cycle": inputCycle}, func() {
		inputCycle["value"] = "after"
		runtimeCycle["value"] = "after"
	}, WithRuntimeVars(runtimeCycle))

	if !sawInputSelfCycle {
		t.Fatal("snapshotted input cycle no longer points to itself")
	}
	if !sawRuntimeSelfCycle {
		t.Fatal("snapshotted runtime cycle no longer points to itself")
	}
	if seenInputValue != "before" {
		t.Fatalf("snapshotted input cycle value = %q, want before", seenInputValue)
	}
	if seenRuntimeValue != "before" {
		t.Fatalf("snapshotted runtime cycle value = %q, want before", seenRuntimeValue)
	}
}

func TestFAFRejectsUnsafeSnapshotBeforeAsyncLaunch(t *testing.T) {
	eng, workflowID, handler := newFAFSnapshotRegression(t, func(*types.Input) error {
		return nil
	})

	err := eng.FireAndForget(context.Background(), workflowID, map[string]any{
		"callback": func() {},
	})
	if !errors.Is(err, ErrFAFPayloadSnapshotUnsupported) {
		t.Fatalf("FireAndForget() error = %v, want ErrFAFPayloadSnapshotUnsupported", err)
	}
	if got := handler.calls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0 after snapshot rejection", got)
	}
}

type fafSnapshotSliceView struct {
	length   int
	capacity int
	values   []string
}

func snapshotSliceView(value []string) fafSnapshotSliceView {
	return fafSnapshotSliceView{
		length:   len(value),
		capacity: cap(value),
		values:   append([]string(nil), value[:cap(value)]...),
	}
}

func assertSnapshotSliceView(t *testing.T, name string, got fafSnapshotSliceView, wantLength, wantCapacity int, wantValues []string) {
	t.Helper()
	if got.length != wantLength || got.capacity != wantCapacity || !reflect.DeepEqual(got.values, wantValues) {
		t.Fatalf("%s snapshot = len %d cap %d values %#v, want len %d cap %d values %#v", name, got.length, got.capacity, got.values, wantLength, wantCapacity, wantValues)
	}
}

type fafSnapshotRegressionHandler struct {
	started chan struct{}
	release chan struct{}
	result  chan error
	inspect func(*types.Input) error
	calls   atomic.Int32
}

func (*fafSnapshotRegressionHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.faf.snapshot.regression", Kind: types.NodeKindAction}
}

func (h *fafSnapshotRegressionHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.calls.Add(1)
	close(h.started)
	<-h.release
	err := h.inspect(input)
	h.result <- err
	if err != nil {
		return nil, err
	}
	return &types.Output{}, nil
}

func newFAFSnapshotRegression(t *testing.T, inspect func(*types.Input) error) (*Engine, types.WorkflowID, *fafSnapshotRegressionHandler) {
	t.Helper()
	handler := &fafSnapshotRegressionHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result:  make(chan error, 1),
		inspect: inspect,
	}
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	// Release a started handler before stopping the local engine, including on a
	// test failure that occurs before the normal dispatch helper releases it.
	t.Cleanup(func() {
		select {
		case <-handler.release:
		default:
			close(handler.release)
		}
		eng.Stop()
	})

	wf := Workflow("faf-snapshot-regression").FAF()
	wf.LocalNode("only", handler)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	return eng, workflowID, handler
}

func dispatchFAFSnapshotRegression(t *testing.T, eng *Engine, workflowID types.WorkflowID, handler *fafSnapshotRegressionHandler, input map[string]any, mutate func(), opts ...InvokeOption) {
	t.Helper()
	if err := eng.FireAndForget(context.Background(), workflowID, input, opts...); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not start")
	}

	mutate()
	close(handler.release)
	select {
	case err := <-handler.result:
		if err != nil {
			t.Fatalf("FAF handler snapshot assertion failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FAF handler did not finish")
	}
}
