package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// settledTimedOutBatch is a batch that settled without an error but whose data
// still carries the marker: the continue_on_error shape, where the timed-out
// item is a placeholder and the batch as a whole succeeds.
func settledTimedOutBatch() TaskResult {
	return TaskResult{Output: &types.Output{Data: map[string]any{
		"items":         []any{"ok", map[string]any{batchErrorKey: "timeout: deadline exceeded", "_index": 1}},
		"count":         2,
		"failed":        1,
		batchTimeoutKey: true,
	}}}
}

// TestOnlyTheMiddleBatchCarryingTheMarkerExpiresTheExecution: the barrier looks
// at every batch's result, not at the first or the last one. Three batches, only
// the second reports the marker, and the execution's budget is gone. A guard
// that reads results[0] or results[len-1] never sees it and lets on_error route
// the failure.
func TestOnlyTheMiddleBatchCarryingTheMarkerExpiresTheExecution(t *testing.T) {
	shapes := []struct {
		name   string
		result func() TaskResult
	}{
		{"failed_batch", timedOutBatch},
		{"settled_batch", settledTimedOutBatch},
	}
	for _, onError := range []string{"", "continue", "error_output", "main_output"} {
		for _, shape := range shapes {
			t.Run(guardName(onError)+"/"+shape.name, func(t *testing.T) {
				rig := newBatchGuardRig(t, onError, true, time.Now().Add(-time.Minute))
				if len(rig.batches) != 3 {
					t.Fatalf("map expanded to %d batches, want 3 so that a middle one exists", len(rig.batches))
				}
				queued := rig.reportAll(t, 1, shape.result())
				rig.requireExpired(t, queued)
			})
		}
	}
}

// failedBatchWithMarker is failedBatch with batchTimeoutKey set to value, for a
// marker that is present but is not the boolean true the host writes.
func failedBatchWithMarker(value any) TaskResult {
	result := failedBatch()
	result.Output.Data[batchTimeoutKey] = value
	return result
}

// TestMarkerThatIsNotBooleanTrueDoesNotExpireTheExecution: the guard reads the
// marker's value, not its presence. A key holding false, or the string "true",
// is not the host's timeout verdict, so even with the execution budget gone the
// batch keeps its on_error semantics. A guard that tests only for the key would
// expire the execution here.
func TestMarkerThatIsNotBooleanTrueDoesNotExpireTheExecution(t *testing.T) {
	values := []struct {
		name  string
		value any
	}{
		{"bool_false", false},
		{"string_true", "true"},
	}
	for _, v := range values {
		t.Run(v.name, func(t *testing.T) {
			rig := newBatchGuardRig(t, "continue", true, time.Now().Add(-time.Minute))
			queued := rig.reportAll(t, 0, failedBatchWithMarker(v.value))
			requireBatchOnError(t, rig, queued, types.ExecutionStatusRunning, types.NodeStatusContinued, "next")
		})
	}
}

// TestMarkerStaysOutOfEveryBatchItemShape: the marker is one top-level key and
// the items array is left exactly as the body produced it, whatever shape the
// timed-out entries have. The three shapes are an item with no data, an item
// whose data is a map the guest wrote, and an item that failed. A guest's map
// must come back untouched (the marker would otherwise be forgeable and its own
// output rewritten), a nil must stay nil, and a failure placeholder is only
// {_error, _index}.
func TestMarkerStaysOutOfEveryBatchItemShape(t *testing.T) {
	guestMap := map[string]any{"id": 1}
	results := []BatchItemResult{
		{Index: 0, Data: nil, Timeout: true},
		{Index: 1, Data: guestMap, Timeout: true},
		{Index: 2, Err: errors.New("timeout: deadline exceeded"), Timeout: true},
		{Index: 3, Data: map[string]any{"id": 3}},
	}
	data, verdict := BatchResultForCommit(results, true, nil)
	if verdict != nil {
		t.Fatalf("verdict = %v, want a settled batch under continue_on_error", verdict)
	}
	if marker, _ := data[batchTimeoutKey].(bool); !marker {
		t.Fatalf("batch data %q = %v, want true at the top level", batchTimeoutKey, data[batchTimeoutKey])
	}
	items, ok := data["items"].([]any)
	if !ok || len(items) != len(results) {
		t.Fatalf("items = %v, want %d entries", data["items"], len(results))
	}
	for i, item := range items {
		if row, isMap := item.(map[string]any); isMap {
			if _, inItem := row[batchTimeoutKey]; inItem {
				t.Fatalf("item %d = %v holds %q: the marker belongs beside items, not in them", i, row, batchTimeoutKey)
			}
		}
	}
	// A nil Data is a typed nil map once it sits in an []any, so it is checked as
	// a map: a marker written into a freshly made map would make it non-nil.
	if first, isMap := items[0].(map[string]any); !isMap || first != nil {
		t.Fatalf("item 0 = %#v, want a nil map kept as nil", items[0])
	}
	if len(guestMap) != 1 || guestMap["id"] != 1 {
		t.Fatalf("the guest's map = %v, want it left untouched", guestMap)
	}
	placeholder, isMap := items[2].(map[string]any)
	if !isMap || len(placeholder) != 2 {
		t.Fatalf("item 2 = %v, want exactly {%s, _index}", items[2], batchErrorKey)
	}
}
