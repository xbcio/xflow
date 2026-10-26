package engine

import (
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
