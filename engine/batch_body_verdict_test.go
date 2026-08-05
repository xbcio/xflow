package engine

import (
	"errors"
	"testing"
)

// A batch that stopped at a failed item is a FAILED batch, whatever else it
// managed to complete first. This is §9's continue_on_error:false contract:
// "立即停止该批剩余项，该批整体失败" — and the reason it matters is that the
// verdict is the only thing that reaches completeLoopSplit. A batch reported
// Success terminalizes the map node as Success, so a workflow that asked to stop
// on the first error gets a clean success with a hole in its results instead.
//
// The "every item failed" rule this replaces was silently correct for
// continue_on_error:true (a partial failure there IS a successful batch) and
// silently wrong for false, where the batch stops early and the failed item is
// its LAST entry — never all of them.
func TestBatchStoppedAtAFailedItemIsAFailedBatch(t *testing.T) {
	results := []BatchItemResult{
		{Index: 0, Data: map[string]any{"id": 0}},
		{Index: 1, Err: errors.New("item body failed")},
	}

	data, verdict := BatchResultForCommit(results, false)
	if verdict == nil {
		t.Fatalf("batch with a failed item under continue_on_error=false reported no "+
			"failure; the map node will commit Success with a hole in its results: %+v", data)
	}
	if items, _ := data["items"].([]any); len(items) != 2 {
		t.Errorf("items = %+v, want both slots — a failed item holds its slot", items)
	}
}

// Under continue_on_error:true a partially-failed batch is a SUCCESSFUL batch:
// the placeholders are the deliverable, and the map node stays successful so
// downstream can filter them. Without this arm the fix above would collapse
// continue_on_error into a no-op — every failure would fail the map node either
// way, which is the setting's entire purpose gone.
func TestPartiallyFailedBatchUnderContinueOnErrorIsSuccessful(t *testing.T) {
	results := []BatchItemResult{
		{Index: 0, Data: map[string]any{"id": 0}},
		{Index: 1, Err: errors.New("item body failed")},
		{Index: 2, Data: map[string]any{"id": 2}},
	}

	if _, verdict := BatchResultForCommit(results, true); verdict != nil {
		t.Errorf("partially-failed batch under continue_on_error=true reported %v; "+
			"placeholders are the deliverable there, so the batch succeeded", verdict)
	}
}

// A batch whose every item failed is failed under either setting. This is the
// case the old rule got right; keeping it asserted stops the fix from narrowing
// to "only the last item decides".
func TestBatchWithEveryItemFailedIsFailedUnderEitherSetting(t *testing.T) {
	results := []BatchItemResult{
		{Index: 0, Err: errors.New("first")},
		{Index: 1, Err: errors.New("second")},
	}
	for _, continueOnError := range []bool{false, true} {
		if _, verdict := BatchResultForCommit(results, continueOnError); verdict == nil {
			t.Errorf("continue_on_error=%v: a batch whose every item failed reported no failure",
				continueOnError)
		}
	}
}

// An empty batch is not a failure. An expansion can legitimately produce one
// (a final short batch, an empty items array), and reporting it as failed would
// fail the map node for having nothing to do.
func TestEmptyBatchIsNotAFailure(t *testing.T) {
	for _, continueOnError := range []bool{false, true} {
		if _, verdict := BatchResultForCommit(nil, continueOnError); verdict != nil {
			t.Errorf("continue_on_error=%v: empty batch reported %v, want no failure",
				continueOnError, verdict)
		}
	}
}
