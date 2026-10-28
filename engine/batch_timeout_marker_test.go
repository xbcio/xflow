package engine

import (
	"errors"
	"testing"
)

// The batch result carries a host-written marker when an item ran out of time,
// because the error the verdict is built from loses the cause: an item's
// failure reaches BatchResultForCommit as plain text, and the all-batches-done
// barrier hands completeLoopSplit only the result maps. The marker is the one
// thing that tells an execution-level timeout from any other item failure there.
//
// It sits at the top level of the batch result, beside items, never inside it:
// a guest's return value can only land in items, so a top-level key is one a
// guest cannot write.
func TestBatchResultMarksATimeoutAtTheTopLevel(t *testing.T) {
	cases := []struct {
		name            string
		results         []BatchItemResult
		continueOnError bool
		wantFailed      bool
		wantMarker      bool
	}{
		{
			name: "every item timed out",
			results: []BatchItemResult{
				{Index: 0, Err: errors.New("timeout: deadline exceeded"), Timeout: true},
				{Index: 1, Err: errors.New("timeout: deadline exceeded"), Timeout: true},
			},
			wantFailed: true,
			wantMarker: true,
		},
		{
			name: "one item timed out and the rest succeeded under continue_on_error",
			results: []BatchItemResult{
				{Index: 0, Data: map[string]any{"id": 0}},
				{Index: 1, Err: errors.New("timeout: deadline exceeded"), Timeout: true},
			},
			continueOnError: true,
			wantFailed:      false,
			wantMarker:      true,
		},
		{
			name: "a timeout among other failures",
			results: []BatchItemResult{
				{Index: 0, Err: errors.New("boom")},
				{Index: 1, Err: errors.New("timeout: deadline exceeded"), Timeout: true},
			},
			continueOnError: true,
			wantFailed:      true,
			wantMarker:      true,
		},
		{
			name: "an item failed for another reason",
			results: []BatchItemResult{
				{Index: 0, Data: map[string]any{"id": 0}},
				{Index: 1, Err: errors.New("item body failed")},
			},
			wantFailed: true,
			wantMarker: false,
		},
		{
			name: "every item succeeded",
			results: []BatchItemResult{
				{Index: 0, Data: map[string]any{"id": 0}},
				{Index: 1, Data: map[string]any{"id": 1}},
			},
			wantFailed: false,
			wantMarker: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, verdict := BatchResultForCommit(tc.results, tc.continueOnError, nil)
			if (verdict != nil) != tc.wantFailed {
				t.Fatalf("verdict = %v, want failed = %v", verdict, tc.wantFailed)
			}
			marker, present := data[batchTimeoutKey]
			if !tc.wantMarker {
				if present {
					t.Fatalf("batch result carries %q = %v, want no marker without a timed-out item", batchTimeoutKey, marker)
				}
				return
			}
			if marker != true {
				t.Fatalf("batch result %q = %v (present %v), want true", batchTimeoutKey, marker, present)
			}
			// Beside items, not in them: no item map may hold the key.
			items, _ := data["items"].([]any)
			for i, item := range items {
				if row, ok := item.(map[string]any); ok {
					if _, inItem := row[batchTimeoutKey]; inItem {
						t.Fatalf("item %d holds %q: the marker must stay out of what a guest can write", i, batchTimeoutKey)
					}
				}
			}
		})
	}
}
