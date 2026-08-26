package script

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestScriptNode_BatchCountReflectsSurvivingResultsNotRecordCount pins that
// the batch output's "count" field is len(results) — the number of records
// that actually produced a result — not the number of records handed in.
//
// engine.ExecuteBatchSerial (the fallback batch path goja takes, since it does
// not implement engine.BatchEngine) explicitly skips a record whose Execute
// call errors, as long as the context has not expired: "one malformed record
// must not invalidate the batch" (see its doc comment). So results can be
// shorter than records, and "count" must track the shorter, surviving slice —
// a caller reading count to know how many entries "results" holds would be
// misled by the original record count.
func TestScriptNode_BatchCountReflectsSurvivingResultsNotRecordCount(t *testing.T) {
	n := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  "goja",
			// The middle record's path deterministically throws, so goja's
			// Execute returns an error for it and ExecuteBatchSerial skips it
			// (ctx never expires here — there is no timeout in play).
			"code": `if ($input.path === "/bad") { throw new Error("skip"); }
({path: $input.path})`,
		},
		Data: map[string]any{
			"messages": []any{
				map[string]any{"path": "/api/a"},
				map[string]any{"path": "/bad"},
				map[string]any{"path": "/api/a"},
			},
			"count": 3,
		},
	}
	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want main", out.Port)
	}
	results, ok := out.Data["results"].([]any)
	if !ok {
		t.Fatalf("results = %#v, want []any", out.Data["results"])
	}
	// Three records went in, one errored and was skipped: two results survive.
	if len(results) != 2 {
		t.Fatalf("results has %d entries, want 2 (one record should have been skipped)", len(results))
	}
	if out.Data["count"] != 2 {
		t.Fatalf("count = %v, want 2 (len(results), not the 3 records handed to the engine)", out.Data["count"])
	}
}
