package script

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestScriptNode_BatchHitCountCountsOnlyNonEmptyTags pins countHits: the
// "hit_count" field in a batch result must count only the results whose
// "tags" is a non-empty list, not every result in the batch. That distinction
// is the entire point of the metric (see countHits's doc comment) — a
// hit_count that just echoes len(results) would make a rule set that stopped
// matching anything look identical to one still hitting on every message.
func TestScriptNode_BatchHitCountCountsOnlyNonEmptyTags(t *testing.T) {
	n := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  "goja",
			// Only "/api/a" records get a non-empty tags list.
			"code": `({tags: $input.path === "/api/a" ? ["matched"] : []})`,
		},
		Data: map[string]any{
			"messages": []any{
				map[string]any{"path": "/api/a"},
				map[string]any{"path": "/other"},
				map[string]any{"path": "/api/a"},
				map[string]any{"path": "/other"},
			},
			"count": 4,
		},
	}
	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want main", out.Port)
	}
	// Two of the four records produced a non-empty tags list.
	if out.Data["hit_count"] != 2 {
		t.Fatalf("hit_count = %v, want 2", out.Data["hit_count"])
	}
	if out.Data["count"] != 4 {
		t.Fatalf("count = %v, want 4", out.Data["count"])
	}
}

// TestScriptNode_BatchHitCountZeroWhenNoTagsMatch is the discriminating
// counterpart: every record produces an empty tags list, so hit_count must be
// 0 even though count (and len(results)) is nonzero. A stub that always
// returns len(results) (or any other function of the batch size) cannot pass
// both this test and the one above with the same batch size.
func TestScriptNode_BatchHitCountZeroWhenNoTagsMatch(t *testing.T) {
	n := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  "goja",
			"code":     `({tags: []})`,
		},
		Data: map[string]any{
			"messages": []any{
				map[string]any{"path": "/other"},
				map[string]any{"path": "/other"},
				map[string]any{"path": "/other"},
			},
			"count": 3,
		},
	}
	out, err := n.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Data["count"] != 3 {
		t.Fatalf("count = %v, want 3", out.Data["count"])
	}
	if out.Data["hit_count"] != 0 {
		t.Fatalf("hit_count = %v, want 0 (no result had a non-empty tags list)", out.Data["hit_count"])
	}
}
