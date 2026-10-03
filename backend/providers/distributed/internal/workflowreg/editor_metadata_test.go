package workflowreg

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

// TestEditorMetadataRoundTripsThroughRedis proves backend.WorkflowRecord's
// EditorMetadata field survives a real AddWorkflow -> GetWorkflow round trip
// through the Redis-backed registry (ADR-D4 §4, D4).
func TestEditorMetadataRoundTripsThroughRedis(t *testing.T) {
	reg, _ := newTestRegistry(t)
	ctx := context.Background()

	rec := testRecord(t, "editor-metadata", "runtime-sha256:v1:deadbeef")
	rec.EditorMetadata = &types.WorkflowEditorMetadata{
		Positions: map[string]types.Position{"start": {X: 1, Y: 2}, "review": {X: 3, Y: 4}},
		Viewport:  &types.Viewport{X: 10, Y: 20, Zoom: 1.25},
		UI:        map[string]any{"start": map[string]any{"color": "blue"}},
		Notes:     map[string]string{"review": "double-check before merge"},
	}

	added, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	got, err := reg.GetWorkflow(ctx, added.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.EditorMetadata == nil {
		t.Fatal("round-tripped record has no EditorMetadata")
	}
	if got.EditorMetadata.Positions["start"] != (types.Position{X: 1, Y: 2}) {
		t.Errorf("positions[start] = %+v", got.EditorMetadata.Positions["start"])
	}
	if got.EditorMetadata.Positions["review"] != (types.Position{X: 3, Y: 4}) {
		t.Errorf("positions[review] = %+v", got.EditorMetadata.Positions["review"])
	}
	if got.EditorMetadata.Viewport == nil || *got.EditorMetadata.Viewport != *rec.EditorMetadata.Viewport {
		t.Errorf("viewport = %+v, want %+v", got.EditorMetadata.Viewport, rec.EditorMetadata.Viewport)
	}
	if got.EditorMetadata.Notes["review"] != "double-check before merge" {
		t.Errorf("notes[review] = %q", got.EditorMetadata.Notes["review"])
	}

	// GetWorkflowByKey must agree with GetWorkflow on EditorMetadata.
	byKey, err := reg.GetWorkflowByKey(ctx, added.Key)
	if err != nil {
		t.Fatalf("GetWorkflowByKey: %v", err)
	}
	if byKey.EditorMetadata == nil || byKey.EditorMetadata.Notes["review"] != "double-check before merge" {
		t.Errorf("GetWorkflowByKey EditorMetadata = %+v, want it to match GetWorkflow", byKey.EditorMetadata)
	}
}

// TestLegacyRecordWithNoEditorMetadataDecodesToNil proves a record written
// before EditorMetadata existed on disk (no "editor_metadata" JSON field at
// all) decodes without error and with a nil EditorMetadata -- no migration
// pass is required (ADR-D4 §4 implementation plan, commit 5 contract).
func TestLegacyRecordWithNoEditorMetadataDecodesToNil(t *testing.T) {
	reg, _ := newTestRegistry(t)
	ctx := context.Background()

	// testRecord's record carries no EditorMetadata (its zero value is nil),
	// which is exactly the shape of a pre-ADR-D4 record once marshaled: the
	// "editor_metadata" key is omitted by omitempty.
	rec := testRecord(t, "legacy-no-metadata", "runtime-sha256:v1:cafef00d")

	added, err := reg.AddWorkflow(ctx, rec)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	got, err := reg.GetWorkflow(ctx, added.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.EditorMetadata != nil {
		t.Fatalf("EditorMetadata = %+v, want nil for a record that never had one", got.EditorMetadata)
	}
}

// TestEditorMetadataSurvivesCompareAndReplaceWorkflow proves a replacement
// record's EditorMetadata (not the previous record's) is what's stored after
// an atomic CAS replace, matching the "whole record" replace semantics every
// other WorkflowRecord field already has.
func TestEditorMetadataSurvivesCompareAndReplaceWorkflow(t *testing.T) {
	reg, _ := newTestRegistry(t)
	ctx := context.Background()

	original := testRecord(t, "cas-metadata", "runtime-sha256:v1:11111111")
	original.EditorMetadata = &types.WorkflowEditorMetadata{Notes: map[string]string{"start": "old note"}}
	added, err := reg.AddWorkflow(ctx, original)
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	replacement := added
	replacement.EditorMetadata = &types.WorkflowEditorMetadata{Notes: map[string]string{"start": "new note"}}
	replacement.DefinitionHash = "runtime-sha256:v1:22222222"

	result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "mutation-1",
		Expected:    backend.RevisionOfWorkflow(added),
		Replacement: replacement,
	})
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow: %v", err)
	}
	if result.Current.EditorMetadata == nil || result.Current.EditorMetadata.Notes["start"] != "new note" {
		t.Fatalf("result.Current.EditorMetadata = %+v, want the replacement's", result.Current.EditorMetadata)
	}

	got, err := reg.GetWorkflow(ctx, added.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.EditorMetadata == nil || got.EditorMetadata.Notes["start"] != "new note" {
		t.Fatalf("stored EditorMetadata = %+v, want the replacement's", got.EditorMetadata)
	}
}
