package script

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// TestRootsProjectionSurvivesBatchInjection pins that a Roots() declaration and
// the Kafka batch path compose.
//
// The batch path is the one SAS actually runs: a Kafka trigger delivers
// {messages: [...]}, ScriptNode.Execute detects that shape, and the engine calls
// BuildRecordGlobals per record — AFTER buildScriptGlobals has already run. So
// the projection sees the batch envelope and the per-record injection sees the
// projection's output.
//
// The risk that motivates this test: the projection could strip a key that
// BuildRecordGlobals or the guest still needs, turning every record in every
// batch into a decode failure. That failure is invisible in a unit test of
// projectRoots alone, because it only appears once the two stages are composed.
func TestRootsProjectionSurvivesBatchInjection(t *testing.T) {
	record := map[string]any{"request": map[string]any{"method": "GET"}}
	input := &types.Input{
		Data: map[string]any{
			"messages": []any{record},
			"count":    1,
		},
		Params: map[string]any{"roots": []string{"$item"}},
	}

	globals := buildScriptGlobals(input, nil, nil)
	records, isBatch := batchRecords(input.Data)
	if !isBatch {
		t.Fatal("batchRecords did not recognise the trigger envelope — the probe is not exercising the batch path")
	}

	perRecord := engine.BuildRecordGlobals(globals, records[0])

	// BuildRecordGlobals republishes the record as $input plus its non-$ keys at
	// the top level, so the record reaches the guest regardless of the
	// projection. What the projection must not do is prevent that.
	if _, ok := perRecord["$input"]; !ok {
		t.Error("$input missing after batch injection — the guest would see no record")
	}
	if _, ok := perRecord["request"]; !ok {
		t.Error("record keys missing after batch injection")
	}
}

// The decode guest reads env["$item"] and returns errDecode when it is absent.
// On the batch path $item is NOT what carries the record — BuildRecordGlobals
// publishes $input and the flattened keys instead — so a Roots("$item")
// declaration on a batch-fed node declares a key that never arrives.
//
// This test states which keys actually reach a batch-path guest, so the
// declaration can be checked against reality rather than against the map-body
// path's shape.
func TestBatchPathCarriesInputNotItem(t *testing.T) {
	record := map[string]any{"request": map[string]any{"method": "GET"}}
	input := &types.Input{
		Data:   map[string]any{"messages": []any{record}, "count": 1},
		Params: map[string]any{},
	}

	globals := buildScriptGlobals(input, nil, nil)
	records, _ := batchRecords(input.Data)
	perRecord := engine.BuildRecordGlobals(globals, records[0])

	if _, ok := perRecord["$item"]; ok {
		t.Error("batch path published $item — this test's premise is wrong, recheck the guest declaration")
	}
	if _, ok := perRecord["$input"]; !ok {
		t.Fatal("batch path published neither $item nor $input")
	}
	b, err := json.Marshal(perRecord["$input"])
	if err != nil || len(b) < 10 {
		t.Fatalf("$input did not carry the record: %s (%v)", b, err)
	}
}
