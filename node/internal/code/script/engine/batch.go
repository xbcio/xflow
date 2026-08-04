package engine

import "context"

// BatchEngine is an optional capability: an engine that can evaluate a slice of
// records in one call. ScriptNode type-asserts for it and falls back to
// ExecuteBatchSerial when absent.
type BatchEngine interface {
	ExecuteBatch(ctx context.Context, code string, records []any, globals map[string]any) ([]any, error)
}

// BuildRecordGlobals constructs the per-record expression environment by
// copying globals, setting $input to the record, and merging the record's
// non-$-prefixed keys into the top level.
//
// The $ prefix skip is the collision guard isEngineRoot relies on: engine roots
// ($input, $credentials, $config, …) are namespaced by convention, and a record
// field must never shadow them. Since user-authored data fields never start with
// $, the collision is impossible — this guard makes it structurally so.
func BuildRecordGlobals(globals map[string]any, rec any) map[string]any {
	perRecord := make(map[string]any, len(globals)+1)
	for k, v := range globals {
		perRecord[k] = v
	}
	perRecord["$input"] = rec
	if m, ok := rec.(map[string]any); ok {
		for k, v := range m {
			if len(k) > 0 && k[0] == '$' {
				continue
			}
			perRecord[k] = v
		}
	}
	return perRecord
}

// ExecuteBatchSerial is the fallback batch implementation: N separate Execute
// calls. Engines with a cheaper batch path (the wasm reactor reuses one pooled
// instance across the loop) implement BatchEngine themselves; this exists so
// ScriptNode's batch path is not wasm-only.
//
// A per-record failure is skipped, matching BatchEngine implementations: one
// malformed record must not invalidate the batch. A cancelled context fails the
// whole batch — it will hit every remaining record anyway, and returning a short
// result would misrepresent a timeout as "these records didn't match".
func ExecuteBatchSerial(ctx context.Context, e Engine, code string, records []any, globals map[string]any) ([]any, error) {
	out := make([]any, 0, len(records))
	for _, rec := range records {
		perRecord := BuildRecordGlobals(globals, rec)
		res, err := e.Execute(ctx, code, perRecord, DefaultHelpers())
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			continue
		}
		out = append(out, res)
	}
	return out, nil
}
