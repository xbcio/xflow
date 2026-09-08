package engine

import (
	"context"
	"errors"
)

// BatchEngine is an optional capability: an engine that can evaluate a slice of
// records in one call. ScriptNode type-asserts for it and falls back to
// ExecuteBatchSerial when absent.
type BatchEngine interface {
	ExecuteBatch(ctx context.Context, src Source, records []any, globals map[string]any) ([]any, error)
}

// RecordSkippable is implemented by an error that condemns exactly ONE record
// rather than the engine instance, the batch, or the node. The canonical case is
// a guest-classified per-record failure (bad input JSON, oversized output) where
// the runtime is still healthy and the next record will evaluate fine.
//
// It lives in this package, below both callers, because the batch path and the
// single-record path MUST agree on it. They did not, and the divergence cost a
// production pipeline: the wasm reactor's ExecuteBatch skipped an oversized
// record and carried on, while the identical error on the single-record path
// (script.go, which is what a map body's item takes -- one record per item never
// has the {messages:[...]} shape that selects the batch path) fell through to the
// error port. engine/commit.go's outputPortRetryError then rebuilt it as
// errors.New, stripping the unwrap chain, so the node failed unclassified, the
// Kafka batch was never admitted, its offsets never advanced, and the partition
// stalled behind one record until the aggregate buffer overflowed. Measured:
// 14 of 18 partitions dead, 188 822 messages discarded, keep-up 3.5%.
//
// One interface, two callers, so a future code cannot be skippable on one path
// and fatal on the other. Adding a code means changing RecordSkippable's
// implementation once.
type RecordSkippable interface {
	error
	// RecordSkippable reports whether only this record is condemned.
	RecordSkippable() bool
}

// IsRecordSkippable reports whether err condemns a single record rather than the
// batch or the engine instance. It matches anywhere in the unwrap chain, so
// wrapping an error on the way out does not change the verdict.
//
// Everything unrecognised is NOT skippable. That default is deliberate: an
// unknown error is more likely a host or infrastructure failure, where skipping
// would silently shorten the result, than a per-record fault.
func IsRecordSkippable(err error) bool {
	var rs RecordSkippable
	if !errors.As(err, &rs) {
		return false
	}
	return rs.RecordSkippable()
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
func ExecuteBatchSerial(ctx context.Context, e Engine, src Source, records []any, globals map[string]any) ([]any, error) {
	out := make([]any, 0, len(records))
	for _, rec := range records {
		perRecord := BuildRecordGlobals(globals, rec)
		res, err := e.Execute(ctx, src, perRecord, DefaultHelpers())
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
