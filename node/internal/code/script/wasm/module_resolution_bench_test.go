package wasm

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// BenchmarkEngineForCode_ModuleResolution measures what it costs to resolve an
// already-compiled module on the per-message hot path — the lookup ONLY, with
// no eval, no pool borrow, and no compile.
//
// It exists because that lookup was 32% of all wasm time in a production
// profile (SAS cross-env collection, 20k messages, batch=500: engineForCode
// 7.06s against 13.57s of actual guest execution), and none of it was work.
// The cost is the cache KEY: a base64 wasm module is ~9 MB, and a map hit must
// confirm the stored key equals the lookup key, so runtime.memequal walks all
// 9 MB. Hashing the key is cheap (AES-NI) and was the only cost the original
// design accounted for; the post-hash compare was not.
//
// The two sub-benchmarks differ ONLY in whether the lookup string shares a
// backing array with the stored one:
//
//   - SameHeader: memequal sees pointer equality and returns immediately. This
//     is the case the original measurement captured, and it reads as ~free.
//   - DistinctHeader: an equal but separately allocated string. memequal has no
//     shortcut and compares the full length. This is what production does — the
//     activation path base64-encodes the module itself while the message path
//     gets its copy from the artifact cache, so the two are never the same
//     allocation.
//
// A benchmark that reuses one code variable for both Add and Get measures the
// first case and concludes a multi-MB key is free. Measured on an M3, the two
// differ by ~7400x. Keep the pair: SameHeader is what makes DistinctHeader's
// number legible as a defect rather than a constant.
//
// Digest is the fix, measured on the same module: the caller passes the artifact
// digest it already holds, engineForSource probes h.engines with a 64-byte key,
// and the multi-MB string is never compared. Its probe string is deliberately
// distinct-header too, so the only difference from DistinctHeader is which key
// the lookup uses. It must land near SameHeader; if it drifts toward
// DistinctHeader, some caller has stopped supplying the digest and the fix is
// silently gone.
func BenchmarkEngineForCode_ModuleResolution(b *testing.B) {
	ctx := context.Background()
	code := b64(reactorWasm)

	// Compile once, outside the timed region: this benchmark is about resolving
	// a module that is already in the cache.
	if _, err := sharedReactorHost.engineForCode(ctx, code); err != nil {
		b.Fatalf("prime module cache: %v", err)
	}

	b.Run("SameHeader", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := sharedReactorHost.engineForCode(ctx, code); err != nil {
				b.Fatalf("engineForCode: %v", err)
			}
		}
	})

	b.Run("DistinctHeader", func(b *testing.B) {
		// An equal string with its own backing array, built once so the
		// allocation is not part of the measurement.
		probe := string(append([]byte(nil), code...))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := sharedReactorHost.engineForCode(ctx, probe); err != nil {
				b.Fatalf("engineForCode: %v", err)
			}
		}
	})

	b.Run("Digest", func(b *testing.B) {
		key, err := moduleKey(code)
		if err != nil {
			b.Fatalf("moduleKey: %v", err)
		}
		src := engine.Source{
			Code:   string(append([]byte(nil), code...)),
			Digest: "sha256:" + key,
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := sharedReactorHost.engineForSource(ctx, src); err != nil {
				b.Fatalf("engineForSource: %v", err)
			}
		}
	})
}
