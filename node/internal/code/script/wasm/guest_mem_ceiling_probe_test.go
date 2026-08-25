package wasm

import (
	"context"
	"os"
	"sort"
	"testing"
)

// The size bucket this run feeds the guest, as a percentile of the fixture's
// record-size distribution. One bucket per process: the point of the probe is
// that each arm gets a runtime that has never seen another arm's records.
const memProbeBucketEnv = "XFLOW_PROBE_MEM_BUCKET"

// memProbeCap bounds the run. It is well above the eval count at which the
// unfixed guest trapped (477 at p95, 5 464 at p75) and above
// maxEvalsPerInstance, so a clean run to the cap means the recycle carried the
// guest through several instance lifetimes rather than merely finishing before
// the first one would have died.
const memProbeCap = 40_000

// TestGuestMemoryCeiling checks that one guest can evaluate an unbounded run of
// records without a single one failing, and does it one record size at a time.
//
// This is the probe maxEvalsPerInstance's own comment used to ask for and say
// was never committed: a fresh runtime per arm, swept against the REAL size
// distribution rather than its mean. It falsified the claim it was written to
// check --
//
//	"8 000 is a value that has never been observed to be too high"
//	                                          -- pool.go, before 2026-08-25
//
// -- because on the SAS apisix fixture the decode guest exhausts its 256-page
// (16 MiB) linear memory after 477 evals at p95 and 5 464 at p75, so the
// proactive recycle could never fire and an instance's normal end was a TRAPPED
// eval. The instance itself recovered (doom discards and rebuilds it), but the
// message being evaluated when the memory ran out FAILED, and where a failed
// message goes from there is decided by an error classification that
// ScriptNode's error port erases.
//
// recycleMemoryHighWater is the fix, and this test is now its verification: with
// the recycle keyed on linear memory rather than on a count, every bucket must
// run clean. The numbers in that constant's table came from this test.
//
// Run one bucket per invocation -- sharing a runtime across buckets makes each
// arm's trap point a function of the arms before it, which is the flaw that
// produced the earlier, unreproducible "every 18 856 evals" figure:
//
//	XFLOW_BENCH_DECODE_WASM=<guest>.wasm \
//	XFLOW_BENCH_FIXTURE=<fixture>.ndjson \
//	XFLOW_PROBE_MEM_BUCKET=p50 \
//	go test ./node/internal/code/script/wasm/ -run TestGuestMemoryCeiling -v
//
// It skips unless both the guest and the fixture are pointed at explicitly, so
// it does not run in a normal build.
func TestGuestMemoryCeiling(t *testing.T) {
	bucket := os.Getenv(memProbeBucketEnv)
	if bucket == "" {
		t.Skipf("set %s=p25|p50|p75|p95|all (with %s and %s) to sweep the guest's memory ceiling",
			memProbeBucketEnv, benchDecodeWasmEnv, benchFixtureEnv)
	}
	records := benchFixtureRecords(t)
	sized := recordsInBucket(t, records, bucket)

	obsRec := &recordingObserver{}
	SetObserver(obsRec)
	defer SetObserver(nil)

	ctx := context.Background()
	eng, f := benchGuestEngine(t, ctx, benchGuestModule(t, benchDecodeWasmEnv))

	// The pool benchGuestEngine builds is one instance wide (swapConfig width 1),
	// so a serial driver hands every eval to the same instance and the counts
	// below are per-instance, not per-pool. Borrowing between evals is therefore
	// uncontended -- and it is the only way to see the number that actually
	// decides the trap: how far the guest's linear memory has grown.
	peekMem := func() uint32 {
		inst, pool, err := eng.borrow(ctx)
		if err != nil {
			return 0
		}
		size := inst.mem.Size()
		eng.giveBack(ctx, pool, inst)
		return size
	}

	var evals, bytesFed int
	var memHigh, memPrev uint32
	var firstErr error
	for evals < memProbeCap {
		rec := sized[evals%len(sized)]
		in, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			t.Fatalf("encodeStdin: %v", err)
		}
		if _, err := f.evalFromPool(ctx, eng, in); err != nil {
			firstErr = err
			break
		}
		evals++
		bytesFed += len(rec)

		size := peekMem()
		if size < memPrev {
			// A drop can only mean a fresh instance: linear memory never
			// shrinks. Log it so the recycles are visible in sequence next to
			// the growth curve they interrupted.
			t.Logf("  eval %6d: recycled (%.2f -> %.2f MiB)",
				evals, float64(memPrev)/(1<<20), float64(size)/(1<<20))
		} else if size > memHigh {
			// Log every step up rather than every N evals: linear memory only
			// ever grows within one instance, and what the recycle bound needs
			// to know is whether it plateaus below the cap or climbs into it.
			// Sampling on a fixed interval would show the same value repeatedly
			// through a plateau and hide where the steps are.
			t.Logf("  eval %6d: linear memory %.2f MiB", evals, float64(size)/(1<<20))
		}
		memPrev = size
		if size > memHigh {
			memHigh = size
		}
	}

	mean := 0
	for _, r := range sized {
		mean += len(r)
	}
	mean /= len(sized)

	causes := map[string]int{}
	for _, c := range obsRec.recycledCauses() {
		causes[c]++
	}
	// memHigh is the highest value a POST-eval sample saw, which is strictly
	// less than the value that trips the recycle: by the time this loop can
	// borrow again, an instance that crossed the threshold has already been torn
	// down and replaced. Reading 11.50 MiB against a 12 MiB threshold is that
	// blind spot, not a recycle firing early.
	t.Logf("bucket %s (%d records, mean %d B): %d evals / %.1f MiB fed, "+
		"linear memory sampled up to %.2f MiB of 16 MiB, recycles %v",
		bucket, len(sized), mean, evals, float64(bytesFed)/(1<<20), float64(memHigh)/(1<<20), causes)

	if firstErr != nil {
		t.Fatalf("eval %d failed after %.1f MiB fed, with linear memory at %.2f MiB: %v\n\n"+
			"A failed eval here is a message production would lose or redeliver. The planned "+
			"recycle at %.2f MiB is supposed to retire the instance before it reaches this "+
			"point -- either it did not fire, or this record climbed past the threshold and "+
			"into the cap within a single eval.",
			evals+1, float64(bytesFed)/(1<<20), float64(memHigh)/(1<<20), firstErr,
			float64(recycleMemoryHighWater)/(1<<20))
	}
}

// recordsInBucket returns the fixture records clustered around one percentile of
// its size distribution.
//
// A window rather than the single record at that percentile: one record repeated
// 2 000 times is a shape the guest's allocator can get unrealistically good at,
// and the question here is about a real stream. The window is real records, so
// the tail bucket carries the tail's actual contents rather than a padded
// approximation of them.
func recordsInBucket(t *testing.T, records [][]byte, bucket string) [][]byte {
	t.Helper()
	if bucket == "all" {
		return records
	}
	var q float64
	switch bucket {
	case "p25":
		q = 0.25
	case "p50":
		q = 0.50
	case "p75":
		q = 0.75
	case "p95":
		q = 0.95
	default:
		t.Fatalf("%s=%q: want p25, p50, p75, p95 or all", memProbeBucketEnv, bucket)
	}

	sorted := make([][]byte, len(records))
	copy(sorted, records)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) < len(sorted[j]) })

	width := len(sorted) / 10
	if width < 8 {
		width = 8
	}
	centre := int(q * float64(len(sorted)))
	lo := centre - width/2
	if lo < 0 {
		lo = 0
	}
	hi := lo + width
	if hi > len(sorted) {
		hi = len(sorted)
		lo = hi - width
		if lo < 0 {
			lo = 0
		}
	}
	return sorted[lo:hi]
}
