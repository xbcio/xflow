// sas_guest_bench_test.go measures the REAL production guests, in-sandbox, on
// the byte-exact fixture the CPU attribution matrix ran on.
//
// Why this file exists alongside eval_cost_bench_test.go: that file's layers are
// the right decomposition but its guest is `tagger`, a test guest whose whole
// job is three expression rules. The production decode guest converts an apisix
// record into the hjson shape and emits ~1.84x its input, and clean re-parses
// and re-emits that. The matrix attributes ~8.1 cpu_ms/message to guest
// interiors; tagger accounts for about a quarter of that, so tagger is a FLOOR,
// not the cost. These benchmarks close that gap with the actual artifacts.
//
// Both the modules and the fixture come from outside this module, by path:
//
//	# in the SAS repo, dump the exact fixture bytes:
//	XFLOW_DUMP_FIXTURE_PATH=/tmp/steady-fixture.ndjson \
//	  go test ./pkg/xflow -run '^TestDumpSteadyFixture$' -count=1
//
//	# then here:
//	XFLOW_BENCH_DECODE_WASM=<sas>/guest-decode.wasm \
//	XFLOW_BENCH_CLEAN_WASM=<sas>/guest-clean.wasm \
//	XFLOW_BENCH_FIXTURE=/tmp/steady-fixture.ndjson \
//	  go test ./node/internal/code/script/wasm -run '^$' \
//	    -bench BenchmarkSASGuest -benchtime 200x
//
// The guests and their rule configuration belong to SAS, not to xflow. The rule
// COUNT here matches what the matrix logged (pre_analysis=1 post_decode=1); the
// rule TEXT is a representative non-matching predicate over the documented rule
// env, not a copy of any deployed rule.
//
// Payloads are marshalled fresh inside the timed loop because that is what the
// host does per eval. The host share is measured separately by
// BenchmarkSASGuest_HostBoundary so it can be subtracted rather than assumed.
package wasm

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	benchDecodeWasmEnv = "XFLOW_BENCH_DECODE_WASM"
	benchCleanWasmEnv  = "XFLOW_BENCH_CLEAN_WASM"
	benchFixtureEnv    = "XFLOW_BENCH_FIXTURE"
)

// benchGuestCfg is the one supply both guests read, each decoding only the key
// it knows -- the production arrangement. One rule each, matching the matrix.
//
// The decode rule is the content-type drop, not an arbitrary predicate. That
// matters for the mix: the decode guest's only hardcoded filter is on status, so
// with a rule that matches nothing, image/png responses pass conversion and
// three of the five fixture shapes survive (measured: 60.1%). Dropping non-text
// bodies with content is a configured rule, and it is the filter this repo's
// own native approximation already documents, so a representative rule has to
// be that one or the benchmark runs on a heavier mix than production serves.
var benchGuestCfg = map[string]any{
	"revision": 1,
	"pre_analysis": []any{
		map[string]any{
			"rule_id": 1,
			"expr": `response.content_length > 0 and !(response.content_type in ` +
				`["application/json", "text/html", "text/plain"])`,
		},
	},
	"post_decode": []any{
		map[string]any{"rule_id": 1, "expr": "response.status_code == 599"},
	},
}

// benchPostDecodeExprEnv overrides the post-decode rule text for one run.
//
// WHICH fields a rule names is now a cost input, not just a predicate: the clean
// guest builds only the env members its compiled rules can reach. A rule reading
// nothing but status_code therefore skips both bodies, and quoting a speedup
// measured on it as if it were production's would overstate the change --
// production's seeded rule reads response.body_text, which keeps that half's
// unquote. So an A/B over this feature has to be run at least twice, and the
// arms have to be nameable from outside rather than baked in.
//
// The default is unchanged so every other benchmark in this file keeps its
// baseline.
const benchPostDecodeExprEnv = "XFLOW_BENCH_POST_DECODE_EXPR"

func benchGuestConfig() map[string]any {
	cfg := make(map[string]any, len(benchGuestCfg))
	for k, v := range benchGuestCfg {
		cfg[k] = v
	}
	if e := os.Getenv(benchPostDecodeExprEnv); e != "" {
		cfg["post_decode"] = []any{map[string]any{"rule_id": 1, "expr": e}}
	}
	return cfg
}

func benchGuestModule(b *testing.B, envVar string) []byte {
	b.Helper()
	path := os.Getenv(envVar)
	if path == "" {
		b.Skipf("set %s=<path to guest .wasm> to benchmark the production guests", envVar)
	}
	wasmBytes, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read %s=%s: %v", envVar, path, err)
	}
	return wasmBytes
}

// benchFixtureRecords reads the NDJSON dump of buildSteadyFixture().
func benchFixtureRecords(b *testing.B) [][]byte {
	b.Helper()
	path := os.Getenv(benchFixtureEnv)
	if path == "" {
		b.Skipf("set %s=<ndjson> (see TestDumpSteadyFixture in the SAS repo) to benchmark on the real distribution", benchFixtureEnv)
	}
	f, err := os.Open(path)
	if err != nil {
		b.Fatalf("open %s=%s: %v", benchFixtureEnv, path, err)
	}
	defer f.Close()

	var records [][]byte
	total := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		rec := make([]byte, len(line))
		copy(rec, line)
		records = append(records, rec)
		total += len(rec)
	}
	if err := scanner.Err(); err != nil {
		b.Fatalf("scan %s: %v", path, err)
	}
	if len(records) == 0 {
		b.Fatalf("%s contained no records", path)
	}
	b.Logf("fixture: %d records, mean %d B/record", len(records), total/len(records))
	return records
}

// decodeGlobals is the env the map body hands the decode guest: the Kafka
// envelope under $item, projected to Roots("$item").
func decodeGlobals(record []byte) map[string]any {
	return map[string]any{
		"$item": map[string]any{
			"topic": "sas-bench",
			"value": string(record),
		},
	}
}

// benchGuestEngine compiles a module and installs the shared config.
func benchGuestEngine(b *testing.B, ctx context.Context, wasmBytes []byte) (*reactorEngine, *reactorFacade) {
	b.Helper()
	h := newReactorHost()
	e, err := h.engineFor(ctx, wasmBytes)
	if err != nil {
		b.Fatalf("engineFor: %v", err)
	}
	cfgBytes, err := json.Marshal(benchGuestConfig())
	if err != nil {
		b.Fatalf("marshal config: %v", err)
	}
	if err := e.swapConfig(ctx, cfgBytes, 1, 0); err != nil {
		b.Fatalf("swapConfig: %v", err)
	}
	return e, &reactorFacade{}
}

// cleanGlobalsFrom turns a decode result into clean's env, or reports that
// decode filtered the record out.
//
// The `$input` key is load-bearing and was missing here until it was caught by
// CleanAB's emit guard. clean's eval reads env["$input"] and answers `{}` for an
// env without one -- that is its first branch, before it decodes anything. So a
// map spelled {"req":…,"resp":…} at the top level did not feed clean the record,
// it fed clean the pass-through early exit, and every clean number this file
// produced before the fix priced a JSON parse of ~7 KB plus two writes of `{}`.
// That is the same defect assertSurvival exists to catch one stage upstream; the
// clean stage had no equivalent, because only the paired benchmark counts emits.
//
// The survival check is not decoration. A filtered record returns `{}` and
// costs almost nothing, so a benchmark that unknowingly ran on filtered records
// would report a fraction of the real cost and look like an optimisation. Every
// benchmark below asserts a plausible survival fraction rather than trusting it.
func cleanGlobalsFrom(out any) (map[string]any, bool) {
	m, ok := out.(map[string]any)
	if !ok {
		return nil, false
	}
	req, resp := m["req"], m["resp"]
	if req == nil && resp == nil {
		return nil, false
	}
	return map[string]any{"$input": map[string]any{"req": req, "resp": resp}}, true
}

// benchDecodeAll runs every fixture record through decode once, outside any
// timed region, and returns clean's inputs for the records that survived.
func benchDecodeAll(b *testing.B, ctx context.Context, e *reactorEngine, f *reactorFacade,
	records [][]byte) (survivors []map[string]any) {
	b.Helper()
	for _, rec := range records {
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		out, err := f.evalFromPool(ctx, e, input)
		if err != nil {
			b.Fatalf("decode eval: %v", err)
		}
		if globals, ok := cleanGlobalsFrom(out); ok {
			survivors = append(survivors, globals)
		}
	}
	return survivors
}

// assertSurvival fails when the fixture's survival fraction is outside the range
// the shape mix implies. With the content-type rule configured, two of the five
// realistic shapes pass both the status filter and the rule, so ~40% is
// expected, and the matrix's own run stored 50% of what it produced.
//
// The band is wide because its job is catching the two ways this benchmark could
// silently measure nothing: 0% means every timed iteration hit the `{}` early
// exit and the numbers are a fraction of the real cost, while ~100% means the
// rule stopped matching and the mix is heavier than production. It is not a
// precision assertion on the fixture.
func assertSurvival(b *testing.B, survived, total int) {
	b.Helper()
	if total == 0 {
		b.Fatal("no records")
	}
	frac := float64(survived) / float64(total)
	b.Logf("decode survival: %d/%d = %.1f%%", survived, total, frac*100)
	if survived == 0 {
		b.Fatal("every record was filtered out; these benchmarks would measure the empty-output early exit, not the conversion")
	}
	if frac < 0.25 || frac > 0.55 {
		b.Fatalf("survival %.1f%% is outside the 25-55%% the five-shape mix plus the content-type rule implies; the fixture or the rule is not what this benchmark assumes", frac*100)
	}
}

// pickStride returns a step that walks all n records in a cycle while jumping
// across the fixture rather than reading it front-to-back.
//
// This exists because sequential indexing (records[i%len]) makes every number in
// this file depend on -benchtime. buildSteadyFixture emits a heavy-tailed size
// distribution in ascending order, so records[0:b.N] is the SMALL end: measured,
// -benchtime 30x touches records averaging 1460 B and reports 1.58 ms/record,
// while a full sweep touches the true mean of 8324 B and reports 6.70 ms -- a
// 4.2x understatement from nothing but loop count. Worse, it biases comparisons
// asymmetrically: at 200x the 184 kept records wrap into a full sweep but the 275
// dropped ones do not, so kept was measured on the whole distribution and dropped
// on its cheap head.
//
// Any stride coprime with n visits every index exactly once per cycle, so the walk
// is a permutation, not a sample -- b.N >= n still sweeps everything, and b.N < n
// now draws from across the size range instead of the bottom of it.
func pickStride(n int) int {
	gcd := func(a, b int) int {
		for b != 0 {
			a, b = b, a%b
		}
		return a
	}
	// Chosen to be large enough to jump between size regimes on realistic fixture
	// sizes, then walked down to the first value actually coprime with n.
	for _, s := range []int{211, 199, 181, 167, 149, 131, 113, 101, 97, 89, 83, 79, 73, 71} {
		if s < n && gcd(s, n) == 1 {
			return s
		}
	}
	return 1
}

// benchIndexer returns a closure mapping iteration i to a fixture index, plus a
// running total of the bytes it has handed out.
func benchIndexer(n int) func(i int) int {
	stride := pickStride(n)
	return func(i int) int { return (i * stride) % n }
}

// BenchmarkSASGuest_Decode is the decode guest on the production traffic mix:
// host marshal of the projected env, sandbox conversion, host unmarshal of the
// result. Filtered records are included because production pays for them too.
//
// touched-B/record must land near the fixture's own mean; if it does not, the run
// did not see the real size distribution and the ns/op is not comparable to any
// other run here. See pickStride for why that used to happen silently.
func BenchmarkSASGuest_Decode(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	e, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	assertSurvival(b, len(benchDecodeAll(b, ctx, e, f, records)), len(records))
	idx := benchIndexer(len(records))

	b.ResetTimer()
	b.ReportAllocs()
	touched := 0
	for i := range b.N {
		rec := records[idx(i)]
		touched += len(rec)
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		if _, err := f.evalFromPool(ctx, e, input); err != nil {
			b.Fatalf("decode eval: %v", err)
		}
	}
	b.ReportMetric(float64(touched)/float64(b.N), "touched-B/record")
}

// BenchmarkSASGuest_Clean is the clean guest on decode's real output, over the
// records that survived decode -- the only ones clean ever does work for.
func BenchmarkSASGuest_Clean(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	decodeEng, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	survivors := benchDecodeAll(b, ctx, decodeEng, f, records)
	assertSurvival(b, len(survivors), len(records))
	cleanEng, cf := benchGuestEngine(b, ctx, benchGuestModule(b, benchCleanWasmEnv))
	idx := benchIndexer(len(survivors))

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		input, err := encodeStdin(survivors[idx(i)])
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		if _, err := cf.evalFromPool(ctx, cleanEng, input); err != nil {
			b.Fatalf("clean eval: %v", err)
		}
	}
}

// BenchmarkSASGuest_Chain is the whole per-message guest cost: decode, then
// clean on decode's actual output, skipping clean when decode filtered the
// record -- exactly what the pipeline does. ns/op here is the figure to compare
// against the matrix's ~8.1 cpu_ms/message guest interior.
func BenchmarkSASGuest_Chain(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	decodeEng, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	assertSurvival(b, len(benchDecodeAll(b, ctx, decodeEng, f, records)), len(records))
	cleanEng, cf := benchGuestEngine(b, ctx, benchGuestModule(b, benchCleanWasmEnv))
	idx := benchIndexer(len(records))

	b.ResetTimer()
	b.ReportAllocs()
	cleaned, touched := 0, 0
	for i := range b.N {
		rec := records[idx(i)]
		touched += len(rec)
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		out, err := f.evalFromPool(ctx, decodeEng, input)
		if err != nil {
			b.Fatalf("decode eval: %v", err)
		}
		globals, ok := cleanGlobalsFrom(out)
		if !ok {
			continue
		}
		cleanInput, err := encodeStdin(globals)
		if err != nil {
			b.Fatalf("encodeStdin clean: %v", err)
		}
		if _, err := cf.evalFromPool(ctx, cleanEng, cleanInput); err != nil {
			b.Fatalf("clean eval: %v", err)
		}
		cleaned++
	}
	b.ReportMetric(float64(cleaned)/float64(b.N)*100, "%cleaned")
	b.ReportMetric(float64(touched)/float64(b.N), "touched-B/record")
}

// BenchmarkSASGuest_HostBoundary is both hops' host-side JSON only: marshal the
// env in, unmarshal the guest's bytes out, for decode and for clean. No sandbox.
// Subtract this from Chain to get the in-sandbox share.
func BenchmarkSASGuest_HostBoundary(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	decodeEng, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))

	// Capture each hop's real output bytes once, so the unmarshal side is timed
	// on the same payloads the guests actually return.
	type hop struct{ decodeOut, cleanIn []byte }
	hops := make([]hop, 0, len(records))
	survived := 0
	for _, rec := range records {
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		out, err := f.evalFromPool(ctx, decodeEng, input)
		if err != nil {
			b.Fatalf("decode eval: %v", err)
		}
		globals, ok := cleanGlobalsFrom(out)
		if !ok {
			continue
		}
		survived++
		outBytes, err := json.Marshal(out)
		if err != nil {
			b.Fatalf("marshal decode out: %v", err)
		}
		cleanIn, err := encodeStdin(globals)
		if err != nil {
			b.Fatalf("encodeStdin clean: %v", err)
		}
		hops = append(hops, hop{decodeOut: outBytes, cleanIn: cleanIn})
	}
	assertSurvival(b, survived, len(records))
	recIdx, hopIdx := benchIndexer(len(records)), benchIndexer(len(hops))

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		rec := records[recIdx(i)]
		h := hops[hopIdx(i)]
		if _, err := encodeStdin(decodeGlobals(rec)); err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		if _, err := decodeStdout(h.decodeOut); err != nil {
			b.Fatalf("decodeStdout: %v", err)
		}
		if _, err := decodeStdout(h.cleanIn); err != nil {
			b.Fatalf("decodeStdout clean: %v", err)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Where decode spends it: work done for records that are then thrown away
// ─────────────────────────────────────────────────────────────────────────────

// benchDecodeSplit partitions the fixture by whether decode keeps the record.
func benchDecodeSplit(b *testing.B, ctx context.Context, e *reactorEngine, f *reactorFacade,
	records [][]byte) (kept, dropped [][]byte) {
	b.Helper()
	for _, rec := range records {
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		out, err := f.evalFromPool(ctx, e, input)
		if err != nil {
			b.Fatalf("decode eval: %v", err)
		}
		if _, ok := cleanGlobalsFrom(out); ok {
			kept = append(kept, rec)
			continue
		}
		dropped = append(dropped, rec)
	}
	return kept, dropped
}

func benchDecodeOver(b *testing.B, subset [][]byte) {
	b.Helper()
	ctx := context.Background()
	records := benchFixtureRecords(b)
	e, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	kept, dropped := benchDecodeSplit(b, ctx, e, f, records)
	assertSurvival(b, len(kept), len(records))

	pick := kept
	if subset != nil {
		pick = dropped
	}
	total := 0
	for _, r := range pick {
		total += len(r)
	}
	meanBytes := float64(total) / float64(len(pick))
	idx := benchIndexer(len(pick))

	b.ResetTimer()
	b.ReportAllocs()
	// Reported after ResetTimer: ReportMetric before it is discarded along with
	// the pre-timer state, so the metric never reaches the result line.
	b.ReportMetric(meanBytes, "B/record")
	touched := 0
	for i := range b.N {
		rec := pick[idx(i)]
		touched += len(rec)
		input, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		if _, err := f.evalFromPool(ctx, e, input); err != nil {
			b.Fatalf("decode eval: %v", err)
		}
	}
	b.ReportMetric(float64(touched)/float64(b.N), "touched-B/record")
}

// BenchmarkSASGuest_Decode_Kept is decode on the records it converts and emits.
func BenchmarkSASGuest_Decode_Kept(b *testing.B) { benchDecodeOver(b, nil) }

// BenchmarkSASGuest_Decode_Dropped is decode on the ~60% of records it parses
// and then discards, returning `{}`.
//
// This is the size of a specific piece of waste, not just a curiosity. The
// status check is cheap and happens before conversion, but the content-type rule
// runs against the PARSED message, so a record destined for the bin has already
// paid the full envelope unmarshal, the inner apisix unmarshal, and the body
// parse by the time the rule rejects it. Whatever this benchmark reports is what
// a pre-parse filter on the raw bytes could recover, times the drop rate.
func BenchmarkSASGuest_Decode_Dropped(b *testing.B) { benchDecodeOver(b, [][]byte{}) }

// ─────────────────────────────────────────────────────────────────────────────
// The JSON-in-a-JSON-string envelope -- MEASURED, AND IT DOES NOT MATTER
// ─────────────────────────────────────────────────────────────────────────────

// bareGlobals hands the guest the apisix record as a nested JSON OBJECT rather
// than as a JSON-encoded string.
//
// json.RawMessage is embedded verbatim by encoding/json, so this is the same
// bytes with one layer of quoting removed. The production envelope comes from
// messageData() in node/trigger/kafka/kafka.go, which does `"value":
// string(msg.Value)`: the host escapes every quote in the record to build a
// string literal, the guest unescapes it back, and only then parses it. That
// looks like three scans of 8 KB where one would do.
//
// It measured as no saving at all. Envelope 5.636 ms/record vs bare 5.799 ms --
// the bare shape is fractionally SLOWER, with both arms at the same 40.1%
// survival so they ran the same code path. Host allocations do fall as predicted
// (34.7 vs 44.8 KB/op), which confirms the host did less work; the wall clock
// simply does not care. The reason is visible in the apisix types: Request.Body
// and Response.Body are `string` fields, so parsing a message never parses the
// body as JSON. Unescaping a string into a string field is not where the ~4.6 ms
// parse goes, so removing a layer of it recovers nothing.
//
// Keep this benchmark. It is the evidence that a plausible, frequently-proposed
// change to messageData() is not worth making.
func bareGlobals(record []byte) map[string]any {
	return map[string]any{"$item": json.RawMessage(record)}
}

// BenchmarkSASGuest_Decode_BareItem is decode on the same fixture, same mix,
// with the envelope's string quoting removed. Compare against
// BenchmarkSASGuest_Decode; see bareGlobals for the measured verdict.
func BenchmarkSASGuest_Decode_BareItem(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	e, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))

	// Same survival guard, evaluated through the bare shape: if parseItem does
	// not recognise it, every record returns an error or `{}` and the comparison
	// against the envelope run would be measuring a different code path.
	survived := 0
	for _, rec := range records {
		input, err := encodeStdin(bareGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		out, err := f.evalFromPool(ctx, e, input)
		if err != nil {
			b.Fatalf("decode eval (bare): %v", err)
		}
		if _, ok := cleanGlobalsFrom(out); ok {
			survived++
		}
	}
	assertSurvival(b, survived, len(records))
	idx := benchIndexer(len(records))

	b.ResetTimer()
	b.ReportAllocs()
	touched := 0
	for i := range b.N {
		rec := records[idx(i)]
		touched += len(rec)
		input, err := encodeStdin(bareGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		if _, err := f.evalFromPool(ctx, e, input); err != nil {
			b.Fatalf("decode eval: %v", err)
		}
	}
	b.ReportMetric(float64(touched)/float64(b.N), "touched-B/record")
}

// ─────────────────────────────────────────────────────────────────────────────
// Paired A/B of two clean builds, alternating inside one process
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkSASGuest_CleanAB runs two clean .wasm builds over the same decode
// output, alternating arms within a single process.
//
// It exists because the end-to-end harness cannot resolve a change this size.
// Four 1400 msg/s runs of the same pipeline drifted monotonically (drain 16.6 ->
// 38.3 s) and the two runs of the SAME build differed by 11% -- larger than the
// effect under test. Anything measured as "run A, then run B" on this machine
// inherits that drift. Here both arms share one process, one fixture, one decode
// pass, and `-count=N` interleaves them A,B,A,B..., so position averages out
// instead of accumulating.
//
//	XFLOW_BENCH_CLEAN_WASM=<old>.wasm XFLOW_BENCH_CLEAN_WASM_B=<new>.wasm \
//	XFLOW_BENCH_DECODE_WASM=<...> XFLOW_BENCH_FIXTURE=<...> \
//	  go test ./node/internal/code/script/wasm -run '^$' \
//	    -bench BenchmarkSASGuest_CleanAB -benchtime 200x -count 5
//
// Arm A is whichever build XFLOW_BENCH_CLEAN_WASM names, so a single-arm
// BenchmarkSASGuest_Clean run and arm A here measure the same artifact.
const (
	benchCleanWasmBEnv  = "XFLOW_BENCH_CLEAN_WASM_B"
	benchDecodeWasmBEnv = "XFLOW_BENCH_DECODE_WASM_B"
	benchDecodeFloorEnv = "XFLOW_BENCH_DECODE_FLOOR"
	benchCleanFloorEnv  = "XFLOW_BENCH_CLEAN_FLOOR"
)

// cleanABState is built once per process. Rebuilding it per -count iteration
// would put a fresh decode sweep and two module compiles between the arms, which
// is the interleaving this benchmark exists to avoid.
type cleanABState struct {
	survivors []map[string]any
	inputs    [][]byte
	engA      *reactorEngine
	facade    *reactorFacade
}

var (
	cleanABOnce  sync.Once
	cleanABCache *cleanABState
)

func cleanABSetup(b *testing.B) *cleanABState {
	b.Helper()
	// Skip checks run on every call, not inside the Once: a skipped first
	// invocation must not leave a nil cache for the next one to dereference.
	if os.Getenv(benchCleanWasmEnv) == "" {
		b.Skipf("set %s to the production clean build to run a paired comparison", benchCleanWasmEnv)
	}

	cleanABOnce.Do(func() {
		ctx := context.Background()
		records := benchFixtureRecords(b)
		decodeEng, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
		survivors := benchDecodeAll(b, ctx, decodeEng, f, records)
		assertSurvival(b, len(survivors), len(records))

		// Encoded once, outside both timed loops. The host marshal is identical
		// for the two arms, so leaving it in would add the same constant to each
		// and shrink the measured ratio without telling us anything.
		inputs := make([][]byte, len(survivors))
		for i, g := range survivors {
			in, err := encodeStdin(g)
			if err != nil {
				b.Fatalf("encodeStdin: %v", err)
			}
			inputs[i] = in
		}

		engA, cf := benchGuestEngine(b, ctx, benchGuestModule(b, benchCleanWasmEnv))
		cleanABCache = &cleanABState{survivors: survivors, inputs: inputs, engA: engA, facade: cf}
	})
	return cleanABCache
}

// cleanABArm is one build's per-record cost, measured interleaved with the
// other build's.
//
// The two arms are NOT separate benchmark runs. `-count=5` on a parent with two
// sub-benchmarks turned out to run A five times and then B five times, which
// leaves exactly the ordering this file is trying to defeat -- and the spread
// within a single arm came out at ~15%, wider than the effect. So the loop below
// evaluates the same record on both modules back to back and accumulates each
// one's nanoseconds separately. Any drift, thermal or GC or scheduler, lands on
// both arms within microseconds of each other.
//
// Which module goes first alternates per record, because whichever runs second
// evaluates a record whose input bytes the first arm just pulled into cache.
//
// time.Since around a ~1.7 ms call costs well under a part in ten thousand, and
// it costs the same on both arms.
//
// check receives how many records each arm emitted for, and how many records the
// two arms produced DIFFERENT output for. Two builds of the same guest must
// agree on both; a floor probe deliberately agrees on neither, so it brings its
// own rule rather than loosening this one.
//
// The outputs are compared because the counts do not constrain them. An arm that
// decoded a body one byte short, or spliced the wrong token back into a message,
// emits for exactly the same records and posts a faster time for it -- which is
// indistinguishable from an optimisation right up until it reaches storage.
// reflect.DeepEqual runs between the two timed regions, so it lands in neither
// arm's nanoseconds.
func pairedArms(b *testing.B, f *reactorFacade, engA, engB *reactorEngine, inputs [][]byte,
	check func(b *testing.B, emitA, emitB, differed, n int)) {
	pairedArmsInputs(b, f, engA, engB, inputs, inputs, check)
}

// pairedArmsInputs is pairedArms for a change that alters the ENVELOPE the guest
// is handed, not just the code that reads it.
//
// The two arms then need different bytes for the same record, and the ratio
// prices the pair -- envelope shape plus the guest that reads it -- which is the
// only thing that can actually be deployed. Splitting them would price neither:
// the old guest cannot read the new envelope at all.
//
// The record-level interleaving and the output comparison are unchanged, and the
// second is what keeps this honest. Two DIFFERENT inputs claiming to carry the
// same message is exactly the setup where a faster arm can be faster because it
// silently received less, and reflect.DeepEqual on the outputs is what refuses
// that. in-B/record is reported per arm here rather than once, because the whole
// point is that the two arms are handed different byte counts.
func pairedArmsInputs(b *testing.B, f *reactorFacade, engA, engB *reactorEngine, inputsA, inputsB [][]byte,
	check func(b *testing.B, emitA, emitB, differed, n int)) {
	if len(inputsA) != len(inputsB) {
		b.Fatalf("the two arms were given %d and %d records; they must be the same "+
			"records in the same order or the ratio compares different traffic",
			len(inputsA), len(inputsB))
	}
	ctx := context.Background()
	idx := benchIndexer(len(inputsA))
	var curA, curB []byte

	run := func(eng *reactorEngine, cur []byte) (time.Duration, any) {
		t0 := time.Now()
		out, err := f.evalFromPool(ctx, eng, cur)
		d := time.Since(t0)
		if err != nil {
			b.Fatalf("eval: %v", err)
		}
		return d, out
	}

	b.ResetTimer()
	var nsA, nsB time.Duration
	var emitA, emitB, differed, touchedA, touchedB int
	for i := range b.N {
		j := idx(i)
		curA, curB = inputsA[j], inputsB[j]
		touchedA += len(curA)
		touchedB += len(curB)
		var dA, dB time.Duration
		var oA, oB any
		if i%2 == 0 {
			dA, oA = run(engA, curA)
			dB, oB = run(engB, curB)
		} else {
			dB, oB = run(engB, curB)
			dA, oA = run(engA, curA)
		}
		nsA += dA
		nsB += dB
		if producedPayload(oA) {
			emitA++
		}
		if producedPayload(oB) {
			emitB++
		}
		if !reflect.DeepEqual(oA, oB) {
			differed++
		}
	}
	b.StopTimer()

	// testing runs a benchmark once with b.N == 1 before the real run, to find
	// out whether it spawns sub-benchmarks. One record is not a sample: whether
	// it survives the guest's filters is a property of that one record, so an
	// emit guard applied to the discovery run fails outright on any fixture
	// whose first record is a discard -- which is what CleanAB's is. Check
	// nothing and report nothing for it; the measured run follows.
	if b.N == 1 {
		return
	}

	check(b, emitA, emitB, differed, b.N)

	perA := float64(nsA) / float64(b.N)
	perB := float64(nsB) / float64(b.N)
	b.ReportMetric(perA, "A-ns/record")
	b.ReportMetric(perB, "B-ns/record")
	b.ReportMetric(perB/perA, "B/A")
	b.ReportMetric(float64(touchedA)/float64(b.N), "A-in-B/record")
	b.ReportMetric(float64(touchedB)/float64(b.N), "B-in-B/record")
}

// producedPayload reports whether a guest wrote a result of its own for this
// record, as opposed to the empty object it emits when a filter discarded it.
//
// Not len(m) > 0. The host stamps config_generation onto EVERY output, so a
// discarded record arrives as {"config_generation":N} and a length test is true
// for all 459 records of the fixture -- which is how the paired guard below
// spent its first few runs unable to fail. Measured: decode emits req/resp for
// 184 and the bare stamp for 275, while len(m) > 0 counted 459.
func producedPayload(out any) bool {
	m, ok := out.(map[string]any)
	if !ok {
		return false
	}
	for k := range m {
		if k != "config_generation" {
			return true
		}
	}
	return false
}

// sameWorkGuard is the rule for comparing two builds of the same guest.
func sameWorkGuard(b *testing.B, emitA, emitB, differed, n int) {
	// Both builds must have done the work. A build that trapped or filtered
	// everything would post a very fast time for doing nothing, which is the
	// shape a "win" takes.
	if emitA == 0 || emitB == 0 {
		b.Fatalf("emitted A=%d B=%d of %d; a zero here means that arm's time is the "+
			"empty-output path, not the conversion", emitA, emitB, n)
	}
	// And they must have kept or dropped the SAME records. If they disagree, the
	// two arms are not doing comparable work and the ratio is meaningless
	// whichever way it points.
	if emitA != emitB {
		b.Fatalf("the two builds emitted for different record counts (A=%d B=%d); "+
			"they are not doing the same work, so the ratio below compares nothing",
			emitA, emitB)
	}
	// Same records is not the same result. These are two builds of one guest, so
	// every record has to come out identical -- this is the only place a change
	// to how a message is taken apart is checked against real traffic INSIDE
	// wasm, rather than natively in the SAS repo's own tests.
	if differed != 0 {
		b.Fatalf("the two builds produced different output for %d of %d records; "+
			"B is not a faster way of doing A's work, it is doing different work",
			differed, n)
	}
}

func BenchmarkSASGuest_CleanAB(b *testing.B) {
	st := cleanABSetup(b)
	engB := cleanArmB(b, benchCleanWasmBEnv)
	pairedArms(b, st.facade, st.engA, engB, st.inputs, sameWorkGuard)
}

// splicedDecodeGlobals is decodeGlobals with the record carried as the JSON it
// already is, which is what the Kafka trigger produces with ValueAsJSON.
//
// json.RawMessage, not []byte: json.Marshal writes a []byte as base64, and the
// arm would then be measuring the guest's failure path at great speed.
func splicedDecodeGlobals(record []byte) map[string]any {
	return map[string]any{
		"$item": map[string]any{
			"topic": "sas-bench",
			"value": json.RawMessage(record),
		},
	}
}

// BenchmarkSASGuest_DecodeEnvelopeAB prices the envelope shape, not a build.
//
// Both arms run the SAME decode module -- which is the point, and why this does
// not use DecodeAB's two-path guard. The only difference between the arms is the
// bytes the host hands it: arm A the access log escaped into a JSON string,
// arm B the same log spliced in as JSON. Two builds would confound the envelope
// with whatever else differed between them; one build cannot.
//
// This REQUIRES a guest that reads both encodings. That is a property of the
// module under XFLOW_BENCH_DECODE_WASM, not something this repo can assume: a
// guest declaring the envelope's value as a string fails arm B outright, with
// "item is neither a kafka envelope nor an apisix message", because the spliced
// object leaves its value field empty and drops the item down the bare-message
// path. If that is what you see, the guest is the thing to fix -- an escaped-only
// guest cannot collect this saving no matter what the host sends.
//
// Measured, three paired runs on one build: 0.78 / 0.84 / 0.82 B/A on decode's
// own eval. The floor probes had put unquoting at 0.345 of decode's eval (0.169
// finding the token's end, 0.168 decoding escapes the host applied moments
// earlier, 0.008 the copy) -- roughly half of that reaches the end-to-end number,
// which is what a floor probe is for. A-in-B/record vs B-in-B/record reports the
// smaller payload that comes with it.
//
// sameWorkGuard does the load-bearing work here. Two different byte sequences
// claiming to carry the same record is exactly where a faster arm can be faster
// because it silently received less, and the per-record output comparison is
// what refuses that -- on real traffic, inside wazero, which no test in the SAS
// repo can do.
func BenchmarkSASGuest_DecodeEnvelopeAB(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	engA, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	engB, _ := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))

	// Every record, including the ~60% decode discards: production pays the
	// envelope cost before it knows which a record is.
	escaped := make([][]byte, len(records))
	spliced := make([][]byte, len(records))
	for i, rec := range records {
		esc, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin escaped: %v", err)
		}
		spl, err := encodeStdin(splicedDecodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin spliced: %v", err)
		}
		if len(spl) >= len(esc) {
			b.Fatalf("record %d: the spliced envelope (%d B) is not smaller than the escaped one (%d B); "+
				"the host is still escaping, so arm B is not the shape this measures", i, len(spl), len(esc))
		}
		escaped[i], spliced[i] = esc, spl
	}
	pairedArmsInputs(b, f, engA, engB, escaped, spliced, sameWorkGuard)
}

// BenchmarkSASGuest_ChainEnvelopeAB is DecodeEnvelopeAB over BOTH hops.
//
// The envelope only touches decode's input, so the decode-only ratio overstates
// what a deployment gets: clean is unchanged between the arms and dilutes it.
// Measured in the same invocation as the decode-only pair, clean adds about a
// fifth again to the per-record chain (3.37 -> 4.03 ms/record averaged over all
// records, so ~1.6 ms per record that survives the 40% filter). Quote the RATIO,
// not those milliseconds: absolute times here moved 39% between two runs an hour
// apart on the same build.
//
// A ratio between the decode-only one and 1.0 is the only coherent result, since
// the diluting hop is identical in both arms. A chain ratio BELOW the decode-only
// ratio means the two came from different builds or different machine load, not
// from a real effect -- which is exactly what a first attempt at this pair showed.
// Run both benchmarks in one `go test` invocation.
//
// This is the number that can be quoted as a throughput change, because it is
// the whole per-record chain and both shapes are measured in the same process,
// alternating per record -- a separate run per shape would put the session drift
// (>25% across sessions, ~11% within one) straight into the ratio.
//
// The comparison is on CLEAN's output, not decode's: it is the end of the chain,
// and it is what reaches storage. Arms that agreed at decode and diverged after
// would be a worse failure than arms that never agreed.
func BenchmarkSASGuest_ChainEnvelopeAB(b *testing.B) {
	ctx := context.Background()
	records := benchFixtureRecords(b)
	decodeEng, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	assertSurvival(b, len(benchDecodeAll(b, ctx, decodeEng, f, records)), len(records))
	cleanEng, cf := benchGuestEngine(b, ctx, benchGuestModule(b, benchCleanWasmEnv))
	idx := benchIndexer(len(records))

	// One chain pass: decode the given envelope, then clean whatever survived.
	// Returns nil for a record decode filtered out, which is a result the two
	// arms must agree on just as much as a payload.
	chain := func(input []byte) (time.Duration, any) {
		t0 := time.Now()
		out, err := f.evalFromPool(ctx, decodeEng, input)
		if err != nil {
			b.Fatalf("decode eval: %v", err)
		}
		globals, ok := cleanGlobalsFrom(out)
		if !ok {
			return time.Since(t0), nil
		}
		cleanInput, err := encodeStdin(globals)
		if err != nil {
			b.Fatalf("encodeStdin clean: %v", err)
		}
		cleaned, err := cf.evalFromPool(ctx, cleanEng, cleanInput)
		if err != nil {
			b.Fatalf("clean eval: %v", err)
		}
		return time.Since(t0), cleaned
	}

	b.ResetTimer()
	var nsA, nsB time.Duration
	var cleanedA, cleanedB, differed, touchedA, touchedB int
	for i := range b.N {
		rec := records[idx(i)]
		inA, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin escaped: %v", err)
		}
		inB, err := encodeStdin(splicedDecodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin spliced: %v", err)
		}
		touchedA += len(inA)
		touchedB += len(inB)

		var dA, dB time.Duration
		var oA, oB any
		if i%2 == 0 {
			dA, oA = chain(inA)
			dB, oB = chain(inB)
		} else {
			dB, oB = chain(inB)
			dA, oA = chain(inA)
		}
		nsA += dA
		nsB += dB
		if oA != nil {
			cleanedA++
		}
		if oB != nil {
			cleanedB++
		}
		if !reflect.DeepEqual(oA, oB) {
			differed++
		}
	}
	b.StopTimer()

	// The envelope encodings are not allowed to change what reaches storage, and
	// on this corpus they must both get past decode's filters for a majority of
	// records -- an arm that decoded nothing would post the fastest chain here.
	if cleanedA == 0 || cleanedB == 0 {
		b.Fatalf("cleaned A=%d B=%d of %d; a zero means that arm's time is the discard path", cleanedA, cleanedB, b.N)
	}
	if cleanedA != cleanedB {
		b.Fatalf("the two envelope shapes survived decode at different rates (A=%d B=%d); "+
			"they are not carrying the same records", cleanedA, cleanedB)
	}
	if differed != 0 {
		b.Fatalf("the two envelope shapes produced different stored output for %d of %d records; "+
			"the spliced form is not a cheaper encoding of the same message", differed, b.N)
	}

	perA := float64(nsA) / float64(b.N)
	perB := float64(nsB) / float64(b.N)
	b.ReportMetric(perA, "A-ns/record")
	b.ReportMetric(perB, "B-ns/record")
	b.ReportMetric(perB/perA, "B/A")
	b.ReportMetric(1e9/perA, "A-msg/s/core")
	b.ReportMetric(1e9/perB, "B-msg/s/core")
	b.ReportMetric(float64(touchedA)/float64(b.N), "A-in-B/record")
	b.ReportMetric(float64(touchedB)/float64(b.N), "B-in-B/record")
	b.ReportMetric(float64(cleanedA)/float64(b.N)*100, "%cleaned")
}

// cleanArmB compiles whichever clean build the named variable points at, once
// per path per process.
//
// The compile is cached because -count reruns the benchmark body, and a module
// compile between the arms is exactly the gap pairedArms exists to close. It is
// keyed by path rather than shared, so the A/B and floor benchmarks can run in
// one process over one decode sweep without either seeing the other's module.
var (
	cleanArmBMu    sync.Mutex
	cleanArmBCache = map[string]*reactorEngine{}
)

func cleanArmB(b *testing.B, env string) *reactorEngine {
	b.Helper()
	path := os.Getenv(env)
	if path == "" {
		b.Skipf("set %s to the second clean build for this comparison", env)
	}
	if path == os.Getenv(benchCleanWasmEnv) {
		b.Fatalf("both arms point at %s; this would report the machine's noise as a difference between builds", path)
	}
	cleanArmBMu.Lock()
	defer cleanArmBMu.Unlock()
	if eng, ok := cleanArmBCache[path]; ok {
		return eng
	}
	eng, _ := benchGuestEngine(b, context.Background(), benchGuestModule(b, env))
	cleanArmBCache[path] = eng
	return eng
}

// BenchmarkSASGuest_DecodeAB is the same paired comparison for the decode guest.
//
// The two guests cost about the same. Measured on this fixture with the `$input`
// key finally present in clean's env (see cleanGlobalsFrom), decode is ~2.93
// ms/record over every record and clean is ~2.99 ms over the 40% that reach it,
// so clean is ~1.2 ms/record amortised over the whole stream against decode's
// 2.93 -- roughly a 70/30 split, not the 4:1 this comment used to claim. The
// earlier "clean ~0.35 ms" was clean answering `{}` on its missing-$input
// branch; a change worth 20% in clean is worth ~6% of the chain, not 4%.
//
// Arm A is XFLOW_BENCH_DECODE_WASM, the same module every other benchmark here
// uses; arm B is XFLOW_BENCH_DECODE_WASM_B.
//
// Keep -benchtime under ~1500x per arm. The guest's linear memory is capped at
// engine.DefaultWasmMemoryPages (16 MiB) and does not survive more evals than
// that on this fixture -- see the note on maxEvalsPerInstance in pool.go, which
// this run falsified.
func BenchmarkSASGuest_DecodeAB(b *testing.B) {
	pathB := os.Getenv(benchDecodeWasmBEnv)
	if pathB == "" {
		b.Skipf("set %s to a second decode build to run the paired A/B", benchDecodeWasmBEnv)
	}
	pathA := os.Getenv(benchDecodeWasmEnv)
	if pathA == pathB {
		b.Fatalf("both arms point at %s; this would report the machine's noise as a difference between builds", pathA)
	}

	ctx := context.Background()
	records := benchFixtureRecords(b)
	engA, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	engB, _ := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmBEnv))

	// Every record, not only the survivors: production pays for the ~60% decode
	// discards too, and a change that only helps the kept path would look better
	// than it is if the dropped ones were left out.
	inputs := make([][]byte, len(records))
	for i, rec := range records {
		in, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		inputs[i] = in
	}
	pairedArms(b, f, engA, engB, inputs, sameWorkGuard)
}

// BenchmarkSASGuest_DecodeFloor prices the phases INSIDE the decode guest.
//
// Arm B is a floorprobe build of the decode guest (see cmd/guest/decode/
// eval_floor.go in the SAS repo): the same package, the same structs, the same
// encoding/json calls, with eval cut short after stage N and emitting a single
// number derived from what it parsed. Arm A is the production artifact. B/A is
// then the fraction of decode that stage N and everything before it accounts
// for, measured in wazero rather than natively -- which matters, because the
// existing native analysis in cmd/guest/decode/prefilter_bench_test.go and
// scanfloor_bench_test.go concluded a hand-rolled extractor was not worth it,
// and both of its instruments are the shape that got the sign wrong on the
// clean guest.
//
// The emit guard here is the inverse of sameWorkGuard's, and deliberately so: a
// floor is only a floor if it stops early, so arm B must emit for EVERY record
// while arm A emits for the ~40% that survive its filters. Equal counts would
// mean the probe is doing production's work, or production is doing none.
func BenchmarkSASGuest_DecodeFloor(b *testing.B) {
	pathB := os.Getenv(benchDecodeFloorEnv)
	if pathB == "" {
		b.Skipf("set %s to a -tags floorprobe build of the decode guest", benchDecodeFloorEnv)
	}

	ctx := context.Background()
	records := benchFixtureRecords(b)
	engA, f := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeWasmEnv))
	engB, _ := benchGuestEngine(b, ctx, benchGuestModule(b, benchDecodeFloorEnv))

	inputs := make([][]byte, len(records))
	for i, rec := range records {
		in, err := encodeStdin(decodeGlobals(rec))
		if err != nil {
			b.Fatalf("encodeStdin: %v", err)
		}
		inputs[i] = in
	}

	// Which stage this artifact is built at is not in its name or its size --
	// the three builds differ by one -X'd string and are byte-identical in
	// length. Report the number it emits for a fixed record so the reader can
	// tell the runs apart, and so a -X that silently did not take shows up as
	// three identical values instead of a flat cost curve.
	out, err := f.evalFromPool(ctx, engB, inputs[0])
	if err != nil {
		b.Fatalf("floor probe eval: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["n"] == nil {
		b.Fatalf("floor probe emitted no n for record 0 (got keys %v); it is not running the stage code", m)
	}
	b.Logf("floor stage marker n=%v (record 0, %d B input)", m["n"], len(inputs[0]))

	pairedArms(b, f, engA, engB, inputs, func(b *testing.B, emitA, emitB, differed, n int) {
		// differed is ignored here, and only here: the probe stops short of
		// production's output by construction, so every record differs and the
		// count carries no information. The emit rule below is what stands in
		// for it.
		_ = differed
		if emitB != n {
			b.Fatalf("floor arm emitted for %d of %d records; a floor probe returns a "+
				"number for every record, so this one is failing or filtering", emitB, n)
		}
		// The survival check needs a sample. testing always runs one b.N=1
		// probe iteration before the real one, and a single record either
		// survives or does not; only the real iteration's ratio is meaningful,
		// and only its numbers are reported.
		if n < 50 {
			return
		}
		if emitA == 0 || emitA == n {
			b.Fatalf("production arm emitted for %d of %d records; expected the ~40%% "+
				"that survive its filters", emitA, n)
		}
	})
}

// BenchmarkSASGuest_CleanFloor prices the phases INSIDE the clean guest.
//
// The same instrument as BenchmarkSASGuest_DecodeFloor, pointed at the other
// guest, and it is overdue: with decode rewritten, clean plus the host hand-off
// is now ~1.95 of the 2.17 cpu_ms a decoded message costs end to end, against
// decode's 0.21. Whatever is left to win is mostly in here.
//
// Arm B is a floorprobe build of the clean guest (cmd/guest/clean/eval_floor.go
// in the SAS repo), arm A is the production artifact, and the inputs are the
// real ones: what the decode guest actually emitted for the fixture records that
// survived its filters, which is the only shape this guest ever sees.
//
// The emit guard is the floor rule, not sameWorkGuard's: the probe stops short
// of production's output by construction, so every record differs and B must
// emit a number for all of them. Unlike the decode floor, arm A is NOT required
// to drop a fraction -- clean's inputs have already been through decode's
// filters, and how many its own rules reject is a property of the configured
// rule set rather than of the harness. The count is reported instead.
func BenchmarkSASGuest_CleanFloor(b *testing.B) {
	st := cleanABSetup(b)
	engB := cleanArmB(b, benchCleanFloorEnv)

	// Which stage this artifact is built at is not in its name or its size --
	// the builds differ by one -X'd string. Report the number it emits for a
	// fixed record so the runs can be told apart, and so a -X that silently did
	// not take shows up as identical values instead of a flat cost curve.
	out, err := st.facade.evalFromPool(context.Background(), engB, st.inputs[0])
	if err != nil {
		b.Fatalf("floor probe eval: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["n"] == nil {
		b.Fatalf("floor probe emitted no n for record 0 (got keys %v); it is not running the stage code", m)
	}
	b.Logf("floor stage marker n=%v (record 0, %d B input)", m["n"], len(st.inputs[0]))

	pairedArms(b, st.facade, st.engA, engB, st.inputs, func(b *testing.B, emitA, emitB, differed, n int) {
		// differed is ignored here, and only here: see the note above.
		_ = differed
		if emitB != n {
			b.Fatalf("floor arm emitted for %d of %d records; a floor probe returns a "+
				"number for every record, so this one is failing or filtering", emitB, n)
		}
		if emitA == 0 {
			b.Fatalf("production arm emitted for none of %d records; its time is the "+
				"empty-output path, not the conversion", n)
		}
		b.ReportMetric(float64(emitA)/float64(n), "A-emit-frac")
	})
}
