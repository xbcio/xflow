package wasm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
)

// TestConfigGaugesAggregateAcrossEngines pins that the rule-count and
// generation gauges describe the whole process rather than the last engine to
// swap. It is the same defect TestInstanceGaugeCountsAllEngines pins, on the
// two gauges that were left behind when that one was fixed.
//
// SupplyMetrics Sets both series keyed only by namespace — no module identity —
// so every report REPLACES the previous value. Reported per-engine, with SAS's
// two modules resident, xflow_wasm_config_rule_count read one module's rule
// count while both modules' rules were evaluating, and
// xflow_wasm_config_generation read whichever module swapped most recently
// rather than the oldest content still serving. During a rollout that made the
// generation gauge announce completion while a module was still on the old
// revision.
//
// A module label is not the alternative: Observer's contract forbids it in as
// many words, and a module's only identity here is its sha256.
func TestConfigGaugesAggregateAcrossEngines(t *testing.T) {
	ctx := context.Background()
	obs := &recordingObserver{}
	SetObserver(obs)
	t.Cleanup(func() { SetObserver(nil) })

	h := newReactorHost()

	cfgA, err := json.Marshal(ruleConfig(
		[2]string{"a0", `request.uri startsWith "/api/"`},
		[2]string{"a1", `request.uri startsWith "/v2/"`},
	))
	if err != nil {
		t.Fatalf("marshal cfgA: %v", err)
	}
	cfgB, err := json.Marshal(ruleConfig(
		[2]string{"b0", `request.uri startsWith "/b0/"`},
		[2]string{"b1", `request.uri startsWith "/b1/"`},
		[2]string{"b2", `request.uri startsWith "/b2/"`},
	))
	if err != nil {
		t.Fatalf("marshal cfgB: %v", err)
	}

	// Two DISTINCT module blobs so engineFor's sha256 dedup yields two engines.
	// A custom section is skipped by any conformant loader, so the second module
	// differs only in its hash.
	secondModule := appendCustomSection(reactorWasm, "xflow-test-config-b", []byte{0x02})

	// The lower revision is applied FIRST, so last-writer-wins and take-the-
	// minimum disagree on both values: a per-engine report ends at rules=3,
	// revision=9, while the correct host-wide answer is rules=5, revision=5.
	// Ordering them the other way would have let the generation assertion pass
	// against the unfixed code by coincidence.
	const (
		revOld = 5
		revNew = 9
	)
	for i, step := range []struct {
		mod []byte
		cfg []byte
		rev uint64
	}{
		{reactorWasm, cfgA, revOld},
		{secondModule, cfgB, revNew},
	} {
		e, err := h.engineFor(ctx, step.mod)
		if err != nil {
			t.Fatalf("engineFor[%d]: %v", i, err)
		}
		if err := e.swapConfig(ctx, step.cfg, 2, step.rev); err != nil {
			t.Fatalf("swapConfig[%d]: %v", i, err)
		}
	}

	swaps := obs.swapCalls()
	if len(swaps) == 0 {
		t.Fatal("no pool-swap observations at all")
	}
	// The last observation is what a scrape sees: both metrics behind it are
	// gauges, so each report replaces the previous value.
	last := swaps[len(swaps)-1]
	if last.result != "applied" {
		t.Fatalf("last swap result = %q, want applied", last.result)
	}
	if want := 5; last.ruleCount != want {
		t.Errorf("reported rule count = %d, want %d (2 rules in one module plus 3 in "+
			"the other). A gauge Set once per engine reports the LAST engine's count, "+
			"so with two modules resident the series understates the rules actually "+
			"evaluating traffic.", last.ruleCount, want)
	}
	if last.revision != revOld {
		t.Errorf("reported revision = %d, want %d (the OLDEST content still serving). "+
			"Reporting the swapping engine's own revision means a rollout reads as "+
			"complete as soon as ANY module moves, while a sibling is still "+
			"evaluating the previous rules.", last.revision, revOld)
	}
}

// TestConfigRuleCountPoisonsOnUnrecognizedShape pins that one module whose
// config shape ruleCount cannot read suppresses the whole gauge instead of
// quietly dropping out of the sum.
//
// ruleCount returns -1 for "shape not recognized", and SupplyMetrics skips the
// gauge on a negative value. Folding that -1 into a sum as a zero would publish
// a confident total that is short by one module's worth of rules — a number an
// operator cannot tell apart from a correct one. Absent is readable; wrong is
// not.
func TestConfigRuleCountPoisonsOnUnrecognizedShape(t *testing.T) {
	ctx := context.Background()
	obs := &recordingObserver{}
	SetObserver(obs)
	t.Cleanup(func() { SetObserver(nil) })

	h := newReactorHost()

	good, err := json.Marshal(ruleConfig([2]string{"g0", `request.uri startsWith "/api/"`}))
	if err != nil {
		t.Fatalf("marshal good cfg: %v", err)
	}
	// Valid JSON with no "rules" key: ruleCount cannot count it and returns -1.
	// The guest still accepts it (no rules configured is a legal state), so the
	// swap applies and the pool becomes part of the host's resident set — which
	// is exactly the situation the poison rule exists for.
	unreadable := []byte(`{"revision":7,"entries":[{"id":1}]}`)
	if ruleCount(unreadable) != -1 {
		t.Fatal("premise failed: the fixture config is now countable, so this test no " +
			"longer exercises the unknown-shape path")
	}

	secondModule := appendCustomSection(reactorWasm, "xflow-test-poison-b", []byte{0x03})
	// The unreadable config is applied FIRST and the countable one LAST, so the
	// two behaviours disagree: a per-engine report ends at 1 (the last engine's
	// own count) while the correct host-wide answer is -1. Ordering them the
	// other way round would have let this test pass against the unfixed code —
	// the last engine's own count would have been -1 by coincidence.
	for i, step := range []struct {
		mod []byte
		cfg []byte
	}{
		{reactorWasm, unreadable},
		{secondModule, good},
	} {
		e, err := h.engineFor(ctx, step.mod)
		if err != nil {
			t.Fatalf("engineFor[%d]: %v", i, err)
		}
		if err := e.swapConfig(ctx, step.cfg, 2, uint64(i+1)); err != nil {
			t.Fatalf("swapConfig[%d]: %v", i, err)
		}
	}

	swaps := obs.swapCalls()
	last := swaps[len(swaps)-1]
	if last.ruleCount != -1 {
		t.Errorf("reported rule count = %d, want -1: one resident module's config shape "+
			"is unreadable, so the host-wide total is unknown. Counting it as zero "+
			"publishes %d as if it were the whole process's rule set.",
			last.ruleCount, last.ruleCount)
	}
}

// TestConfigAgeReportsTheStalestModule pins the wiring of the age gauge: an
// Execute on a module that just refreshed must still report the STALEST
// module's age.
//
// xflow_supply_age_seconds is the only signal that exposes a source which
// stopped updating — every version counter stays frozen while a source keeps
// failing, so the config looks healthy. Sampled per-engine it meant "whichever
// module executed most recently"; with two modules resident and traffic
// interleaved, consecutive scrapes alternated between the frozen one and the
// fresh one. An alert on that flaps, and a flapping alert gets muted.
//
// The assertion is a lower bound rather than an exact value. This drives the
// process-wide sharedReactorHost, which is what the production Execute path
// resolves through, and other tests leave engines in it; those can only make
// the maximum LARGER. The unfixed code reports the executing engine's own age,
// which is near zero here no matter what else is resident, so the bound
// separates the two behaviours in both directions.
func TestConfigAgeReportsTheStalestModule(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	t.Cleanup(func() { SetObserver(nil) })

	ctx := context.Background()
	stale := testReactorCode(t)
	fresh := testReactorCode(t)

	staleReg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(stale, "rules", staleReg); err != nil {
		t.Fatalf("register stale: %v", err)
	}
	freshReg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(fresh, "rules", freshReg); err != nil {
		t.Fatalf("register fresh: %v", err)
	}

	if err := staleReg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "stale", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply stale: %v", err)
	}

	// Real elapsed time, not a stubbed clock: ConfigAge reads a monotonic
	// timestamp stored at swap, and there is no seam to inject. The gap is small
	// because the assertion only needs to separate "the stale module's age" from
	// "the executing module's age", and the latter is bounded by how long an
	// Execute takes.
	const gap = 50 * time.Millisecond
	time.Sleep(gap)

	if err := freshReg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "fresh", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply fresh: %v", err)
	}

	if _, err := sharedReactorEngine.Execute(ctx, engine.Code(fresh),
		map[string]any{"x": 1.0}, engine.DefaultHelpers()); err != nil {
		t.Fatalf("Execute(fresh): %v", err)
	}

	ages := rec.ageCalls()
	if len(ages) == 0 {
		t.Fatal("no OnConfigAge observation; the gauge would report nothing at all")
	}
	got := ages[len(ages)-1]
	if got < gap {
		t.Errorf("reported config age = %s, want at least %s (the age of the module "+
			"whose source last refreshed %s ago). Sampling the executing engine's own "+
			"age reports ~0 here, so a module whose source froze is invisible for as "+
			"long as any other module keeps taking traffic.", got, gap, gap)
	}
}
