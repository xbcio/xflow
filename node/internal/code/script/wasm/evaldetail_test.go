package wasm

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// captureWarnings installs a slog handler that collects records, and restores
// the previous default when the test ends.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// resetEvalDetailLog clears the throttle so tests do not suppress each other.
// The throttle is process-global by design (it protects the process, not a
// call), so every test that emits must start from a known state.
func resetEvalDetailLog(t *testing.T) {
	t.Helper()
	reset := func() {
		evalDetailLog.mu.Lock()
		defer evalDetailLog.mu.Unlock()
		evalDetailLog.last = make(map[string]time.Time)
		evalDetailLog.suppressed = make(map[string]int)
		evalDetailLog.capped = false
	}
	reset()
	t.Cleanup(reset)
}

func loggedReasons(t *testing.T, buf *bytes.Buffer) []string {
	t.Helper()
	var reasons []string
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if reason, ok := rec["reason"].(string); ok {
			reasons = append(reasons, reason)
		}
	}
	return reasons
}

// TestEvalDetailDistinguishesGuestBranches is the point of the whole file: the
// guest returns one code from several branches, and the host must let an
// operator tell them apart.
//
// Measured against live SAS traffic, 93.6% of batch admissions failed as
// "wasm reactor: input decode failed" — one sentence covering four distinct
// guest branches (bad envelope, unparseable value, missing $item, unrecognised
// shape). The reason each was rejected had been computed inside the sandbox and
// copied across the ABI into reactorEvalError.detail, where nothing read it.
//
// The assertion is that four branches produce four DISTINCT logged reasons.
// Asserting merely that "something was logged" would pass against a throttle
// keyed on the error code, which emits one branch and suppresses the other
// three for the whole interval — the operator then reads one branch's text as
// the whole population, which is the exact mistake the flat admission counter
// already made once.
func TestEvalDetailDistinguishesGuestBranches(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	// The four branches SAS's decode guest returns errDecode from. All share
	// one code, so only the detail separates them.
	branches := []string{
		"decode env: unexpected end of JSON input",
		"decode: missing $item",
		"decode input: kafka envelope value is not an apisix message: invalid character",
		"decode input: item is neither a kafka envelope nor an apisix message",
	}
	for _, branch := range branches {
		detail, err := json.Marshal(map[string]any{"error": branch})
		if err != nil {
			t.Fatal(err)
		}
		logEvalDetail("eval", errDecode, detail)
	}

	got := loggedReasons(t, buf)
	distinct := map[string]bool{}
	for _, reason := range got {
		distinct[reason] = true
	}
	if len(distinct) != len(branches) {
		t.Errorf("%d distinct reasons logged for %d distinct guest branches: %v.\n"+
			"All four return the same error code, so a throttle keyed on the code "+
			"emits one and hides the rest — and the operator reads that one as the "+
			"whole population.", len(distinct), len(branches), got)
	}
	for _, branch := range branches {
		if !distinct[branch] {
			t.Errorf("guest branch %q never reached the log", branch)
		}
	}
}

// TestEvalDetailReachesTheLogFromARealGuest is the wiring test, and it exists
// because the tests above do not prove anything about the product.
//
// Every other test in this file calls logEvalDetail directly. Deleting the call
// site in pool.go left all of them green — the same shape as assigning a fake
// into a private field and calling it coverage. What has to be shown is that a
// real guest's rejection reaches the log through the real host code, so the two
// halves of this test drive the two call sites and assert on text only the
// compiled guest can produce.
//
// The reference guest rejects a payload it cannot unmarshal from both exports,
// with a different sentence each time: "decode config: ..." from configure and
// "decode input: ..." from eval. Neither string appears in this file except as
// the thing being looked for.
func TestEvalDetailReachesTheLogFromARealGuest(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	eng, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	t.Run("configure", func(t *testing.T) {
		resetEvalDetailLog(t)
		buf := captureWarnings(t)

		// Not a JSON object, so the guest's configure cannot unmarshal it.
		if _, err := eng.newInstance(ctx, []byte("this is not a config")); err == nil {
			t.Fatal("newInstance accepted an unparseable config")
		}

		if !strings.Contains(buf.String(), "decode config:") {
			t.Errorf("the guest rejected the config and its reason never reached the "+
				"log. logEvalDetail is not wired into configure. log = %q", buf.String())
		}
	})

	t.Run("eval", func(t *testing.T) {
		resetEvalDetailLog(t)

		inst, err := eng.newInstance(ctx, []byte(`{"rules":[]}`))
		if err != nil {
			t.Fatalf("newInstance: %v", err)
		}
		defer inst.teardown(ctx)

		// Capture only around evalOnce: newInstance above logs nothing, but
		// narrowing the window keeps this assertion about the eval path alone.
		buf := captureWarnings(t)
		out, _, err := inst.evalOnce(ctx, []byte("this is not an env"))
		if err == nil {
			t.Fatalf("evalOnce accepted an unparseable input, returning %q", out)
		}

		if !strings.Contains(buf.String(), "decode input:") {
			t.Errorf("the guest rejected the input and its reason never reached the "+
				"log. logEvalDetail is not wired into evalOnce — which is the path "+
				"live traffic takes. log = %q", buf.String())
		}
		// The same guest text must NOT be in the error: Error() becomes a map
		// batch's _error entry, which is node output and can be persisted.
		if strings.Contains(err.Error(), "decode input:") {
			t.Errorf("the guest's reason reached Error(): %q", err.Error())
		}
	})
}

// TestEvalDetailThrottlesRepeats guards the other direction. A malformed
// producer invalidates every record in a partition, so the same reason arriving
// thousands of times must collapse to one line — and that line must say how
// many it stands for, or a throttled log understates the volume and reads like
// an isolated incident.
func TestEvalDetailThrottlesRepeats(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	detail := []byte(`{"error":"decode: missing $item"}`)
	const repeats = 50
	for range repeats {
		logEvalDetail("eval", errDecode, detail)
	}

	lines := strings.Count(strings.TrimSpace(buf.String()), "\n") + 1
	if lines != 1 {
		t.Errorf("%d lines for %d identical rejections; an unthrottled reject log "+
			"becomes the outage it is reporting", lines, repeats)
	}

	// The first line stands for 1: everything after it is suppressed and shows
	// up in the NEXT emission's count. What must not happen is the count being
	// absent, which is what makes a throttled log understate volume.
	var rec map[string]any
	first, _, _ := strings.Cut(strings.TrimSpace(buf.String()), "\n")
	if err := json.Unmarshal([]byte(first), &rec); err != nil {
		t.Fatalf("log line is not JSON: %q", first)
	}
	if _, ok := rec["occurrences"]; !ok {
		t.Error("the logged line carries no occurrences field, so a suppressed " +
			"flood reads as a single incident")
	}

	// Advance past the interval: the next emission must report everything
	// suppressed in between, not restart from 1.
	evalDetailLog.mu.Lock()
	evalDetailLog.last["eval\x00decode: missing $item"] = time.Now().Add(-2 * evalDetailLogInterval)
	evalDetailLog.mu.Unlock()

	buf.Reset()
	logEvalDetail("eval", errDecode, detail)
	rec = nil
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("second log line is not JSON: %q", buf.String())
	}
	if got, want := rec["occurrences"], float64(repeats); got != want {
		t.Errorf("occurrences = %v, want %v (the %d suppressed plus this one). A "+
			"count that restarts at 1 turns a sustained flood into a trickle.",
			got, want, repeats-1)
	}
}

// TestEvalDetailKeySetIsBounded covers the failure mode this logger could
// introduce: a guest that embeds a per-record value (an offset, a request ID)
// in its reason gives every rejection a unique key, and an unbounded map keyed
// on guest-controlled text is a memory leak driven by the traffic.
//
// Past the cap the entries share one slot, and the emitted line must SAY the
// key set was truncated — otherwise the log claims a variety it stopped
// tracking, which is worse than admitting the limit.
func TestEvalDetailKeySetIsBounded(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	for i := range evalDetailKeyCap * 3 {
		detail, err := json.Marshal(map[string]any{
			"error": "decode input: offset " + string(rune('a'+i%26)) + string(rune('a'+i/26)),
		})
		if err != nil {
			t.Fatal(err)
		}
		logEvalDetail("eval", errDecode, detail)
	}

	evalDetailLog.mu.Lock()
	keys := len(evalDetailLog.last)
	evalDetailLog.mu.Unlock()
	if keys > evalDetailKeyCap+1 { // +1 for the shared overflow slot
		t.Errorf("throttle holds %d keys against a cap of %d; guest-controlled "+
			"text is growing a host map", keys, evalDetailKeyCap)
	}

	if !strings.Contains(buf.String(), `"reasons_capped":true`) {
		t.Error("the key set hit its cap and no logged line said so. A log that " +
			"silently stops distinguishing reasons reads exactly like one where " +
			"the variety genuinely stopped.")
	}
}

// TestEvalDetailStaysOutOfTheError pins the boundary the detail must not cross.
//
// Error() flows into a map batch's _error entry, which is node output and can
// be persisted. A guest is free to quote its input while explaining a rejection
// — that input is live traffic — so the detail belongs in a log, at a stack
// trace's trust level, and not in a database column.
func TestEvalDetailStaysOutOfTheError(t *testing.T) {
	secret := "user-token-abc123"
	err := &reactorEvalError{
		code:   errDecode,
		detail: []byte(`{"error":"decode input: bad value ` + secret + `"}`),
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("the guest's detail reached Error(): %q. Error() becomes a map "+
			"batch's _error entry, which is node output and can be persisted; a "+
			"guest quoting live traffic would put it there.", err.Error())
	}
}

// TestEvalDetailUnstructuredIsNotDiscarded covers the shape the ABI does not
// enforce. The convention is {"error": "..."} but a guest may write anything;
// discarding what does not parse would recreate the blind spot — the reason
// would once again exist in the process and never be readable.
func TestEvalDetailUnstructuredIsNotDiscarded(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	logEvalDetail("eval", errDecode, []byte("plain text reason, no JSON"))

	if !strings.Contains(buf.String(), "plain text reason") {
		t.Errorf("a detail that is not the conventional shape was dropped; the "+
			"reason exists in the process and is unreadable. log = %q", buf.String())
	}
}

// TestEvalDetailTruncates keeps one reason bounded. The guest writes it, so it
// carries a guest's trust level and a stack trace's shape.
func TestEvalDetailTruncates(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	long := strings.Repeat("x", evalDetailLimit*3)
	detail, err := json.Marshal(map[string]any{"error": long})
	if err != nil {
		t.Fatal(err)
	}
	logEvalDetail("eval", errDecode, detail)

	reasons := loggedReasons(t, buf)
	if len(reasons) != 1 {
		t.Fatalf("expected one logged reason, got %v", reasons)
	}
	if len(reasons[0]) > evalDetailLimit+len("…(truncated)") {
		t.Errorf("logged reason is %d bytes, cap is %d", len(reasons[0]), evalDetailLimit)
	}
}

// TestEvalDetailOpDistinguishesConfigureFromEval pins why op is a label at all.
// The same code means different things across the two exports: errDecode from
// configure is a malformed rule set an operator deployed, from eval it is a
// malformed record a producer sent. One is your problem, the other is theirs.
func TestEvalDetailOpDistinguishesConfigureFromEval(t *testing.T) {
	resetEvalDetailLog(t)
	buf := captureWarnings(t)

	detail := []byte(`{"error":"decode: bad json"}`)
	logEvalDetail("configure", errDecode, detail)
	logEvalDetail("eval", errDecode, detail)

	lines := strings.Count(strings.TrimSpace(buf.String()), "\n") + 1
	if lines != 2 {
		t.Errorf("%d lines for the same reason from two different exports; a "+
			"bad rule set and a bad record collapsed into one entry", lines)
	}
}
