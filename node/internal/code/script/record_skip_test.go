package script_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// skippableFault implements engine.RecordSkippable with a verdict the test
// chooses. The two tests below differ ONLY in that boolean, so a passing pair
// proves the node consults the interface rather than pattern-matching a type or
// a message.
type skippableFault struct {
	msg  string
	skip bool
}

func (e *skippableFault) Error() string         { return e.msg }
func (e *skippableFault) RecordSkippable() bool { return e.skip }

// TestScript_SkippableRecordDoesNotReachErrorPort is the regression test for a
// production stall.
//
// The wasm reactor's ExecuteBatch skips a record whose eval failed for that
// record's own reasons (oversized output, undecodable input) and carries on.
// The single-record path did not: it routed the identical error to the "error"
// port, where engine/commit.go's outputPortRetryError rebuilds it as
// errors.New -- an error with an empty unwrap chain. The node then failed
// unclassified, the Kafka batch was never admitted, its offsets never advanced,
// and the partition stalled behind that one record. 14 of 18 partitions died;
// 188 822 messages were discarded at an aggregate buffer that had nowhere to
// drain.
//
// Which path runs is not a tuning choice. A map body hands its script node ONE
// record per item, and one record never has the {messages:[...], count:N} shape
// that selects the batch path -- so the deployment that mattered took the path
// that had no skip.
func TestScript_SkippableRecordDoesNotReachErrorPort(t *testing.T) {
	failWith(t, &skippableFault{msg: "wasm reactor: output exceeds limit", skip: true})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("a single-record fault became a node failure: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want %q -- the error port is what stalled the "+
			"partition, because outputPortRetryError strips the classification "+
			"the Kafka admission check needs", out.Port, "main")
	}
	if len(out.Data) != 0 {
		t.Fatalf("data = %v, want empty -- a skipped record produced no result, "+
			"and inventing one would put a phantom row downstream", out.Data)
	}
}

// TestScript_SkippableRecordIsCountedNotSilent guards the cost of the fix.
//
// Skipping trades a loud failure for a quiet one: downstream, a dropped record
// is indistinguishable from a record that matched no rule. outcome="skipped" is
// the only thing that tells them apart, so it is part of the fix, not decoration
// on top of it. Without it the pipeline would look healthy while losing data.
func TestScript_SkippableRecordIsCountedNotSilent(t *testing.T) {
	failWith(t, &skippableFault{msg: "wasm reactor: output exceeds limit", skip: true})

	rec := &recordingScriptObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	if _, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)}); err != nil {
		t.Fatalf("unexpected node failure: %v", err)
	}

	if rec.executes != 1 {
		t.Fatalf("execute notifications = %d, want 1", rec.executes)
	}
	if rec.lastOutcome != "skipped" {
		t.Fatalf("outcome = %q, want %q -- counted as %[1]q the drop is "+
			"invisible: it either inflates the error rate or vanishes into the "+
			"success rate, and neither reveals lost records", rec.lastOutcome, "skipped")
	}
}

// TestScript_NonSkippableFaultStillRoutesToErrorPort is the counter-case that
// keeps the fix from swallowing failures it has no business swallowing.
//
// It differs from the positive test in exactly one bit -- the verdict the error
// reports -- so if the node ever decided skippability by inspecting the type or
// the message instead of asking, this test flips and the positive one does not.
func TestScript_NonSkippableFaultStillRoutesToErrorPort(t *testing.T) {
	failWith(t, &skippableFault{msg: "wasm reactor: output exceeds limit", skip: false})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("unclassified failure became a Go error: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("port = %q, want %q -- a fault that did NOT declare itself "+
			"per-record was dropped anyway, which loses records silently and "+
			"breaks every workflow branching on the error port", out.Port, "error")
	}
}

// TestScript_WrappedSkippableFaultIsStillSkipped pins the unwrap behaviour.
//
// The verdict travels by errors.As, so a layer that adds context on the way out
// must not change it. Without this, the fix would hold today and quietly stop
// holding the first time someone wraps the reactor's error.
func TestScript_WrappedSkippableFaultIsStillSkipped(t *testing.T) {
	inner := &skippableFault{msg: "output exceeds limit", skip: true}
	failWith(t, fmt.Errorf("wasm/wazero-reactor: %w", inner))

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("unexpected node failure: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want %q -- wrapping the error changed the verdict", out.Port, "main")
	}
}

// TestScript_CancellationOutranksSkippability pins the ordering inside the
// error branch.
//
// A cancelled context makes every remaining record fail. If the skip check ran
// first, a shutdown or a deadline would be read as "these records were
// individually bad": the node would report success, the offsets would advance,
// and the unprocessed records would be gone with no error anywhere. The
// timeout check therefore comes first, and this test is what keeps it there --
// the error installed here is skippable, so only the ordering can produce the
// expected result.
func TestScript_CancellationOutranksSkippability(t *testing.T) {
	failWith(t, &skippableFault{msg: "wasm reactor: output exceeds limit", skip: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(ctx, &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatalf("cancellation was absorbed as a per-record skip (port=%q): the "+
			"records that never ran would be committed as processed", portOf(out))
	}
	var classified *types.ClassifiedError
	if !errors.As(err, &classified) {
		t.Fatalf("cancellation lost its classification: %v", err)
	}
}
