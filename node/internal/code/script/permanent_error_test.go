package script_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// failRuntime is a (language, runtime) pair whose engine fails with whatever
// error the test installs. It exists to drive the node layer's error path with
// a chosen classification -- something no real interpreter lets a test do.
const failRuntime = "fail-test"

var (
	failMu   sync.Mutex
	failSlot error
	failOnce sync.Once
)

type failEngine struct{}

func (failEngine) Name() string { return "js/" + failRuntime }

func (failEngine) Execute(_ context.Context, _ engine.Source, _ map[string]any, _ engine.Helpers) (any, error) {
	failMu.Lock()
	defer failMu.Unlock()
	return nil, failSlot
}

// ExecuteBatch makes the same installed error drive the batch path, which the
// Kafka trigger uses and which has its own copy of the error handling.
func (failEngine) ExecuteBatch(_ context.Context, _ engine.Source, records []any, _ map[string]any) ([]any, error) {
	failMu.Lock()
	defer failMu.Unlock()
	return nil, failSlot
}

// failWith claims the engine's error slot for one test.
func failWith(t *testing.T, err error) {
	t.Helper()
	failOnce.Do(func() {
		engine.Register("js", failRuntime, func() engine.Engine { return failEngine{} })
	})
	failMu.Lock()
	if failSlot != nil {
		failMu.Unlock()
		t.Fatal("fail slot already held; these tests must not run in parallel")
	}
	failSlot = err
	failMu.Unlock()
	t.Cleanup(func() {
		failMu.Lock()
		failSlot = nil
		failMu.Unlock()
	})
}

// permanentFault is a failure that classifies itself permanent and says nothing
// about which record is to blame -- an engine that decided its own state is
// unusable, not that one input was bad.
//
// It deliberately does NOT implement engine.RecordSkippable. It used to be
// described as mirroring the wasm reactor's host trap, and that stopped being
// true when *permanentHostFault gained RecordSkippable: a real trap is now
// permanent AND skippable, and takes the skip branch. Leaving this double
// described as a trap would have kept these two tests green while they pinned
// behaviour the type they claimed to mirror no longer has -- see
// permanentSkippableFault below for the shape a trap actually makes.
type permanentFault struct{ err error }

func (e *permanentFault) Error() string   { return e.err.Error() }
func (e *permanentFault) Unwrap() []error { return []error{e.err, types.ErrPermanent} }

// permanentSkippableFault mirrors what the wasm reactor's *permanentHostFault
// is after the trap fix: permanent (redelivering these bytes traps identically)
// AND record-skippable (the instance was torn down and rebuilt before the error
// escaped, so the blame stops at this record).
type permanentSkippableFault struct{ err error }

func (e *permanentSkippableFault) Error() string         { return e.err.Error() }
func (e *permanentSkippableFault) Unwrap() []error       { return []error{e.err, types.ErrPermanent} }
func (e *permanentSkippableFault) RecordSkippable() bool { return true }

// TestScript_SkippableOutranksPermanenceOnSingleRecordPath pins the branch
// ORDER inside script.go's single-record error handler, which is the only thing
// that makes the trap fix reachable.
//
// A wasm host trap satisfies both predicates. Whichever check runs first wins,
// and for a while the permanence check did: it returned the error at
// script.go's IsPermanent branch and the skip branch below it never ran. Every
// test still passed, because they all called the classifier directly instead of
// driving this handler -- the classifier was right and the wiring was dead.
//
// Skipping is what keeps the partition moving. Returning the error does not:
// entryseed.go's admission check declines to admit a failed group on BOTH
// branches, so a permanently-classified trap parks the commit frontier exactly
// as an unclassified one does, the aggregate buffer overflows, and the measured
// cost was tens of thousands of discarded messages against the one this drops.
func TestScript_SkippableOutranksPermanenceOnSingleRecordPath(t *testing.T) {
	failWith(t, &permanentSkippableFault{
		err: fmt.Errorf("wasm reactor: eval: wasm error: unreachable"),
	})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("a record-skippable host trap was returned as a node error "+
			"instead of skipping the record: the map body item fails, the group "+
			"fails, entryseed.go refuses the batch on either branch, and the "+
			"partition's commit frontier parks until an operator intervenes. "+
			"err = %v", err)
	}
	if out == nil || out.Port != "main" {
		t.Fatalf("a skipped record must report success on the main port so the "+
			"offsets advance; got port=%q", portOf(out))
	}
	if len(out.Data) != 0 {
		t.Errorf("a skipped record must carry no data, or the trap's record is "+
			"indistinguishable from one that matched no rule: %v", out.Data)
	}
}

// TestScript_PermanentEngineErrorStaysPermanent is the counter-case that keeps
// the skip branch from swallowing everything permanent.
//
// A failure that classifies itself permanent WITHOUT declaring a record to
// blame is the node failing, not a record being rejected. It must travel AS an
// error: the error port flattens a failure into Output.Data["error"], and
// engine/outputPortRetryError rebuilds it via errors.New(msg) -- a fresh error
// with an empty unwrap chain. Classification then comes back Classified:false
// and GroupExecResult.Deterministic stays false, so a failure that cannot
// succeed on retry gets reported as retryable.
//
// The trap case deliberately does NOT come here any more; see
// TestScript_SkippableOutranksPermanenceOnSingleRecordPath.
func TestScript_PermanentEngineErrorStaysPermanent(t *testing.T) {
	failWith(t, &permanentFault{err: fmt.Errorf("engine state unusable: compile cache corrupt")})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatalf("a permanently-classified engine failure was flattened into "+
			"error-port output (port=%q), so the engine rebuilds it as a bare "+
			"errors.New and reports a hopeless failure as retryable", portOf(out))
	}
	if !types.IsPermanent(err) {
		t.Errorf("the returned error lost its permanent classification: %v", err)
	}
	// The engine's own detail is the only thing naming WHAT failed; an operator
	// reading a log line sees err.Error(), not the unwrap chain.
	if !errorContains(err, "compile cache corrupt") {
		t.Errorf("the engine's diagnostic detail was dropped: %v", err)
	}
}

// TestScript_PermanentBatchErrorStaysPermanent covers the batch path, which the
// Kafka trigger actually takes and which duplicates the single-record path's
// error handling rather than sharing it.
//
// The batch path has no skip branch, deliberately: down here "skip" would mean
// dropping the whole batch, and a short results array is indistinguishable from
// a batch where no record matched. Both batch implementations already skip
// per-record internally, so a record-skippable error never reaches this handler.
func TestScript_PermanentBatchErrorStaysPermanent(t *testing.T) {
	failWith(t, &permanentFault{err: fmt.Errorf("engine state unusable: compile cache corrupt")})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"messages": []any{map[string]any{"x": 1}}, "count": 1},
	}
	out, err := h.Execute(context.Background(), input)

	if err == nil {
		t.Fatalf("a permanently-classified batch failure was flattened into "+
			"error-port output (port=%q): the whole point of classifying it was "+
			"to let the Kafka path skip the batch instead of redelivering it",
			portOf(out))
	}
	if !types.IsPermanent(err) {
		t.Errorf("the returned batch error lost its permanent classification: %v", err)
	}
}

// TestScript_OrdinaryEngineErrorStillRoutesToErrorPort is the counter-case that
// keeps the fix from overreaching.
//
// An unclassified engine failure -- a script that throws, a guest that rejects
// a record -- is the script's own deterministic outcome and has always been
// routed to the "error" port so a workflow can branch on it. Turning every
// engine failure into a Go error would silently break that routing for every
// existing workflow. Only a failure that explicitly classified itself changes
// lanes.
func TestScript_OrdinaryEngineErrorStillRoutesToErrorPort(t *testing.T) {
	failWith(t, errors.New("plain boom"))

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("an unclassified engine failure became a Go error, breaking "+
			"error-port routing for every workflow that branches on it: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
}

// TestScript_ClassifiedTransientErrorStillRoutesToErrorPort guards the other
// half of the classification split: an error that classified itself as
// retryable must NOT be turned into a permanent Go error. Only permanence
// changes lanes; a self-declared transient failure keeps the existing
// behaviour, so the Kafka path can still redeliver it.
func TestScript_ClassifiedTransientErrorStillRoutesToErrorPort(t *testing.T) {
	failWith(t, types.NewTransientError("guest.busy", "try again"))

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err != nil {
		t.Fatalf("a self-declared transient failure was returned as a Go error, "+
			"which the engine may commit as terminal: %v", err)
	}
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
}

func portOf(out *types.Output) string {
	if out == nil {
		return "<nil output>"
	}
	return out.Port
}

func errorContains(err error, sub string) bool {
	return err != nil && contains(err.Error(), sub)
}
