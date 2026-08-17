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

// permanentFault mirrors how the wasm reactor marks a host trap: the original
// error's text stays the message, and the sentinel rides in the unwrap chain so
// types.IsPermanent finds it without a *types.ClassifiedError.
type permanentFault struct{ err error }

func (e *permanentFault) Error() string   { return e.err.Error() }
func (e *permanentFault) Unwrap() []error { return []error{e.err, types.ErrPermanent} }

// TestScript_PermanentEngineErrorStaysPermanent is the load-bearing assertion
// for the Kafka batch path.
//
// A wasm host trap is deterministic: the same bytes trap the same way on every
// redelivery. The reactor marks it types.IsPermanent for exactly that reason.
// But the marker only matters if it survives to engine.buildEffectiveClassification,
// which reads the error the node returned. If the node instead flattens the
// failure into &Output{Port: "error", Data: {"error": err.Error()}}, the engine
// rebuilds it via outputPortRetryError's errors.New(msg) -- a fresh error with
// an empty unwrap chain. Classification then comes back Classified:false,
// GroupExecResult.Deterministic stays false, and the Kafka batch is refused and
// redelivered forever: one malformed message stalls the partition and every
// message queued behind it.
//
// So the node must return a permanently-classified engine error AS an error,
// not as error-port data.
func TestScript_PermanentEngineErrorStaysPermanent(t *testing.T) {
	failWith(t, &permanentFault{err: fmt.Errorf("wasm reactor: eval: wasm error: unreachable")})

	h, _ := registry.Lookup("xflow.script")
	b := node.Script(`whatever`).Language("js").Runtime(failRuntime)
	out, err := h.Execute(context.Background(), &types.Input{Params: b.RawParams().(map[string]any)})

	if err == nil {
		t.Fatalf("a permanently-classified engine failure was flattened into "+
			"error-port output (port=%q), so the engine rebuilds it as a bare "+
			"errors.New and the Kafka batch is redelivered forever", portOf(out))
	}
	if !types.IsPermanent(err) {
		t.Errorf("the returned error lost its permanent classification: %v", err)
	}
	// The wasm stack trace is the only thing naming WHICH guest bug fired.
	if !errorContains(err, "unreachable") {
		t.Errorf("the engine's diagnostic detail was dropped: %v", err)
	}
}

// TestScript_PermanentBatchErrorStaysPermanent covers the batch path, which the
// Kafka trigger actually takes and which duplicates the single-record path's
// error handling rather than sharing it.
func TestScript_PermanentBatchErrorStaysPermanent(t *testing.T) {
	failWith(t, &permanentFault{err: fmt.Errorf("wasm reactor: eval: wasm error: unreachable")})

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
