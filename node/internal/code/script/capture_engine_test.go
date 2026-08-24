package script_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// captureRuntime is the (language, runtime) pair the params tests execute
// against. It records the globals map the node layer hands the engine and
// returns immediately — the assertions are about what the engine RECEIVES, so
// no real interpreter is involved and the tests stay fast and hermetic.
const captureRuntime = "capture-test"

// The engine registry is process-global and has no Unregister (see
// engine.Register), so the factory is installed exactly once and reads a slot
// that each test owns for its duration. Without the slot, a leftover recorder
// from a finished test would silently absorb a later test's globals — the same
// class of cross-test leakage supply.Default already caused elsewhere in this
// repo.
var (
	captureMu   sync.Mutex
	captureSlot *capturedGlobals
	captureOnce sync.Once
)

type capturedGlobals struct {
	globals map[string]any
	code    string
	digest  string
	// identity is what engine.NodeIdentityFromContext reports inside the engine.
	// It is captured here rather than asserted at the node layer because the
	// context is the seam: the node layer can build the identity correctly and
	// still fail to attach it, and only a reader on the far side notices.
	identity engine.NodeIdentity
	calls    int
}

type captureEngine struct{}

func (captureEngine) Name() string { return "js/" + captureRuntime }

func (captureEngine) Execute(ctx context.Context, src engine.Source, globals map[string]any, _ engine.Helpers) (any, error) {
	captureMu.Lock()
	defer captureMu.Unlock()
	if captureSlot != nil {
		captureSlot.globals = globals
		captureSlot.code = src.Code
		captureSlot.digest = src.Digest
		captureSlot.identity = engine.NodeIdentityFromContext(ctx)
		captureSlot.calls++
	}
	return map[string]any{"ok": true}, nil
}

// captureGlobals claims the recorder slot for the duration of one test and
// returns the record the engine will write into.
func captureGlobals(t *testing.T) *capturedGlobals {
	t.Helper()
	captureOnce.Do(func() {
		engine.Register("js", captureRuntime, func() engine.Engine { return captureEngine{} })
	})

	rec := &capturedGlobals{}
	captureMu.Lock()
	if captureSlot != nil {
		captureMu.Unlock()
		t.Fatal("capture slot already held; these tests must not run in parallel")
	}
	captureSlot = rec
	captureMu.Unlock()

	t.Cleanup(func() {
		captureMu.Lock()
		captureSlot = nil
		captureMu.Unlock()
	})
	return rec
}
