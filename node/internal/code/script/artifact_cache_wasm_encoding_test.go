package script_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// captureWasmRuntime is a private (language, runtime) slot for this file only
// — separate from captureRuntime in capture_engine_test.go, which is
// registered under "js" and cannot see a "wasm" lookup.
const captureWasmRuntime = "capture-wasm-test"

var (
	captureWasmMu   sync.Mutex
	captureWasmSlot *capturedWasmCode
	captureWasmOnce sync.Once
)

type capturedWasmCode struct {
	code  string
	calls int
}

type captureWasmEngine struct{}

func (captureWasmEngine) Name() string { return "wasm/" + captureWasmRuntime }

func (captureWasmEngine) Execute(_ context.Context, src engine.Source, _ map[string]any, _ engine.Helpers) (any, error) {
	captureWasmMu.Lock()
	defer captureWasmMu.Unlock()
	if captureWasmSlot != nil {
		captureWasmSlot.code = src.Code
		captureWasmSlot.calls++
	}
	return map[string]any{"ok": true}, nil
}

// claimCaptureWasmSlot registers the capture engine once per test binary and
// claims the single recorder slot for the duration of one test, mirroring
// captureGlobals in capture_engine_test.go.
func claimCaptureWasmSlot(t *testing.T) *capturedWasmCode {
	t.Helper()
	captureWasmOnce.Do(func() {
		engine.Register("wasm", captureWasmRuntime, func() engine.Engine { return captureWasmEngine{} })
	})

	rec := &capturedWasmCode{}
	captureWasmMu.Lock()
	if captureWasmSlot != nil {
		captureWasmMu.Unlock()
		t.Fatal("capture-wasm slot already held; these tests must not run in parallel")
	}
	captureWasmSlot = rec
	captureWasmMu.Unlock()

	t.Cleanup(func() {
		captureWasmMu.Lock()
		captureWasmSlot = nil
		captureWasmMu.Unlock()
	})
	return rec
}

// TestScript_WasmArtifactCodeArrivesBase64Encoded pins artifactCodeCache.get's
// wasm branch (artifact_cache.go): for language "wasm", the resolved artifact
// bytes must reach the engine as base64 text, exactly as the "code" parameter
// would carry an inline wasm module (see ScriptNode's Descriptor doc: "base64
// wasm module (wasm)"). Both wasm engines run engine.Source.Code through
// decodeCode (base64): the wazero engine unconditionally (wasm/wazero.go:143),
// the reactor on its engineForCode fallback (wasm/host.go:229-236). If this
// branch stopped encoding, an artifact-backed wasm node would hand raw,
// undecodable bytes to its engine.
//
// One nuance worth stating rather than glossing: the reactor SHORT-CIRCUITS on
// a digest hit (host.go:194-221) and never looks at Code at all, so a broken
// encoding would not bite a module that is already resident. It bites the cold
// path — first execution after a restart, or any module not yet compiled —
// which is precisely when a failure is hardest to attribute.
//
// The js artifact-cache tests in artifact_cache_test.go never exercise this
// branch — they all use language "js" — so it has no other coverage in this
// package.
func TestScript_WasmArtifactCodeArrivesBase64Encoded(t *testing.T) {
	rec := claimCaptureWasmSlot(t)

	raw := []byte("\x00asm-fixture-bytes-not-a-real-module")
	sum := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{
		Params: map[string]any{
			"language":        "wasm",
			"runtime":         captureWasmRuntime,
			"artifact_digest": digest,
		},
	}
	input.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) { return raw, nil })

	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if rec.calls == 0 {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}

	want := base64.StdEncoding.EncodeToString(raw)
	if rec.code != want {
		t.Fatalf("engine received code %q, want base64 of the resolved artifact bytes %q", rec.code, want)
	}
}
