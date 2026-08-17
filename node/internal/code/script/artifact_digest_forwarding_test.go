package script_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// srcDigestForwarding is this file's private payload. The artifact code cache is
// process-wide and keyed by (namespace, digest, language), so a source string
// shared with another test would let this one be served that test's entry.
const srcDigestForwarding = `({forwarded: 1})`

// TestScript_ArtifactDigestReachesEngine pins the wiring that makes the wasm
// reactor's module lookup cheap.
//
// The engine resolves a compiled module by digest — a 64-byte map key — instead
// of by the base64 module string, which is ~9 MB for the SAS decode guest and
// costs ~200 µs per lookup in runtime.memequal because production never shares a
// string header between the activation path and the artifact cache (see
// engine.Source and wasm/module_resolution_bench_test.go).
//
// All of that is unreachable if ScriptNode drops the digest on the way to the
// engine, and dropping it fails NOTHING: every result stays correct, every test
// stays green, and the node quietly returns to the 9 MB compare. That is exactly
// how this defect arrived, so the assertion is on what the engine RECEIVES.
//
// It uses the capture engine rather than wasm/wazero-reactor because the seam
// under test is ScriptNode -> engine.Source, which is above the language split:
// script.go resolves the artifact and builds the Source before it looks up any
// engine.
func TestScript_ArtifactDigestReachesEngine(t *testing.T) {
	captured := captureGlobals(t)

	src := []byte(srcDigestForwarding)
	sum := sha256.Sum256(src)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{
		Params: map[string]any{
			"language":        "js",
			"runtime":         captureRuntime,
			"artifact_digest": digest,
		},
		Data: map[string]any{"x": 1.0},
	}
	input.SetArtifactCodeResolver(func(_ context.Context, _ string) ([]byte, error) {
		return src, nil
	})

	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if captured.calls == 0 {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}
	// The code must still arrive — a Source carrying only a digest would break
	// every engine that has never compiled the module.
	if captured.code != srcDigestForwarding {
		t.Fatalf("engine received code %q, want the resolved artifact bytes", captured.code)
	}
	if captured.digest != digest {
		t.Fatalf("engine received digest %q, want %q;\n"+
			"without it the wasm reactor falls back to comparing the whole module "+
			"string on every message (~200 µs against ~46 ns)", captured.digest, digest)
	}
}

// TestScript_InlineCodeCarriesNoDigest pins the other half of the contract:
// engine.Source.Digest names the bytes in Code, or it is empty. An inline script
// has no artifact behind it, so a non-empty Digest here would be a fabricated
// identity — and the reactor treats a digest as authoritative, so it would serve
// whatever module happened to be compiled under that key.
func TestScript_InlineCodeCarriesNoDigest(t *testing.T) {
	captured := captureGlobals(t)

	h, _ := registry.Lookup("xflow.script")
	input := &types.Input{
		Params: map[string]any{
			"language": "js",
			"runtime":  captureRuntime,
			"code":     srcDigestForwarding,
		},
		Data: map[string]any{"x": 1.0},
	}
	if _, err := h.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if captured.calls == 0 {
		t.Fatal("engine was never invoked; the capture seam did not fire")
	}
	if captured.digest != "" {
		t.Fatalf("inline code carried digest %q; a digest must name the bytes it "+
			"travels with, and there is no artifact behind an inline script", captured.digest)
	}
}
