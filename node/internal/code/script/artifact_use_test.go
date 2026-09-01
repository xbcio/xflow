package script_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// runWithCollector drives one xflow.script Execute (via the registry handler,
// the same seam TestScript_ArtifactDigestReachesEngine and its neighbours in
// artifact_digest_forwarding_test.go use -- ScriptNode itself is unexported
// outside this package) under a fresh ArtifactUseCollector, and returns both
// the node's Output and whatever the host recorded.
//
// language/runtime are fixed to js/goja throughout this file: goja needs no
// wasm artifact, so these tests cannot trip the wasm package's -race timeout.
func runWithCollector(t *testing.T, params map[string]any, data map[string]any) (*types.Output, []types.ArtifactUse) {
	t.Helper()
	ctx, collector := types.WithArtifactUseCollector(context.Background())
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script not registered")
	}
	in := &types.Input{
		NodeName: "decode",
		Params:   params,
		Data:     data,
	}
	out, err := h.Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return out, collector.Uses()
}

// digestUsed is runWithCollector for the tests that only care about what got
// recorded, not the Output shape.
func digestUsed(t *testing.T, params map[string]any, data map[string]any) []types.ArtifactUse {
	t.Helper()
	_, uses := runWithCollector(t, params, data)
	return uses
}

// The host attests which artifact ran. A node whose code came from an artifact
// digest must leave exactly one record naming itself and that digest.
func TestScriptNodeRecordsArtifactUseOnSuccess(t *testing.T) {
	uses := digestUsed(t, map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"code":            `({ok: 1})`,
		"artifact_digest": "sha256:aaaa",
	}, nil)
	if len(uses) != 1 || uses[0] != (types.ArtifactUse{Node: "decode", Digest: "sha256:aaaa"}) {
		t.Fatalf("uses = %+v, want exactly one {decode sha256:aaaa}", uses)
	}
}

// A node running inline code has no digest, so there is nothing to attest.
func TestScriptNodeRecordsNothingWithoutADigest(t *testing.T) {
	uses := digestUsed(t, map[string]any{
		"language": "js",
		"runtime":  "goja",
		"code":     `({ok: 1})`,
	}, nil)
	if uses != nil {
		t.Fatalf("uses = %+v, want nil (no artifact_digest was set)", uses)
	}
}

// A guest that threw produced no data. Attributing its digest to the batch's
// rows would name an artifact that produced none of them.
func TestScriptNodeRecordsNothingOnTheErrorPort(t *testing.T) {
	out, uses := runWithCollector(t, map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"code":            `throw new Error("boom")`,
		"artifact_digest": "sha256:aaaa",
	}, nil)
	if out.Port != "error" {
		t.Fatalf("expected error port, got %q", out.Port)
	}
	if uses != nil {
		t.Fatalf("uses = %+v, want nil (guest threw; nothing to attest)", uses)
	}
}

// Handlers call Record unconditionally; a context with no collector is the
// normal case outside a batch and must not panic.
func TestScriptNodeToleratesNoCollector(t *testing.T) {
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script not registered")
	}
	in := &types.Input{
		NodeName: "decode",
		Params: map[string]any{
			"language":        "js",
			"runtime":         "goja",
			"code":            `({ok: 1})`,
			"artifact_digest": "sha256:aaaa",
		},
	}
	if _, err := h.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

// The size cap is the second way a guest that ran to completion still ends up
// on the error port. Nothing downstream distinguishes it from a trap: both
// return Port "error" with an "error" key and no business data, so a manifest
// that attests one but not the other claims a distinction its consumer cannot
// observe.
//
// The oversized-result branch is the only error-port exit that sits *after*
// the guest returned successfully, which is why it is the one that can
// disagree with the trap branch in the first place.
func TestScriptNodeRecordsNothingWhenTheResultExceedsTheSizeCap(t *testing.T) {
	// engine.DefaultMaxOutputBytes is 1 MiB; 1.1 MB of 'x' clears it even
	// before JSON quoting.
	out, uses := runWithCollector(t, map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"code":            `({blob: "x".repeat(1100000)})`,
		"artifact_digest": "sha256:aaaa",
	}, nil)
	if out.Port != "error" {
		t.Fatalf("port = %q, want %q -- the fixture no longer trips the size cap, so this test proves nothing", out.Port, "error")
	}
	if uses != nil {
		t.Fatalf("uses = %+v, want nil (result was rejected by the size cap; it reached no business row)", uses)
	}
}

// Same for the batch path, which has its own copy of the cap check.
func TestScriptNodeRecordsNothingWhenTheBatchResultExceedsTheSizeCap(t *testing.T) {
	out, uses := runWithCollector(t, map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"code":            `({blob: "x".repeat(1100000)})`,
		"artifact_digest": "sha256:bbbb",
	}, map[string]any{
		"messages": []any{map[string]any{"id": 1}},
		"count":    1,
	})
	if out.Port != "error" {
		t.Fatalf("port = %q, want %q -- the fixture no longer trips the size cap, so this test proves nothing", out.Port, "error")
	}
	if uses != nil {
		t.Fatalf("uses = %+v, want nil (batch result was rejected by the size cap)", uses)
	}
}
