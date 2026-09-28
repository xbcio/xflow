package script

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// TestWedgeContentAppliedBeforeEngineStillExecutes is the end-to-end regression
// for the 2026-09-28 collection-runner wedge: a runner that accepted zero Kafka
// messages for hours (commit frontier frozen, ~51M records of lag) while its
// process stayed healthy, and recovered only on restart.
//
// It drives the REAL node path — ScriptNode.Execute through
// ensureWasmSupplyConsumers — rather than the engine layer, because that is the
// layer the production error came from and the layer the seam test file
// documents as the one a well-covered engine let a bug slip past.
//
// The interleaving is ordinary. Supply content reaches the registry before
// anything has compiled the module (ApplyHints runs on its own goroutine and
// does not wait for activation), and the registration that would install it
// arrives from an owner that is already recorded. Both facts are true in
// production for the warm-up consumer, whose owner string is byte-identical to
// the execution-time guard's by design.
//
// Before the fix this returns the production error verbatim:
//
//	wasm module sha256:... is declared source-driven for [...] but holds no
//	supply-borne configuration; refusing to evaluate against an empty rule set
//
// and it returns that error on EVERY message forever, because the registration
// short-circuit means nothing ever re-delivers the content the registry has
// already marked accepted. After the fix the node must configure from the
// supply and execute normally.
func TestWedgeContentAppliedBeforeEngineStillExecutes(t *testing.T) {
	ctx := context.Background()
	code := testSeamCode(t)
	raw := decodeSeamCode(t, code)
	digest := "sha256:" + seamModuleKey(raw)

	// Unique per test: supply.Default and the process-wide reactor host both
	// outlive this test, and supply.UnregisterConsumer never drops the applied
	// Snapshot (see registerWasmSupplyConsumerForTest's comment). Sibling tests
	// rely on the same per-test-name discipline.
	tag := strings.NewReplacer("/", "-", " ", "_").Replace(t.Name())
	workflow := "wf-" + tag
	nodeName := "decode"
	supplyName := "rules-" + tag
	owner := "node:" + workflow + "/" + nodeName

	// Content arrives first, under the name the node declares. This is the
	// state the runner is in when ApplyHints lands before activation.
	if err := supply.Default.Apply(ctx, supply.Snapshot{
		Name:      supplyName,
		Content:   []byte(`{"rules":[{"name":"from-supply"}]}`),
		Hash:      "h1",
		Revision:  11,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply supply content: %v", err)
	}

	// The warm-up consumer registers while the module is still uncompiled. Its
	// owner string is exactly the execution-time guard's ("node:<wf>/<node>"),
	// which is the documented intent of both call sites. The notify finds no
	// engine and succeeds, so the registry records the content as accepted and
	// the owner set now contains this owner.
	if err := wasm.RegisterSupplyConsumerByDigest(digest, supplyName, owner, supply.Default); err != nil {
		t.Fatalf("warm-up register: %v", err)
	}
	if wasm.SupplyConfiguredByDigest(digest) {
		t.Fatal("precondition: nothing has been applied to an engine yet, so the module " +
			"cannot report configured")
	}

	DeclareWasmSupplyConsumers(workflow, nodeName, []string{supplyName})
	t.Cleanup(func() {
		UndeclareWasmSupplyConsumers(workflow, nodeName, []string{supplyName})
		wasm.UnregisterSupplyConsumerByDigest(digest, supplyName, owner, supply.Default)
	})

	input := &types.Input{
		WorkflowName: workflow,
		NodeName:     nodeName,
		Params: map[string]any{
			"language":        "wasm",
			"runtime":         "wazero-reactor",
			"artifact_digest": digest,
		},
	}
	input.SetArtifactCodeResolver(func(context.Context, string) ([]byte, error) {
		return raw, nil
	})

	out, err := (&ScriptNode{}).Execute(ctx, input)
	if err != nil {
		t.Fatalf("the node refused a message although the supply content was already applied "+
			"and the module's own declaration names it: %v", err)
	}
	if out == nil {
		t.Fatal("execution returned no output")
	}
	if out.Port == "error" {
		t.Fatalf("guest errored: %#v", out.Data)
	}

	// The supply revision, not 0: rules came from the supply channel rather than
	// the legacy globals path.
	if got := out.Data[wasm.ConfigGenerationKey]; got != uint64(11) {
		t.Fatalf("%s = %#v, want 11 (the supply revision); the module did not configure "+
			"from the supply that was already applied", wasm.ConfigGenerationKey, got)
	}
	if !matchedRuleNames(t, out.Data)["from-supply"] {
		t.Fatalf("matched = %v, want from-supply present", matchedRuleNames(t, out.Data))
	}
}

// decodeSeamCode and seamModuleKey mirror the wasm package's decodeCode +
// moduleKey for a base64 module, which package script cannot reach (they are
// unexported there). The digest must equal store.ContentHash of the module
// bytes, because that is what the artifact path and the reactor host both key
// on.
func decodeSeamCode(t *testing.T, code string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		t.Fatalf("decode module: %v", err)
	}
	return b
}

func seamModuleKey(wasmBytes []byte) string {
	sum := sha256.Sum256(wasmBytes)
	return hex.EncodeToString(sum[:])
}
