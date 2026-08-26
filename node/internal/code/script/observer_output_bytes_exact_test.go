package script_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestScript_ObserverOutputBytesMatchesEncodedSize pins the exact value
// reported to OnScriptOutputBytes, not merely that it is positive.
//
// The existing TestScript_ExecuteNotifiesObserver only asserts outputBytes >
// 0, which a stub reporting any fixed positive constant would also satisfy.
// The size reported must be the length of the JSON encoding of the node's own
// output data — the same bytes checkResultSize computes to enforce
// DefaultMaxOutputBytes — so a caller sizing buffers or alerting on payload
// growth from this metric sees the real number.
func TestScript_ObserverOutputBytesMatchesEncodedSize(t *testing.T) {
	h, _ := registry.Lookup("xflow.script")
	rec := &sizeRecordingObserver{}
	node.SetScriptObserver(rec)
	defer node.SetScriptObserver(nil)

	// A result with several keys and a variable-length string, so a stub that
	// happens to match a small fixed-size result by coincidence is unlikely to
	// also match this one.
	b := node.Script(`({doubled: $input.x * 2, label: "abcdefghijklmnopqrstuvwxyz"})`).
		Language("js").Runtime("goja")
	input := &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"x": 21.0},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// out.Data is the exact map checkResultSize marshalled to compute the size
	// it reported; re-marshalling it here must reproduce the identical byte
	// length (encoding/json sorts map keys, so this is deterministic).
	want, err := json.Marshal(out.Data)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if rec.outputBytes != len(want) {
		t.Fatalf("reported output bytes = %d, want %d (len of the encoded result %s)",
			rec.outputBytes, len(want), want)
	}
}

type sizeRecordingObserver struct {
	outputBytes int
}

func (r *sizeRecordingObserver) OnScriptExecute(context.Context, string, string, string, time.Duration) {
}

func (r *sizeRecordingObserver) OnScriptOutputBytes(_ context.Context, _, _ string, size int) {
	r.outputBytes = size
}
