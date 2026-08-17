package wasm

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// 单条求值失败不该让整批失败 —— 一条烂记录不能污染其余。
func TestExecuteBatch_SkipsFailingRecord(t *testing.T) {
	f := &reactorFacade{host: newReactorHost()}
	code := b64(taggerWasm)

	records := []any{
		map[string]any{"path": "/admin/a", "authorization": "Bearer secret"},
		map[string]any{"path": "/other"},
	}
	globals := map[string]any{
		"$config": taggerConfig(
			cleanRule("authorization", ""),
			tagRule("admin-api", `path startsWith "/admin"`),
		),
	}

	out, err := f.ExecuteBatch(context.Background(), engine.Code(code), records, globals)
	if err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("results = %d, want 2", len(out))
	}
	// 第一条命中且凭证被清掉；第二条不命中。
	rec0, tags0 := taggerResult(t, out[0])
	if _, present := rec0["authorization"]; present {
		t.Fatalf("authorization leaked in batch mode: %v", rec0)
	}
	if !tags0["admin-api"] {
		t.Fatalf("admin-api tag missing on first record: %v", tags0)
	}
	_, tags1 := taggerResult(t, out[1])
	if tags1["admin-api"] {
		t.Fatalf("second record must not match: %v", tags1)
	}
}

func TestExecuteBatch_EmptyInput(t *testing.T) {
	f := &reactorFacade{host: newReactorHost()}
	out, err := f.ExecuteBatch(context.Background(), engine.Code(b64(taggerWasm)), nil, map[string]any{})
	if err != nil {
		t.Fatalf("empty batch must not error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("results = %d, want 0", len(out))
	}
}

// TestIsBatchSkippable exercises the classification pure function with synthetic
// errors covering all four evalOnce doom paths plus the non-doom guest errors.
func TestIsBatchSkippable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantSkip bool
	}{
		// Guest-classified, non-doom: skippable.
		{"errDecode", &reactorEvalError{code: errDecode}, true},
		{"errUnconfigured", &reactorEvalError{code: errUnconfigured}, true},
		{"errConfig", &reactorEvalError{code: errConfig}, true},
		{"errOutput", &reactorEvalError{code: errOutput}, true},
		// Guest-classified, doom code: NOT skippable.
		{"errEval (doom)", &reactorEvalError{code: errEval}, false},
		// Host-side bare errors (alloc trap, write OOB, eval trap): NOT skippable.
		{"alloc trap", fmt.Errorf("wasm reactor: alloc: %w", fmt.Errorf("trap")), false},
		{"write OOB", fmt.Errorf("wasm reactor: write input out of range"), false},
		{"eval trap", fmt.Errorf("wasm reactor: eval: %w", fmt.Errorf("trap")), false},
		// Wrapped guest error (errors.As must unwrap): skippable.
		{"wrapped errDecode", fmt.Errorf("outer: %w", &reactorEvalError{code: errDecode}), true},
		// Wrapped doom: NOT skippable.
		{"wrapped errEval", fmt.Errorf("outer: %w", &reactorEvalError{code: errEval}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBatchSkippable(tt.err)
			if got != tt.wantSkip {
				t.Fatalf("isBatchSkippable(%v) = %v, want %v", tt.err, got, tt.wantSkip)
			}
		})
	}
}

// TestExecuteBatch_CancelledContextFails confirms a pre-cancelled context fails
// the batch immediately rather than producing a silently empty result.
func TestExecuteBatch_CancelledContextFails(t *testing.T) {
	f := &reactorFacade{host: newReactorHost()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.ExecuteBatch(ctx, engine.Code(b64(taggerWasm)), []any{map[string]any{"x": 1}}, map[string]any{})
	if err == nil {
		t.Fatal("cancelled context must fail the batch")
	}
}

var _ = engine.DefaultHelpers
