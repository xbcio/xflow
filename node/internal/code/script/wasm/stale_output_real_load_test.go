package wasm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// buildOversizeFillerRules returns enough identical-condition rules for the
// reference guest to marshal a matched array past its 1 MiB output cap.
//
// The fixture measures the encoded result instead of relying on a fixed rule
// count. Long names keep the number of compiled expressions small; thousands
// of short names would test the guest's memory ceiling rather than ERR_OUTPUT.
func buildOversizeFillerRules(t *testing.T) [][2]string {
	t.Helper()
	const nameWidth = 200_000
	var rules [][2]string
	var names []string
	for {
		b, err := json.Marshal(map[string]any{"matched": names})
		if err != nil {
			t.Fatalf("marshal output probe: %v", err)
		}
		if len(b) > engine.DefaultMaxOutputBytes {
			return rules
		}
		name := fmt.Sprintf("filler-%06d-%s", len(names), strings.Repeat("x", nameWidth))
		names = append(names, name)
		rules = append(rules, [2]string{name, "big == true"})
	}
}

// TestReactorRealGuestRejectsStaleOutputDetail exercises the stale-output path
// with the unmodified reference guest from testdata/reactor/main.go.
//
// Its ERR_OUTPUT branch returns without calling writeErr, so out_len continues
// to describe the previous successful output. A one-slot pool and an explicit
// return/re-borrow prove that the normal pool lifecycle hands the same instance
// to the oversized call; holding an instance across both calls would establish
// identity but would not prove that the successful call avoids planned recycle.
// The host must read those stale bytes into reactorEvalError.detail, then refuse
// to log them because successful output is not an {"error": ...} envelope.
func TestReactorRealGuestRejectsStaleOutputDetail(t *testing.T) {
	resetEvalDetailLog(t)
	ctx := context.Background()
	h := newTestReactorHost(t)
	e, err := h.engineForBytes(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineForBytes: %v", err)
	}

	const secretMarker = "previous-record-secret-must-not-be-logged"
	rules := append([][2]string{
		{"marker-" + secretMarker, `marker == "seed"`},
	}, buildOversizeFillerRules(t)...)
	cfg, err := normalizeConfig(ruleConfig(rules...))
	if err != nil {
		t.Fatalf("normalizeConfig: %v", err)
	}
	if err := e.swapConfig(ctx, cfg, 1, 0); err != nil {
		t.Fatalf("swapConfig: %v", err)
	}

	inst, pool, err := e.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow seed instance: %v", err)
	}
	held := true
	defer func() {
		if held {
			e.giveBack(ctx, pool, inst)
		}
	}()

	seedOut, doomed, err := inst.evalOnce(ctx, []byte(`{"marker":"seed","big":false}`))
	if err != nil {
		t.Fatalf("seed eval: %v", err)
	}
	if doomed {
		t.Fatal("seed eval requested planned recycle; an ERR_OUTPUT on the next borrow would not share its instance")
	}
	if !bytes.Contains(seedOut, []byte(secretMarker)) {
		t.Fatalf("seed output does not contain the marker: %q", seedOut)
	}
	seedInst := inst
	e.giveBack(ctx, pool, inst)
	held = false
	if got := len(pool.free); got != 1 {
		t.Fatalf("returning seed left %d free instances in a one-slot pool", got)
	}

	borrowCtx, cancelBorrow := context.WithTimeout(ctx, 10*time.Second)
	defer cancelBorrow()
	inst, pool, err = e.borrow(borrowCtx)
	if err != nil {
		t.Fatalf("borrow oversize instance: %v", err)
	}
	held = true
	if inst != seedInst {
		t.Fatalf("one-slot pool replaced the seed instance: seed=%p oversize=%p", seedInst, inst)
	}

	buf := captureWarnings(t)
	_, doomed, err = inst.evalOnce(ctx, []byte(`{"marker":"oversize","big":true}`))
	if err == nil {
		t.Fatal("oversize eval succeeded; reference guest did not take ERR_OUTPUT")
	}
	var rerr *reactorEvalError
	if !errors.As(err, &rerr) {
		t.Fatalf("oversize error is %T (%v), want *reactorEvalError", err, err)
	}
	if rerr.code != errOutput {
		t.Fatalf("oversize error code = %d, want %d (ERR_OUTPUT)", rerr.code, errOutput)
	}
	if doomed {
		t.Fatal("ERR_OUTPUT doomed the instance; it is documented as a retained call-level failure")
	}
	if !bytes.Equal(rerr.detail, seedOut) {
		t.Fatalf("ERR_OUTPUT detail = %q, want the preceding output %q; stale bytes did not reach the host decoder", rerr.detail, seedOut)
	}
	if got := strings.TrimSpace(buf.String()); got != "" {
		t.Fatalf("stale successful output was logged as the oversized call's reason: %s", got)
	}
}
