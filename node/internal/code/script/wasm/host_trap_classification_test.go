package wasm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// TestReactorHostTrapClassification pins how a host-side wasm trap is
// classified for retry purposes.
//
// This is the question the Kafka batch path turns on. A trap dooms the
// instance, so isBatchSkippable makes it batch-fatal and the error propagates
// out of ExecuteBatch. Downstream, node/trigger/kafka/entryseed.go reads
// GroupExecResult.Deterministic to decide between committing the offset (skip
// the batch) and refusing to admit (redeliver). That flag traces back to
// engine.buildEffectiveClassification, which only sets Permanent when the error
// carries a *types.ClassifiedError or wraps types.ErrPermanent.
//
// A trap driven by a malformed message is deterministic: redelivering the same
// bytes traps identically, forever. If the error carries no classification, the
// batch is treated as transient and Kafka redelivers it in a loop -- the
// partition stops advancing, and every later message behind it is stuck too.
//
// This test does not assert which answer is correct; it records which answer
// the code actually gives, so the classification cannot change unnoticed.
func TestReactorHostTrapClassification(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	code := b64(reactorTrapWasm)

	_, err := e.Execute(context.Background(), code,
		map[string]any{"$input": map[string]any{"x": 1}}, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected the guest to trap")
	}
	t.Logf("trap error: %v", err)

	// Guard the premise: a trap must not be mistaken for a per-record guest
	// rejection, or it would be silently skipped rather than classified at all.
	if isBatchSkippable(err) {
		t.Fatal("a host trap was classified as skippable: the batch would " +
			"drop the record and continue on a doomed instance")
	}

	var ce *types.ClassifiedError
	hasClassified := errors.As(err, &ce)
	isPermanent := types.IsPermanent(err)

	t.Logf("ClassifiedError=%v IsPermanent=%v", hasClassified, isPermanent)

	if !isPermanent {
		t.Errorf("host trap is not types.IsPermanent, so GroupExecResult."+
			"Deterministic stays false and the Kafka batch is redelivered "+
			"forever: a single malformed message stalls the partition. "+
			"err = %v", err)
	}

	// Marking the error must not cost its diagnostic value: the wasm stack
	// trace is the only thing that says WHICH guest bug fired, and an operator
	// reading a log line sees err.Error(), not the unwrap chain.
	if !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("the guest trap detail was lost from the error text, leaving "+
			"nothing to diagnose the failing message with: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "eval") {
		t.Errorf("the error no longer says which ABI call failed: %q", err.Error())
	}
}

// TestReactorTimeoutIsNotPermanent is the counter-case that keeps the fix for
// the trap case from overreaching.
//
// A timeout dooms the instance exactly like a trap does, and surfaces through
// the same bare fmt.Errorf in evalOnce. But it is environmental: a loaded host,
// a slow disk, a deadline set too tight. Redelivering that batch can well
// succeed, so it must NOT be marked permanent -- doing so would commit the
// offset and silently discard real messages.
//
// Whatever distinguishes traps from timeouts has to be finer than "the instance
// was doomed".
func TestReactorTimeoutIsNotPermanent(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := e.Execute(ctx, b64(reactorSpinWasm),
		map[string]any{"$input": map[string]any{"x": 1}}, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected the spin guest to fail on a canceled context")
	}
	t.Logf("timeout error: %v", err)

	if types.IsPermanent(err) {
		t.Errorf("a canceled/timed-out eval was marked permanent: the Kafka "+
			"batch would be committed and its messages discarded, though a "+
			"retry could have succeeded. err = %v", err)
	}
}

// TestReactorGuestRejectionStaysSkippable guards the third case: a guest that
// returns a negative non-doom code rejects one record without harming the
// instance. That must stay skippable, or one bad record would fail the whole
// batch.
func TestReactorGuestRejectionStaysSkippable(t *testing.T) {
	err := &reactorEvalError{code: errDecode, detail: []byte("bad input")}
	if !isBatchSkippable(err) {
		t.Error("a guest-classified per-record rejection became batch-fatal")
	}
	if types.IsPermanent(err) {
		t.Error("a skippable per-record error must not carry batch-level " +
			"permanence: it never reaches the batch classification at all")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Errorf("error text does not name the guest's rejection reason: %v", err)
	}
}
