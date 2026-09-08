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
// This is the question the Kafka batch path turns on. A trap is a property of
// the input: redelivering the same bytes traps identically, forever. So it must
// carry types.ErrPermanent, and it must be skippable per-record.
//
// The skippable half is the one measured in production. On 2026-09-07, against
// the real cluster, 5 of 18 partitions stalled on `wasm error: unreachable` --
// the same batch retried 16-22 times at identical offsets, never advancing,
// until the aggregate buffer overflowed and discarded up to 19 846 messages on a
// single partition. Refusing the skip is not the safe side: BOTH branches of
// entryseed.go's admission check decline to admit a failed batch, so an
// unskipped trap parks the commit frontier permanently.
//
// This test used to assert the opposite, on the rationale that skipping would
// "continue on a doomed instance". That rationale was false: evalFromPool
// borrows per call and calls e.doom on the failing instance BEFORE returning
// the error, so the next record cannot receive it. The assertion was pinning a
// stall as if it were a contract.
func TestReactorHostTrapClassification(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	code := b64(reactorTrapWasm)

	_, err := e.Execute(context.Background(), engine.Code(code),
		map[string]any{"$input": map[string]any{"x": 1}}, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("expected the guest to trap")
	}
	t.Logf("trap error: %v", err)

	if !isBatchSkippable(err) {
		t.Fatal("a host trap is not skippable: the batch fails, entryseed.go " +
			"declines to admit it on either branch, and the partition's commit " +
			"frontier parks until an operator intervenes")
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
// the same fmt.Errorf in evalOnce. But it is environmental: a loaded host, a
// slow disk, a deadline set too tight. Redelivering that batch can well
// succeed, so it must NOT be marked permanent -- doing so would commit the
// offset and silently discard real messages.
//
// Since traps became record-skippable, this test carries a second and heavier
// duty: it is the only thing proving the skip verdict follows ctx.Err() rather
// than the error text. Both cases produce a wazero call error on a closed
// module, and the strings can be indistinguishable. If skippability were ever
// decided by matching "unreachable" or "module closed", this test flips and the
// trap test does not -- which is precisely the pair that would otherwise let a
// cancelled batch be silently dropped as N per-record faults.
func TestReactorTimeoutIsNotPermanent(t *testing.T) {
	e, ok := engine.Lookup("wasm", "wazero-reactor")
	if !ok {
		t.Fatal("wazero-reactor engine not registered")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := e.Execute(ctx, engine.Code(b64(reactorSpinWasm)),
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
	if isBatchSkippable(err) {
		t.Errorf("a canceled eval was classified as a per-record fault: every "+
			"remaining record would be 'skipped' for a reason that had nothing "+
			"to do with it, the batch would report success, and the offsets "+
			"would advance past records that never ran. err = %v", err)
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
