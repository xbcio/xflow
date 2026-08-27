package runner

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
)

// TestWasmCompileCost measures what §4.5's synchronous warm-up consumer will
// block Apply for. It is report-only by design: a hard threshold on compile
// time is I/O-fragile under a loaded machine and would go red for reasons that
// have nothing to do with this code (see the ColdStart budget test's history).
// The number this prints is an input to a design decision, not a gate.
//
// Two arms:
//   - the in-tree reactorseam guest, always measured, so the ms/MB rate is
//     recorded on every run;
//   - a real SAS-sized module, only when XFLOW_TEST_LARGE_WASM names a file.
//     The in-tree guest is ~0.5 MB while SAS ships 6.8-9 MB, and compile cost
//     is not linear in file size (a custom section costs nothing to compile),
//     so extrapolating from the small one is an estimate, not a measurement.
func TestWasmCompileCost(t *testing.T) {
	ctx := context.Background()

	raw := wasmGuestBytes(t)
	start := time.Now()
	if err := node.CompileWasmModuleBytes(ctx, raw); err != nil {
		t.Fatalf("compile in-tree guest: %v", err)
	}
	small := time.Since(start)
	t.Logf("in-tree guest: %d bytes, compiled in %v (%.1f ms/MB)",
		len(raw), small, float64(small.Milliseconds())/(float64(len(raw))/(1<<20)))

	path := os.Getenv("XFLOW_TEST_LARGE_WASM")
	if path == "" {
		t.Log("XFLOW_TEST_LARGE_WASM unset: the SAS-sized arm did not run. " +
			"The §4.5 decision gate is NOT satisfied by the in-tree number alone.")
		return
	}
	large, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	start = time.Now()
	if err := node.CompileWasmModuleBytes(ctx, large); err != nil {
		t.Fatalf("compile large module: %v", err)
	}
	d := time.Since(start)
	t.Logf("LARGE module: %d bytes, compiled in %v", len(large), d)
	if d > 500*time.Millisecond {
		t.Logf("DECISION GATE: %v > 500ms -- Task 9 must use ASYNC warm-up. "+
			"L1' degrades to bare L1 and §9.3 probe (2) loses its criterion.", d)
	} else {
		t.Logf("DECISION GATE: %v <= 500ms -- Task 9 proceeds with SYNCHRONOUS warm-up.", d)
	}
}
