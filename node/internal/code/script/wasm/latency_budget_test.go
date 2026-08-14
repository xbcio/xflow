package wasm

import (
	"fmt"
	"time"
)

// maxCalibrationCold is the largest cold (empty-cache) compile that still leaves
// the absolute latency budgets meaningful.
//
// Every absolute budget in this package is a claim about what THIS
// implementation costs, taken from the design's acceptance figures
// (docs/design/WASM-ENGINE-POOLING.md §5.4). A wall-clock budget is only
// measurable on a machine that is not otherwise busy, and there are two
// independent ways for it to stop being measurable:
//
//   - `-race`: the detector instruments the wazero compiler like any other Go
//     code. Measured cold 23-26s vs ~1.7s uninstrumented, a ~15x inflation.
//   - concurrent load: `go test ./...` runs other packages' binaries against the
//     same disk and CPU. Measured cold ~6s and ~16s in full-suite runs vs ~1.7s
//     for the package alone.
//
// Both inflate the number without any change to this implementation, and both
// were previously "solved" separately -- the load case by best-of-5 sampling
// (which does not work: load raises every sample, so the minimum never returns
// to the quiet figure) and the -race case not at all (the two budget tests were
// simply accepted as always-red under -race, which makes a whole-repo -race run
// unable to produce a trustworthy verdict).
//
// The cold sample is the calibration. It is taken in the same process, against
// the same disk, moments before the sample being judged, so it captures BOTH
// contaminants with one measurement and needs no build tags. 3s is ~1.75x the
// measured quiet-machine cold compile (~1.7s) -- loose enough that an ordinary
// single-package run always judges, tight enough that every contaminated run
// observed here (5.9s, 15.8s, 23.2s, 24.8s) declines.
//
// Declining is deliberately NOT the same as skipping the test: the relative
// assertions (restart < cold, warm < cold) always run, and they are the ones
// that catch the defect that actually matters -- a silently bypassed
// compilation cache. They keep their power under any contamination because
// instrumentation and load inflate BOTH samples: measured 23.2s cold vs 519ms
// warm under -race, a 45x gap still intact.
const maxCalibrationCold = 3 * time.Second

// budgetNotMeasurable reports why an absolute latency budget cannot be judged on
// this run, or "" when it can. cold is the empty-cache compile measured moments
// earlier in the same process; see maxCalibrationCold for why it is the right
// calibration signal.
func budgetNotMeasurable(cold time.Duration) string {
	if cold > maxCalibrationCold {
		return fmt.Sprintf("cold compile %v exceeds the %v calibration bar, so this machine is "+
			"instrumented (-race) or busy and the number would measure that, not this "+
			"implementation", cold, maxCalibrationCold)
	}
	return ""
}
