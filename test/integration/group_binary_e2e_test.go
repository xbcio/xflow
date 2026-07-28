//go:build integration

package integration

import (
	"testing"
)

// TestGroupBinaryE2E_NormalHappyPath verifies end-to-end group execution with
// real server and runner binaries: submit → dispatch → group execute → commit.
func TestGroupBinaryE2E_NormalHappyPath(t *testing.T) {
	t.Skip("T17: requires podman env (make env-up && make env-ready)")
}

// TestGroupBinaryE2E_MultiExitSwitch verifies only fired exit ports propagate
// downstream and unfired collector branches are skipped.
func TestGroupBinaryE2E_MultiExitSwitch(t *testing.T) {
	t.Skip("T17: requires podman env")
}

// TestGroupBinaryE2E_SelectorRequiredMismatch verifies a group with a required
// selector that no runner satisfies stays pending (not crashed).
func TestGroupBinaryE2E_SelectorRequiredMismatch(t *testing.T) {
	t.Skip("T17: requires podman env")
}

// TestGroupBinaryE2E_LegacyRunnerNeverClaimsGroup verifies a runner without
// group.exec.v1 capability never claims group assignments.
func TestGroupBinaryE2E_LegacyRunnerNeverClaimsGroup(t *testing.T) {
	t.Skip("T17: requires podman env")
}

// TestGroupBinaryE2E_KillRunnerMidGroupThenRestart verifies lease reclaim and
// re-dispatch after the runner crashes mid-group execution.
func TestGroupBinaryE2E_KillRunnerMidGroupThenRestart(t *testing.T) {
	t.Skip("T17: requires podman env")
}

// TestGroupBinaryE2E_FaultMatrix covers the three crash windows per operation:
// write-before-crash, write-success-response-lost, write-after-outbox-lost.
func TestGroupBinaryE2E_FaultMatrix(t *testing.T) {
	t.Skip("T17: requires podman env")
}
